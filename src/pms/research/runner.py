"""Research backtest runner for queued backtest runs."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from decimal import Decimal
import os
import socket
from typing import Any, Literal, Protocol, TypeAlias, cast
from uuid import uuid4

import asyncpg

from pms.actuator.adapters.paper import PaperActuator
from pms.actuator.risk import InsufficientLiquidityError
from pms.controller.factory import ControllerPipelineFactory
from pms.core.enums import OrderStatus
from pms.core.models import FillRecord, MarketSignal, OrderState, Portfolio, Position
from pms.evaluation.metrics import StrategyVersionKey
from pms.research.entities import PortfolioTarget, serialize_portfolio_target_json
from pms.research.replay import MarketUniverseReplayEngine
from pms.research.spec_codec import (
    deserialize_backtest_spec as _codec_deserialize_backtest_spec,
    deserialize_execution_config as _codec_deserialize_execution_config,
)
from pms.research.specs import (
    BacktestExecutionConfig,
    BacktestSpec,
    ExecutionModel,
)
from pms.storage.strategy_registry import _strategy_from_config_json
from pms.strategies.projections import ActiveStrategy


CancelPoint = Literal["before_first_strategy", "between_strategies", "after_last_strategy"]
CancelProbe: TypeAlias = Callable[[CancelPoint], Awaitable[None] | None]
StrategyLoader: TypeAlias = Callable[
    [tuple[StrategyVersionKey, ...]],
    Awaitable[list[ActiveStrategy]],
]
HostProvider: TypeAlias = Callable[[], str]
PidProvider: TypeAlias = Callable[[], int]


class ReplayEngineLike(Protocol):
    def stream(
        self,
        spec: BacktestSpec,
        exec_config: BacktestExecutionConfig,
    ) -> AsyncIterator[MarketSignal]: ...


class ControllerPipelineLike(Protocol):
    async def on_signal(
        self,
        signal: MarketSignal,
        portfolio: Portfolio | None = None,
    ) -> tuple[object, object] | None: ...


class ControllerPipelineFactoryLike(Protocol):
    def build(self, strategy: ActiveStrategy) -> ControllerPipelineLike: ...


@dataclass(frozen=True, slots=True)
class _ClaimedRun:
    run_id: str
    spec: BacktestSpec
    exec_config: BacktestExecutionConfig


@dataclass(slots=True)
class _StrategyAccumulator:
    strategy_id: str
    strategy_version_id: str
    execution_model: ExecutionModel
    started_at: datetime = field(default_factory=lambda: datetime.now(tz=UTC))
    finished_at: datetime | None = None
    opportunity_count: int = 0
    decision_count: int = 0
    fill_count: int = 0
    # Fills whose signal carried a `resolved_outcome`. Only those can
    # contribute real P&L; when this stays zero we must not publish a
    # spurious 0.0 pnl_cum (see `as_insert_args`).
    fills_with_resolution: int = 0
    brier_scores: list[Decimal] = field(default_factory=list)
    slippage_bps_values: list[Decimal] = field(default_factory=list)
    cumulative_pnl: Decimal = Decimal("0")
    peak_equity: Decimal = Decimal("0")
    max_drawdown: Decimal = Decimal("0")
    targets: dict[tuple[str, str, Literal["buy_yes", "buy_no"], datetime], float] = field(
        default_factory=dict
    )

    def record_decision(
        self,
        *,
        signal: MarketSignal,
        opportunity: object,
        decision: object,
    ) -> None:
        self.opportunity_count += 1
        self.decision_count += 1
        opportunity_side = cast(str, getattr(opportunity, "side"))
        token_id = cast(str | None, getattr(decision, "token_id"))
        if token_id is not None:
            target_side: Literal["buy_yes", "buy_no"]
            target_side = "buy_yes" if opportunity_side == "yes" else "buy_no"
            self.targets[(signal.market_id, token_id, target_side, signal.fetched_at)] = float(
                cast(float, getattr(decision, "size"))
            )
        resolved_outcome = _resolved_outcome(signal)
        if resolved_outcome is not None:
            prob_estimate = Decimal(str(cast(float, getattr(decision, "prob_estimate"))))
            resolved = Decimal(str(resolved_outcome))
            self.brier_scores.append((prob_estimate - resolved) ** 2)

    def record_fill(
        self,
        *,
        signal: MarketSignal,
        opportunity: object,
        decision: object,
        fill_price: float,
    ) -> None:
        self.fill_count += 1
        self.slippage_bps_values.append(Decimal(str(self.execution_model.slippage_bps)))
        if _resolved_outcome(signal) is not None:
            self.fills_with_resolution += 1
        pnl_delta = _pnl_delta(
            signal=signal,
            opportunity_side=cast(str, getattr(opportunity, "side")),
            decision_size=float(cast(float, getattr(decision, "size"))),
            fill_price=fill_price,
            execution_model=self.execution_model,
        )
        self.cumulative_pnl += pnl_delta
        if self.cumulative_pnl > self.peak_equity:
            self.peak_equity = self.cumulative_pnl
        drawdown = self.peak_equity - self.cumulative_pnl
        if drawdown > self.max_drawdown:
            self.max_drawdown = drawdown

    def portfolio_target(self) -> PortfolioTarget:
        return PortfolioTarget(
            strategy_id=self.strategy_id,
            strategy_version_id=self.strategy_version_id,
            targets=dict(self.targets),
        )

    def as_insert_args(self, *, run_id: str) -> tuple[object, ...]:
        self.finished_at = self.finished_at or datetime.now(tz=UTC)
        brier = (
            float(sum(self.brier_scores) / Decimal(len(self.brier_scores)))
            if self.brier_scores
            else None
        )
        slippage_bps = (
            float(sum(self.slippage_bps_values) / Decimal(len(self.slippage_bps_values)))
            if self.slippage_bps_values
            else None
        )
        fill_rate = (
            self.fill_count / self.decision_count if self.decision_count > 0 else 0.0
        )
        # When zero fills had resolution data, cumulative_pnl is structurally
        # 0 from summing no-op `_pnl_delta` contributions — not a genuine
        # zero outcome. Emit NULL so downstream reports can distinguish
        # "no P&L to compute" from "computed P&L that happens to be zero".
        pnl_cum: float | None
        drawdown_max: float | None
        if self.fills_with_resolution > 0:
            pnl_cum = float(self.cumulative_pnl)
            drawdown_max = float(self.max_drawdown)
        else:
            pnl_cum = None
            drawdown_max = None
        return (
            str(uuid4()),
            run_id,
            self.strategy_id,
            self.strategy_version_id,
            brier,
            pnl_cum,
            drawdown_max,
            fill_rate,
            slippage_bps,
            self.opportunity_count,
            self.decision_count,
            self.fill_count,
            serialize_portfolio_target_json(self.portfolio_target()),
            self.started_at,
            self.finished_at,
        )


@dataclass(slots=True)
class BacktestRunner:
    writable_pool: asyncpg.Pool
    readonly_pool: asyncpg.Pool
    controller_factory: ControllerPipelineFactoryLike = field(
        default_factory=ControllerPipelineFactory
    )
    replay_engine: ReplayEngineLike | None = None
    strategy_loader: StrategyLoader | None = None
    cancel_probe: CancelProbe | None = None
    host_provider: HostProvider = socket.gethostname
    pid_provider: PidProvider = os.getpid

    def __post_init__(self) -> None:
        if self.replay_engine is None:
            self.replay_engine = MarketUniverseReplayEngine(pool=self.readonly_pool)

    async def execute(self, run_id: str) -> bool:
        claimed_run = await self._claim_run(run_id)
        if claimed_run is None:
            return False

        try:
            await asyncio.wait_for(
                self._execute_claimed(claimed_run),
                timeout=claimed_run.exec_config.time_budget,
            )
        except TimeoutError:
            await self._mark_failed(run_id, "time_budget_exceeded")
            return False
        except asyncio.CancelledError:
            await self._mark_failed(run_id, "cancelled")
            raise
        except Exception as exc:
            await self._mark_failed(run_id, _failure_reason(exc))
            return False

        await self._mark_completed(run_id)
        return True

    async def _execute_claimed(self, claimed_run: _ClaimedRun) -> None:
        if not claimed_run.spec.strategy_versions:
            msg = "no_strategy_versions"
            raise ValueError(msg)
        strategies = await self._load_strategies(claimed_run.spec.strategy_versions)
        accumulators: list[_StrategyAccumulator] = []
        for index, strategy in enumerate(strategies):
            if index == 0:
                await _maybe_call_cancel_probe(self.cancel_probe, "before_first_strategy")
            else:
                await _maybe_call_cancel_probe(self.cancel_probe, "between_strategies")
            accumulator = await self._run_strategy(
                strategy=strategy,
                spec=claimed_run.spec,
                exec_config=claimed_run.exec_config,
            )
            accumulators.append(accumulator)
        # atomic persist: either every strategy_runs row lands or none — prevents
        # partial success leaking into reports when a later strategy raises.
        await self._insert_strategy_runs_atomically(
            run_id=claimed_run.run_id,
            accumulators=accumulators,
        )
        await _maybe_call_cancel_probe(self.cancel_probe, "after_last_strategy")

    async def _run_strategy(
        self,
        *,
        strategy: ActiveStrategy,
        spec: BacktestSpec,
        exec_config: BacktestExecutionConfig,
    ) -> _StrategyAccumulator:
        replay_engine = self._required_replay_engine()
        pipeline = self.controller_factory.build(strategy)
        portfolio = _default_portfolio(strategy)
        accumulator = _StrategyAccumulator(
            strategy_id=strategy.strategy_id,
            strategy_version_id=strategy.strategy_version_id,
            execution_model=spec.execution_model,
        )

        async for signal in replay_engine.stream(spec, exec_config):
            emission = await pipeline.on_signal(signal, portfolio=portfolio)
            if emission is None:
                continue
            opportunity, decision = emission
            accumulator.record_decision(
                signal=signal,
                opportunity=opportunity,
                decision=decision,
            )
            try:
                order_state = await PaperActuator(
                    orderbooks={signal.market_id: signal.orderbook}
                ).execute(cast(Any, decision), portfolio)
            except InsufficientLiquidityError:
                continue
            fill = _fill_from_order(order_state, decision, signal)
            if fill is None:
                continue
            portfolio = _portfolio_with_fill(portfolio, fill)
            fill_price = cast(float | None, getattr(order_state, "fill_price", None))
            if fill_price is not None:
                accumulator.record_fill(
                    signal=signal,
                    opportunity=opportunity,
                    decision=decision,
                    fill_price=fill_price,
                )

        accumulator.finished_at = datetime.now(tz=UTC)
        return accumulator

    async def _claim_run(self, run_id: str) -> _ClaimedRun | None:
        connection = await self.writable_pool.acquire()
        try:
            async with connection.transaction():
                row = await connection.fetchrow(
                    """
                    WITH candidate AS (
                        SELECT run_id
                        FROM backtest_runs
                        WHERE run_id = $1::uuid
                          AND status = 'queued'
                        FOR UPDATE SKIP LOCKED
                    )
                    UPDATE backtest_runs AS runs
                    SET
                        status = 'running',
                        started_at = clock_timestamp(),
                        worker_pid = $2,
                        worker_host = $3
                    FROM candidate
                    WHERE runs.run_id = candidate.run_id
                    RETURNING runs.run_id, runs.spec_json, runs.exec_config_json
                    """,
                    run_id,
                    self.pid_provider(),
                    self.host_provider(),
                )
        finally:
            await self.writable_pool.release(connection)

        if row is None:
            return None
        return _ClaimedRun(
            run_id=str(cast(object, row["run_id"])),
            spec=_codec_deserialize_backtest_spec(row["spec_json"]),
            exec_config=_codec_deserialize_execution_config(row["exec_config_json"]),
        )

    async def _load_strategies(
        self,
        strategy_versions: tuple[StrategyVersionKey, ...],
    ) -> list[ActiveStrategy]:
        if self.strategy_loader is not None:
            return await self.strategy_loader(strategy_versions)

        connection = await self.readonly_pool.acquire()
        try:
            strategies: list[ActiveStrategy] = []
            for strategy_id, strategy_version_id in strategy_versions:
                row = await connection.fetchrow(
                    """
                    SELECT strategy_id, strategy_version_id, config_json
                    FROM strategy_versions
                    WHERE strategy_id = $1 AND strategy_version_id = $2
                    """,
                    strategy_id,
                    strategy_version_id,
                )
                if row is None:
                    msg = (
                        "BacktestRunner could not load strategy version "
                        f"{strategy_id}:{strategy_version_id}"
                    )
                    raise LookupError(msg)
                strategy = _strategy_from_config_json(row["config_json"])
                strategies.append(
                    ActiveStrategy(
                        strategy_id=cast(str, row["strategy_id"]),
                        strategy_version_id=cast(str, row["strategy_version_id"]),
                        config=strategy.config,
                        risk=strategy.risk,
                        eval_spec=strategy.eval_spec,
                        forecaster=strategy.forecaster,
                        market_selection=strategy.market_selection,
                    )
                )
        finally:
            await self.readonly_pool.release(connection)
        return strategies

    async def _insert_strategy_runs_atomically(
        self,
        *,
        run_id: str,
        accumulators: Sequence[_StrategyAccumulator],
    ) -> None:
        if not accumulators:
            return
        connection = await self.writable_pool.acquire()
        try:
            async with connection.transaction():
                for accumulator in accumulators:
                    await connection.execute(
                        """
                        INSERT INTO strategy_runs (
                            strategy_run_id,
                            run_id,
                            strategy_id,
                            strategy_version_id,
                            brier,
                            pnl_cum,
                            drawdown_max,
                            fill_rate,
                            slippage_bps,
                            opportunity_count,
                            decision_count,
                            fill_count,
                            portfolio_target_json,
                            started_at,
                            finished_at
                        ) VALUES (
                            $1::uuid, $2::uuid, $3, $4, $5, $6, $7, $8, $9,
                            $10, $11, $12, $13::jsonb, $14, $15
                        )
                        """,
                        *accumulator.as_insert_args(run_id=run_id),
                    )
        finally:
            await self.writable_pool.release(connection)

    async def _mark_completed(self, run_id: str) -> None:
        connection = await self.writable_pool.acquire()
        try:
            await connection.execute(
                """
                UPDATE backtest_runs
                SET status = 'completed',
                    finished_at = clock_timestamp(),
                    failure_reason = NULL
                WHERE run_id = $1::uuid
                """,
                run_id,
            )
        finally:
            await self.writable_pool.release(connection)

    async def _mark_failed(self, run_id: str, reason: str) -> None:
        connection = await self.writable_pool.acquire()
        try:
            await connection.execute(
                """
                UPDATE backtest_runs
                SET status = 'failed',
                    finished_at = clock_timestamp(),
                    failure_reason = $2
                WHERE run_id = $1::uuid
                """,
                run_id,
                reason,
            )
        finally:
            await self.writable_pool.release(connection)

    def _required_replay_engine(self) -> ReplayEngineLike:
        if self.replay_engine is None:
            msg = "BacktestRunner replay_engine is not configured"
            raise RuntimeError(msg)
        return self.replay_engine


def _default_portfolio(strategy: ActiveStrategy) -> Portfolio:
    starting_cash = max(1_000.0, strategy.risk.max_position_notional_usdc * 10.0)
    return Portfolio(
        total_usdc=starting_cash,
        free_usdc=starting_cash,
        locked_usdc=0.0,
        open_positions=[],
        max_drawdown_pct=0.0,
    )


def _fill_from_order(
    order_state: OrderState,
    decision: object,
    signal: MarketSignal,
) -> FillRecord | None:
    if order_state.status != OrderStatus.MATCHED.value or order_state.fill_price is None:
        return None
    if order_state.filled_size <= 0.0:
        return None
    if order_state.fill_price <= 0.0:
        return None

    return FillRecord(
        trade_id=order_state.order_id,
        order_id=order_state.order_id,
        decision_id=order_state.decision_id,
        market_id=order_state.market_id,
        token_id=order_state.token_id,
        venue=order_state.venue,
        side=cast(str, getattr(decision, "side")),
        fill_price=order_state.fill_price,
        fill_size=order_state.filled_size,
        executed_at=order_state.submitted_at,
        filled_at=order_state.last_updated_at,
        status=order_state.status,
        anomaly_flags=[],
        strategy_id=order_state.strategy_id,
        strategy_version_id=order_state.strategy_version_id,
        resolved_outcome=_resolved_outcome(signal),
    )


def _portfolio_with_fill(portfolio: Portfolio, fill: FillRecord) -> Portfolio:
    positions = list(portfolio.open_positions)
    fill_size = fill.fill_size
    contracts = _filled_contracts(fill)
    for index, position in enumerate(positions):
        if _same_position(position, fill):
            new_shares = position.shares_held + contracts
            avg_entry_price = (
                position.avg_entry_price * position.shares_held
                + fill.fill_price * contracts
            ) / new_shares
            positions[index] = replace(
                position,
                shares_held=new_shares,
                avg_entry_price=avg_entry_price,
                locked_usdc=position.locked_usdc + fill_size,
            )
            break
    else:
        positions.append(
            Position(
                market_id=fill.market_id,
                token_id=fill.token_id,
                venue=fill.venue,
                side=fill.side,
                shares_held=contracts,
                avg_entry_price=fill.fill_price,
                unrealized_pnl=0.0,
                locked_usdc=fill_size,
            )
        )

    return replace(
        portfolio,
        free_usdc=portfolio.free_usdc - fill_size,
        locked_usdc=portfolio.locked_usdc + fill_size,
        open_positions=positions,
    )


def _filled_contracts(fill: FillRecord) -> float:
    if fill.filled_contracts is not None:
        return fill.filled_contracts
    return fill.fill_size / fill.fill_price


def _same_position(position: Position, fill: FillRecord) -> bool:
    return (
        position.market_id == fill.market_id
        and position.token_id == fill.token_id
        and position.venue == fill.venue
        and position.side == fill.side
    )


async def _maybe_call_cancel_probe(
    probe: CancelProbe | None,
    point: CancelPoint,
) -> None:
    if probe is None:
        return
    outcome = probe(point)
    if asyncio.iscoroutine(outcome):
        await cast(Awaitable[None], outcome)


def _resolved_outcome(signal: MarketSignal) -> float | None:
    value = signal.external_signal.get("resolved_outcome")
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    return None


def _pnl_delta(
    *,
    signal: MarketSignal,
    opportunity_side: str,
    decision_size: float,
    fill_price: float,
    execution_model: ExecutionModel,
) -> Decimal:
    resolved_outcome = _resolved_outcome(signal)
    if resolved_outcome is None:
        return Decimal("0")

    notional = Decimal(str(decision_size))
    fill_price_decimal = Decimal(str(fill_price))
    resolved = Decimal(str(resolved_outcome))
    if opportunity_side == "yes":
        if fill_price_decimal <= 0:
            return Decimal("0")
        shares = notional / fill_price_decimal
        payout = shares * resolved
    else:
        no_price = Decimal("1") - fill_price_decimal
        if no_price <= 0:
            return Decimal("0")
        shares = notional / no_price
        payout = shares * (Decimal("1") - resolved)
    fee = Decimal(
        str(
            execution_model.fee_curve(
                price=float(fill_price_decimal),
                shares=float(shares),
            )
        )
    )
    return payout - notional - fee


def _failure_reason(exc: Exception) -> str:
    message = str(exc).strip()
    if message:
        return message
    return exc.__class__.__name__


__all__ = ["BacktestRunner"]
