from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime
from pathlib import Path
import re
from typing import Any, cast

import pytest

from pms.core.models import MarketSignal, Opportunity, Portfolio, TradeDecision
from pms.research.entities import (
    PortfolioTarget,
    deserialize_portfolio_target_json,
    serialize_portfolio_target_json,
)
from pms.research.runner import BacktestRunner
from pms.research.specs import (
    BacktestDataset,
    BacktestExecutionConfig,
    BacktestSpec,
    ExecutionModel,
)
from pms.strategies.projections import (
    ActiveStrategy,
    EvalSpec,
    ForecasterSpec,
    MarketSelectionSpec,
    RiskParams,
    StrategyConfig,
)


def test_portfolio_target_round_trips_through_json() -> None:
    target = PortfolioTarget(
        strategy_id="alpha",
        strategy_version_id="alpha-v1",
        targets={
            ("market-a", "token-a", "buy_yes", datetime(2026, 4, 20, tzinfo=UTC)): 12.5,
            ("market-b", "token-b", "buy_no", datetime(2026, 4, 21, tzinfo=UTC)): 7.0,
        },
    )

    encoded = serialize_portfolio_target_json(target)
    decoded = deserialize_portfolio_target_json(
        strategy_id="alpha",
        strategy_version_id="alpha-v1",
        raw_value=encoded,
    )

    assert decoded == target


def test_portfolio_target_round_trips_empty_targets() -> None:
    target = PortfolioTarget(
        strategy_id="alpha",
        strategy_version_id="alpha-v1",
        targets={},
    )

    encoded = serialize_portfolio_target_json(target)
    decoded = deserialize_portfolio_target_json(
        strategy_id="alpha",
        strategy_version_id="alpha-v1",
        raw_value=encoded,
    )

    assert decoded == target


def test_portfolio_target_remains_research_only() -> None:
    forbidden_pattern = re.compile(
        r"class PortfolioTarget\b|from pms\.research.*import.*PortfolioTarget"
    )

    for root in (
        Path("src/pms/controller"),
        Path("src/pms/actuator"),
        Path("src/pms/sensor"),
    ):
        for path in root.rglob("*.py"):
            assert forbidden_pattern.search(path.read_text(encoding="utf-8")) is None


class _ReplayEngine:
    def __init__(self, signals: list[MarketSignal]) -> None:
        self._signals = signals

    async def stream(
        self,
        spec: BacktestSpec,
        exec_config: BacktestExecutionConfig,
    ) -> AsyncIterator[MarketSignal]:
        del spec, exec_config
        for signal in self._signals:
            yield signal


class _PortfolioRecordingPipeline:
    def __init__(self) -> None:
        self.portfolios: list[Portfolio] = []

    async def on_signal(
        self,
        signal: MarketSignal,
        portfolio: Portfolio | None = None,
    ) -> tuple[Opportunity, TradeDecision] | None:
        assert portfolio is not None
        self.portfolios.append(portfolio)
        emission_index = len(self.portfolios)
        return (
            Opportunity(
                opportunity_id=f"opp-{emission_index}",
                market_id=signal.market_id,
                token_id=cast(str, signal.token_id),
                side="yes",
                selected_factor_values={},
                expected_edge=0.1,
                rationale="portfolio-state-check",
                target_size_usdc=10.0,
                expiry=signal.resolves_at,
                staleness_policy="research",
                strategy_id="alpha",
                strategy_version_id="alpha-v1",
                created_at=signal.fetched_at,
            ),
            TradeDecision(
                decision_id=f"decision-{emission_index}",
                market_id=signal.market_id,
                token_id=signal.token_id,
                venue=signal.venue,
                side="BUY",
                price=signal.yes_price,
                size=10.0,
                order_type="limit",
                max_slippage_bps=50,
                stop_conditions=[],
                prob_estimate=0.7,
                expected_edge=0.2,
                time_in_force="GTC",
                opportunity_id=f"opp-{emission_index}",
                strategy_id="alpha",
                strategy_version_id="alpha-v1",
                model_id="rules",
            ),
        )


class _PipelineFactory:
    def __init__(self, pipeline: _PortfolioRecordingPipeline) -> None:
        self._pipeline = pipeline

    def build(self, strategy: ActiveStrategy) -> _PortfolioRecordingPipeline:
        del strategy
        return self._pipeline


def _active_strategy() -> ActiveStrategy:
    return ActiveStrategy(
        strategy_id="alpha",
        strategy_version_id="alpha-v1",
        config=StrategyConfig(
            strategy_id="alpha",
            factor_composition=(),
            metadata=(("owner", "test"),),
        ),
        risk=RiskParams(
            max_position_notional_usdc=100.0,
            max_daily_drawdown_pct=2.5,
            min_order_size_usdc=1.0,
        ),
        eval_spec=EvalSpec(metrics=("brier", "pnl")),
        forecaster=ForecasterSpec(forecasters=(("rules", ()),)),
        market_selection=MarketSelectionSpec(
            venue="polymarket",
            resolution_time_max_horizon_days=7,
            volume_min_usdc=500.0,
        ),
    )


def _signal(ts: datetime) -> MarketSignal:
    return MarketSignal(
        market_id="research-runner-market",
        token_id="yes-token",
        venue="polymarket",
        title="Will CP04b runner tests pass?",
        yes_price=0.4,
        volume_24h=1000.0,
        resolves_at=datetime(2026, 4, 30, tzinfo=UTC),
        orderbook={
            "bids": [{"price": 0.39, "size": 100.0}],
            "asks": [{"price": 0.41, "size": 100.0}],
        },
        external_signal={"resolved_outcome": 1.0},
        fetched_at=ts,
        market_status="open",
    )


def _spec() -> BacktestSpec:
    return BacktestSpec(
        strategy_versions=(("alpha", "alpha-v1"),),
        dataset=BacktestDataset(
            source="fixture",
            version="v1",
            coverage_start=datetime(2026, 4, 1, tzinfo=UTC),
            coverage_end=datetime(2026, 4, 30, tzinfo=UTC),
            market_universe_filter={"market_ids": ["research-runner-market"]},
            data_quality_gaps=(),
        ),
        execution_model=ExecutionModel(
            fee_rate=0.0,
            slippage_bps=5.0,
            latency_ms=0.0,
            staleness_ms=60000.0,
            fill_policy="immediate_or_cancel",
        ),
        risk_policy=RiskParams(
            max_position_notional_usdc=100.0,
            max_daily_drawdown_pct=2.5,
            min_order_size_usdc=1.0,
        ),
        date_range_start=datetime(2026, 4, 1, tzinfo=UTC),
        date_range_end=datetime(2026, 4, 30, tzinfo=UTC),
    )


@pytest.mark.asyncio
async def test_backtest_runner_updates_portfolio_between_replay_signals() -> None:
    pipeline = _PortfolioRecordingPipeline()
    runner = BacktestRunner(
        writable_pool=cast(Any, object()),
        readonly_pool=cast(Any, object()),
        replay_engine=_ReplayEngine(
            [
                _signal(datetime(2026, 4, 20, 0, 0, tzinfo=UTC)),
                _signal(datetime(2026, 4, 20, 0, 1, tzinfo=UTC)),
            ]
        ),
        controller_factory=_PipelineFactory(pipeline),
    )

    accumulator = await runner._run_strategy(
        strategy=_active_strategy(),
        spec=_spec(),
        exec_config=BacktestExecutionConfig(),
    )

    assert accumulator.fill_count == 2
    assert len(pipeline.portfolios) == 2
    assert pipeline.portfolios[0].free_usdc == pytest.approx(1000.0)
    assert pipeline.portfolios[0].open_positions == []
    assert pipeline.portfolios[1].free_usdc == pytest.approx(990.0)
    assert pipeline.portfolios[1].locked_usdc == pytest.approx(10.0)
    assert len(pipeline.portfolios[1].open_positions) == 1
    assert pipeline.portfolios[1].open_positions[0].shares_held == pytest.approx(
        10.0 / 0.41
    )


@pytest.mark.asyncio
async def test_backtest_runner_propagates_unexpected_actuator_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pipeline = _PortfolioRecordingPipeline()
    runner = BacktestRunner(
        writable_pool=cast(Any, object()),
        readonly_pool=cast(Any, object()),
        replay_engine=_ReplayEngine([_signal(datetime(2026, 4, 20, 0, 0, tzinfo=UTC))]),
        controller_factory=_PipelineFactory(pipeline),
    )

    async def _boom(self: object, decision: object, portfolio: object | None = None) -> object:
        del self, decision, portfolio
        raise RuntimeError("actuator exploded")

    monkeypatch.setattr("pms.research.runner.PaperActuator.execute", _boom)

    with pytest.raises(RuntimeError, match="actuator exploded"):
        await runner._run_strategy(
            strategy=_active_strategy(),
            spec=_spec(),
            exec_config=BacktestExecutionConfig(),
        )


class _InsertTrackingTransaction:
    def __init__(self, journal: list[str]) -> None:
        self._journal = journal

    async def __aenter__(self) -> None:
        self._journal.append("BEGIN")

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> bool:
        if exc_type is not None:
            self._journal.append("ROLLBACK")
        else:
            self._journal.append("COMMIT")
        return False  # propagate exceptions


class _InsertTrackingConnection:
    def __init__(self, journal: list[str]) -> None:
        self._journal = journal

    def transaction(self) -> _InsertTrackingTransaction:
        return _InsertTrackingTransaction(self._journal)

    async def execute(self, query: str, *args: object) -> str:
        del args
        if "INSERT INTO strategy_runs" in query:
            self._journal.append("INSERT strategy_runs")
        return "INSERT 1"


class _InsertTrackingPool:
    def __init__(self, journal: list[str]) -> None:
        self._journal = journal
        self._connection = _InsertTrackingConnection(journal)

    async def acquire(self) -> _InsertTrackingConnection:
        return self._connection

    async def release(self, connection: _InsertTrackingConnection) -> None:
        del connection


@pytest.mark.asyncio
async def test_insert_strategy_runs_atomically_rolls_back_on_mid_batch_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A strategy_runs insert batch must be all-or-nothing: if a later row
    raises, none of the earlier rows stay committed."""
    journal: list[str] = []
    pool = _InsertTrackingPool(journal)
    runner = BacktestRunner(
        writable_pool=cast(Any, pool),
        readonly_pool=cast(Any, object()),
    )

    # Build two accumulators; make the second one's insert raise.
    from pms.research.runner import _StrategyAccumulator

    acc_one = _StrategyAccumulator(
        strategy_id="alpha",
        strategy_version_id="v1",
        execution_model=ExecutionModel.polymarket_paper(),
    )
    acc_two = _StrategyAccumulator(
        strategy_id="beta",
        strategy_version_id="v1",
        execution_model=ExecutionModel.polymarket_paper(),
    )

    call_count = {"execute": 0}
    original_execute = pool._connection.execute

    async def _maybe_boom(query: str, *args: object) -> str:
        call_count["execute"] += 1
        if call_count["execute"] == 2:
            raise RuntimeError("simulated DB failure")
        return await original_execute(query, *args)

    monkeypatch.setattr(pool._connection, "execute", _maybe_boom)

    with pytest.raises(RuntimeError, match="simulated DB failure"):
        await runner._insert_strategy_runs_atomically(
            run_id="11111111-1111-1111-1111-111111111111",
            accumulators=[acc_one, acc_two],
        )

    assert journal[0] == "BEGIN"
    assert journal[-1] == "ROLLBACK"
    assert "COMMIT" not in journal


def _signal_without_resolution(ts: datetime) -> MarketSignal:
    base = _signal(ts)
    return MarketSignal(
        market_id=base.market_id,
        token_id=base.token_id,
        venue=base.venue,
        title=base.title,
        yes_price=base.yes_price,
        volume_24h=base.volume_24h,
        resolves_at=base.resolves_at,
        orderbook=base.orderbook,
        external_signal={"raw_event_type": "price_change"},  # no resolved_outcome
        fetched_at=base.fetched_at,
        market_status=base.market_status,
    )


@pytest.mark.asyncio
async def test_strategy_accumulator_emits_null_pnl_when_no_resolution_observed() -> None:
    """When the replay stream supplies no `resolved_outcome` (e.g. running on
    live-captured data for a window that hasn't resolved yet), cumulative_pnl
    must surface as NULL rather than a misleading 0.0."""
    pipeline = _PortfolioRecordingPipeline()
    runner = BacktestRunner(
        writable_pool=cast(Any, object()),
        readonly_pool=cast(Any, object()),
        replay_engine=_ReplayEngine(
            [_signal_without_resolution(datetime(2026, 4, 20, 0, 0, tzinfo=UTC))]
        ),
        controller_factory=_PipelineFactory(pipeline),
    )

    accumulator = await runner._run_strategy(
        strategy=_active_strategy(),
        spec=_spec(),
        exec_config=BacktestExecutionConfig(),
    )

    args = accumulator.as_insert_args(run_id="11111111-1111-1111-1111-111111111111")
    # args order: strategy_run_id, run_id, strategy_id, strategy_version_id,
    #             brier, pnl_cum, drawdown_max, fill_rate, slippage_bps, ...
    assert args[4] is None, "brier must be NULL when no resolutions observed"
    assert args[5] is None, "pnl_cum must be NULL when no resolutions observed"


@pytest.mark.asyncio
async def test_strategy_accumulator_emits_numeric_pnl_when_any_resolution_observed() -> None:
    """Calibration: pnl_cum is NOT nulled when at least one fill has a
    resolved_outcome. The `as_insert_args` nulling must not swallow real data."""
    pipeline = _PortfolioRecordingPipeline()
    runner = BacktestRunner(
        writable_pool=cast(Any, object()),
        readonly_pool=cast(Any, object()),
        replay_engine=_ReplayEngine(
            [_signal(datetime(2026, 4, 20, 0, 0, tzinfo=UTC))]
        ),
        controller_factory=_PipelineFactory(pipeline),
    )

    accumulator = await runner._run_strategy(
        strategy=_active_strategy(),
        spec=_spec(),
        exec_config=BacktestExecutionConfig(),
    )

    args = accumulator.as_insert_args(run_id="11111111-1111-1111-1111-111111111111")
    assert args[5] is not None, "pnl_cum must remain numeric when resolutions observed"
