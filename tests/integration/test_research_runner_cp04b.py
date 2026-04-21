from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
import json
import os
from typing import Any, cast
from uuid import uuid4

import asyncpg
import pytest

from pms.core.models import MarketSignal, Opportunity, Portfolio, TradeDecision
from pms.evaluation.metrics import StrategyVersionKey
from pms.research.runner import BacktestRunner
from pms.strategies.projections import (
    ActiveStrategy,
    EvalSpec,
    ForecasterSpec,
    MarketSelectionSpec,
    RiskParams,
    StrategyConfig,
)

PMS_TEST_DATABASE_URL = os.environ.get("PMS_TEST_DATABASE_URL")

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.environ.get("PMS_RUN_INTEGRATION") != "1",
        reason="set PMS_RUN_INTEGRATION=1 to run PostgreSQL integration tests",
    ),
    pytest.mark.skipif(
        PMS_TEST_DATABASE_URL is None,
        reason="set PMS_TEST_DATABASE_URL to the compose-backed PostgreSQL URI",
    ),
]


@dataclass
class TrackingPool:
    inner: asyncpg.Pool
    acquire_calls: int = 0
    release_calls: int = 0

    async def acquire(self) -> asyncpg.Connection:
        self.acquire_calls += 1
        return await self.inner.acquire()

    async def release(self, connection: asyncpg.Connection) -> None:
        self.release_calls += 1
        await self.inner.release(connection)


@dataclass
class StaticReplayEngine:
    signals: list[MarketSignal]
    delay_s: float = 0.0

    async def stream(self, spec: object, exec_config: object) -> AsyncIterator[MarketSignal]:
        del spec, exec_config
        for signal in self.signals:
            if self.delay_s > 0.0:
                await asyncio.sleep(self.delay_s)
            yield signal


class BlockingPipeline:
    def __init__(
        self,
        *,
        strategy_id: str,
        strategy_version_id: str,
        gate: asyncio.Event | None = None,
        started_event: asyncio.Event | None = None,
        completed_event: asyncio.Event | None = None,
        error: Exception | None = None,
    ) -> None:
        self._strategy_id = strategy_id
        self._strategy_version_id = strategy_version_id
        self._gate = gate
        self._started_event = started_event
        self._completed_event = completed_event
        self._error = error
        self._emitted = False

    async def on_signal(
        self,
        signal: MarketSignal,
        portfolio: Portfolio | None = None,
    ) -> tuple[Opportunity, TradeDecision] | None:
        del portfolio
        if self._emitted:
            return None
        self._emitted = True
        if self._started_event is not None:
            self._started_event.set()
        if self._gate is not None:
            await self._gate.wait()
        if self._error is not None:
            raise self._error
        if self._completed_event is not None:
            self._completed_event.set()
        opportunity = Opportunity(
            opportunity_id=f"opp-{self._strategy_id}",
            market_id=signal.market_id,
            token_id=cast(str, signal.token_id),
            side="yes",
            selected_factor_values={},
            expected_edge=0.1,
            rationale="cp04b-test",
            target_size_usdc=10.0,
            expiry=signal.resolves_at,
            staleness_policy="research",
            strategy_id=self._strategy_id,
            strategy_version_id=self._strategy_version_id,
            created_at=signal.fetched_at,
        )
        decision = TradeDecision(
            decision_id=f"decision-{self._strategy_id}",
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
            opportunity_id=opportunity.opportunity_id,
            strategy_id=self._strategy_id,
            strategy_version_id=self._strategy_version_id,
            model_id="rules",
        )
        return opportunity, decision


@dataclass
class PipelineFactory:
    builders: dict[str, BlockingPipeline]

    def build(self, strategy: ActiveStrategy) -> BlockingPipeline:
        return self.builders[strategy.strategy_id]


def _active_strategy(
    *,
    strategy_id: str,
    strategy_version_id: str,
) -> ActiveStrategy:
    return ActiveStrategy(
        strategy_id=strategy_id,
        strategy_version_id=strategy_version_id,
        config=StrategyConfig(
            strategy_id=strategy_id,
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


def _signal() -> MarketSignal:
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
        fetched_at=datetime(2026, 4, 20, tzinfo=UTC),
        market_status="open",
    )


def _spec_payload(strategy_versions: tuple[StrategyVersionKey, ...]) -> dict[str, object]:
    return {
        "strategy_versions": [list(item) for item in strategy_versions],
        "dataset": {
            "source": "fixture",
            "version": "v1",
            "coverage_start": "2026-04-01T00:00:00+00:00",
            "coverage_end": "2026-04-30T00:00:00+00:00",
            "market_universe_filter": {"market_ids": ["research-runner-market"]},
            "data_quality_gaps": [],
        },
        "execution_model": {
            "fee_rate": 0.0,
            "slippage_bps": 5.0,
            "latency_ms": 0.0,
            "staleness_ms": 60000.0,
            "fill_policy": "immediate_or_cancel",
        },
        "risk_policy": {
            "max_position_notional_usdc": 100.0,
            "max_daily_drawdown_pct": 2.5,
            "min_order_size_usdc": 1.0,
        },
        "date_range_start": "2026-04-01T00:00:00+00:00",
        "date_range_end": "2026-04-30T00:00:00+00:00",
    }


async def _insert_run(
    pool: asyncpg.Pool,
    *,
    run_id: str,
    strategy_versions: tuple[StrategyVersionKey, ...],
    time_budget: int = 1800,
) -> None:
    async with pool.acquire() as connection:
        await connection.execute(
            """
            INSERT INTO backtest_runs (
                run_id,
                spec_hash,
                status,
                strategy_ids,
                date_range_start,
                date_range_end,
                exec_config_json,
                spec_json
            ) VALUES (
                $1::uuid,
                $2,
                'queued',
                $3::text[],
                $4,
                $5,
                $6::jsonb,
                $7::jsonb
            )
            """,
            run_id,
            f"spec-{run_id}",
            [strategy_id for strategy_id, _ in strategy_versions],
            datetime(2026, 4, 1, tzinfo=UTC),
            datetime(2026, 4, 30, tzinfo=UTC),
            json.dumps({"chunk_days": 7, "time_budget": time_budget}),
            json.dumps(_spec_payload(strategy_versions)),
        )


async def _load_strategies(
    strategies: dict[StrategyVersionKey, ActiveStrategy],
    keys: tuple[StrategyVersionKey, ...],
) -> list[ActiveStrategy]:
    return [strategies[key] for key in keys]


async def _strategy_run_count(pool: asyncpg.Pool, run_id: str) -> int:
    async with pool.acquire() as connection:
        count = await connection.fetchval(
            """
            SELECT COUNT(*)
            FROM strategy_runs
            WHERE run_id = $1::uuid
            """,
            run_id,
        )
    assert isinstance(count, int)
    return count


async def _run_status(pool: asyncpg.Pool, run_id: str) -> tuple[str, str | None]:
    async with pool.acquire() as connection:
        row = await connection.fetchrow(
            """
            SELECT status, failure_reason
            FROM backtest_runs
            WHERE run_id = $1::uuid
            """,
            run_id,
        )
    assert row is not None
    return cast(str, row["status"]), cast(str | None, row["failure_reason"])


@pytest.mark.asyncio(loop_scope="session")
async def test_backtest_runner_claims_one_queued_run_under_race(
    pg_pool: asyncpg.Pool,
) -> None:
    run_id = str(uuid4())
    strategy_keys: tuple[StrategyVersionKey, ...] = (("alpha", "alpha-v1"),)
    await _insert_run(pg_pool, run_id=run_id, strategy_versions=strategy_keys)
    strategies = {
        strategy_keys[0]: _active_strategy(
            strategy_id="alpha",
            strategy_version_id="alpha-v1",
        )
    }

    runner_one = BacktestRunner(
        writable_pool=pg_pool,
        readonly_pool=pg_pool,
        replay_engine=StaticReplayEngine([]),
        strategy_loader=lambda keys: _load_strategies(strategies, keys),
        controller_factory=PipelineFactory(
            {
                "alpha": BlockingPipeline(
                    strategy_id="alpha",
                    strategy_version_id="alpha-v1",
                )
            }
        ),
    )
    runner_two = BacktestRunner(
        writable_pool=pg_pool,
        readonly_pool=pg_pool,
        replay_engine=StaticReplayEngine([]),
        strategy_loader=lambda keys: _load_strategies(strategies, keys),
        controller_factory=PipelineFactory(
            {
                "alpha": BlockingPipeline(
                    strategy_id="alpha",
                    strategy_version_id="alpha-v1",
                )
            }
        ),
    )

    results = await asyncio.gather(
        runner_one.execute(run_id),
        runner_two.execute(run_id),
    )

    assert sorted(results) == [False, True]
    assert await _run_status(pg_pool, run_id) == ("completed", None)


@pytest.mark.asyncio(loop_scope="session")
async def test_backtest_runner_rejects_empty_strategy_runs_at_application_layer(
    pg_pool: asyncpg.Pool,
) -> None:
    run_id = str(uuid4())
    await _insert_run(pg_pool, run_id=run_id, strategy_versions=())

    runner = BacktestRunner(
        writable_pool=pg_pool,
        readonly_pool=pg_pool,
        replay_engine=StaticReplayEngine([]),
    )

    assert await runner.execute(run_id) is False
    assert await _run_status(pg_pool, run_id) == (
        "failed",
        "BacktestSpec.strategy_versions must be non-empty",
    )
    assert await _strategy_run_count(pg_pool, run_id) == 0


@pytest.mark.asyncio(loop_scope="session")
async def test_backtest_runner_defers_strategy_rows_until_all_strategies_finish(
    pg_pool: asyncpg.Pool,
) -> None:
    run_id = str(uuid4())
    strategy_keys: tuple[StrategyVersionKey, ...] = (
        ("alpha", "alpha-v1"),
        ("beta", "beta-v1"),
        ("gamma", "gamma-v1"),
    )
    await _insert_run(pg_pool, run_id=run_id, strategy_versions=strategy_keys)

    alpha_completed = asyncio.Event()
    beta_started = asyncio.Event()
    beta_gate = asyncio.Event()
    gamma_started = asyncio.Event()
    gamma_gate = asyncio.Event()
    strategies = {
        key: _active_strategy(strategy_id=key[0], strategy_version_id=key[1])
        for key in strategy_keys
    }
    runner = BacktestRunner(
        writable_pool=TrackingPool(pg_pool),
        readonly_pool=TrackingPool(pg_pool),
        replay_engine=StaticReplayEngine([_signal()]),
        strategy_loader=lambda keys: _load_strategies(strategies, keys),
        controller_factory=PipelineFactory(
            {
                "alpha": BlockingPipeline(
                    strategy_id="alpha",
                    strategy_version_id="alpha-v1",
                    completed_event=alpha_completed,
                ),
                "beta": BlockingPipeline(
                    strategy_id="beta",
                    strategy_version_id="beta-v1",
                    gate=beta_gate,
                    started_event=beta_started,
                ),
                "gamma": BlockingPipeline(
                    strategy_id="gamma",
                    strategy_version_id="gamma-v1",
                    gate=gamma_gate,
                    started_event=gamma_started,
                ),
            }
        ),
    )

    task = asyncio.create_task(runner.execute(run_id))
    await asyncio.wait_for(alpha_completed.wait(), timeout=1.0)
    await asyncio.wait_for(beta_started.wait(), timeout=1.0)
    assert await _strategy_run_count(pg_pool, run_id) == 0

    beta_gate.set()
    await asyncio.wait_for(gamma_started.wait(), timeout=1.0)
    assert await _strategy_run_count(pg_pool, run_id) == 0

    gamma_gate.set()
    assert await task is True
    assert await _strategy_run_count(pg_pool, run_id) == 3
    assert await _run_status(pg_pool, run_id) == ("completed", None)
    assert cast(TrackingPool, runner.writable_pool).acquire_calls == cast(
        TrackingPool, runner.writable_pool
    ).release_calls
    assert cast(TrackingPool, runner.readonly_pool).acquire_calls == cast(
        TrackingPool, runner.readonly_pool
    ).release_calls


@pytest.mark.asyncio(loop_scope="session")
async def test_backtest_runner_discards_partial_results_on_strategy_failure(
    pg_pool: asyncpg.Pool,
) -> None:
    run_id = str(uuid4())
    strategy_keys: tuple[StrategyVersionKey, ...] = (
        ("alpha", "alpha-v1"),
        ("beta", "beta-v1"),
        ("gamma", "gamma-v1"),
    )
    await _insert_run(pg_pool, run_id=run_id, strategy_versions=strategy_keys)

    strategies = {
        key: _active_strategy(strategy_id=key[0], strategy_version_id=key[1])
        for key in strategy_keys
    }
    tracking_writable = TrackingPool(pg_pool)
    tracking_readonly = TrackingPool(pg_pool)
    runner = BacktestRunner(
        writable_pool=tracking_writable,
        readonly_pool=tracking_readonly,
        replay_engine=StaticReplayEngine([_signal()]),
        strategy_loader=lambda keys: _load_strategies(strategies, keys),
        controller_factory=PipelineFactory(
            {
                "alpha": BlockingPipeline(
                    strategy_id="alpha",
                    strategy_version_id="alpha-v1",
                ),
                "beta": BlockingPipeline(
                    strategy_id="beta",
                    strategy_version_id="beta-v1",
                    error=RuntimeError("strategy-two boom"),
                ),
                "gamma": BlockingPipeline(
                    strategy_id="gamma",
                    strategy_version_id="gamma-v1",
                ),
            }
        ),
    )

    assert await runner.execute(run_id) is False
    assert await _run_status(pg_pool, run_id) == ("failed", "strategy-two boom")
    assert await _strategy_run_count(pg_pool, run_id) == 0
    assert tracking_writable.acquire_calls == tracking_writable.release_calls
    assert tracking_readonly.acquire_calls == tracking_readonly.release_calls


@pytest.mark.asyncio(loop_scope="session")
async def test_backtest_runner_marks_time_budget_exceeded(
    pg_pool: asyncpg.Pool,
) -> None:
    run_id = str(uuid4())
    strategy_keys: tuple[StrategyVersionKey, ...] = (("alpha", "alpha-v1"),)
    await _insert_run(
        pg_pool,
        run_id=run_id,
        strategy_versions=strategy_keys,
        time_budget=1,
    )

    strategies = {
        strategy_keys[0]: _active_strategy(
            strategy_id="alpha",
            strategy_version_id="alpha-v1",
        )
    }
    tracking_writable = TrackingPool(pg_pool)
    tracking_readonly = TrackingPool(pg_pool)
    runner = BacktestRunner(
        writable_pool=tracking_writable,
        readonly_pool=tracking_readonly,
        replay_engine=StaticReplayEngine([_signal()], delay_s=2.0),
        strategy_loader=lambda keys: _load_strategies(strategies, keys),
        controller_factory=PipelineFactory(
            {
                "alpha": BlockingPipeline(
                    strategy_id="alpha",
                    strategy_version_id="alpha-v1",
                )
            }
        ),
    )

    assert await runner.execute(run_id) is False
    assert await _run_status(pg_pool, run_id) == ("failed", "time_budget_exceeded")
    assert tracking_writable.acquire_calls == tracking_writable.release_calls
    assert tracking_readonly.acquire_calls == tracking_readonly.release_calls


@pytest.mark.asyncio(loop_scope="session")
@pytest.mark.parametrize(
    ("cancel_point", "expected_rows"),
    [
        ("before_first_strategy", 0),
        ("between_strategies", 0),
        ("after_last_strategy", 2),
    ],
)
async def test_backtest_runner_marks_failed_and_releases_connections_on_cancel(
    pg_pool: asyncpg.Pool,
    cancel_point: str,
    expected_rows: int,
) -> None:
    run_id = str(uuid4())
    strategy_keys: tuple[StrategyVersionKey, ...] = (
        ("alpha", "alpha-v1"),
        ("beta", "beta-v1"),
    )
    await _insert_run(pg_pool, run_id=run_id, strategy_versions=strategy_keys)

    strategies = {
        key: _active_strategy(strategy_id=key[0], strategy_version_id=key[1])
        for key in strategy_keys
    }

    async def cancel_probe(point: str) -> None:
        if point == cancel_point:
            raise asyncio.CancelledError

    tracking_writable = TrackingPool(pg_pool)
    tracking_readonly = TrackingPool(pg_pool)
    runner = BacktestRunner(
        writable_pool=tracking_writable,
        readonly_pool=tracking_readonly,
        replay_engine=StaticReplayEngine([_signal()]),
        strategy_loader=lambda keys: _load_strategies(strategies, keys),
        controller_factory=PipelineFactory(
            {
                "alpha": BlockingPipeline(
                    strategy_id="alpha",
                    strategy_version_id="alpha-v1",
                ),
                "beta": BlockingPipeline(
                    strategy_id="beta",
                    strategy_version_id="beta-v1",
                ),
            }
        ),
        cancel_probe=cast(Callable[[str], Awaitable[None]], cancel_probe),
    )

    with pytest.raises(asyncio.CancelledError):
        await runner.execute(run_id)

    assert await _run_status(pg_pool, run_id) == ("failed", "cancelled")
    assert await _strategy_run_count(pg_pool, run_id) == expected_rows
    assert tracking_writable.acquire_calls == tracking_writable.release_calls
    assert tracking_readonly.acquire_calls == tracking_readonly.release_calls
