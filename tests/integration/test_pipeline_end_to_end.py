from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import pytest

from pms.config import PMSSettings, RiskSettings
from pms.core.enums import MarketStatus, RunMode
from pms.core.models import MarketSignal
from pms.runner import Runner
from pms.storage.eval_store import EvalStore
from pms.storage.feedback_store import FeedbackStore
from tests.support.fake_stores import InMemoryEvalStore, InMemoryFeedbackStore


FIXTURE_PATH = Path("tests/fixtures/polymarket_7day_synthetic.jsonl")


class HoldingSensor:
    def __aiter__(self) -> AsyncIterator[MarketSignal]:
        return self._iterate()

    async def _iterate(self) -> AsyncIterator[MarketSignal]:
        while True:
            await asyncio.sleep(60.0)
            yield _signal()


class OneShotSensor:
    def __init__(self, signal: MarketSignal) -> None:
        self.signal = signal

    def __aiter__(self) -> AsyncIterator[MarketSignal]:
        return self._iterate()

    async def _iterate(self) -> AsyncIterator[MarketSignal]:
        yield self.signal


def _settings(mode: RunMode) -> PMSSettings:
    return PMSSettings(
        mode=mode,
        auto_migrate_default_v2=False,
        risk=RiskSettings(
            max_position_per_market=1000.0,
            max_total_exposure=10_000.0,
        ),
    )


def _signal(
    *,
    market_id: str = "runner-hold",
    orderbook: dict[str, Any] | None = None,
    external_signal: dict[str, Any] | None = None,
) -> MarketSignal:
    return MarketSignal(
        market_id=market_id,
        token_id="yes-token",
        venue="polymarket",
        title="Will the runner keep tasks alive?",
        yes_price=0.4,
        volume_24h=1000.0,
        resolves_at=datetime(2026, 4, 20, tzinfo=UTC),
        orderbook=orderbook or {
            "bids": [{"price": 0.39, "size": 100.0}],
            "asks": [{"price": 0.41, "size": 100.0}],
        },
        external_signal=external_signal
        or {"metaculus_prob": 0.7, "resolved_outcome": 1.0},
        fetched_at=datetime(2026, 4, 14, tzinfo=UTC),
        market_status=MarketStatus.OPEN.value,
    )


@pytest.mark.asyncio
async def test_runner_start_stop_and_switch_mode_preserves_tasks(
    tmp_path: Path,
) -> None:
    runner = Runner(
        config=_settings(RunMode.BACKTEST),
        sensors=[HoldingSensor()],
        eval_store=cast(EvalStore, InMemoryEvalStore()),
        feedback_store=cast(FeedbackStore, InMemoryFeedbackStore()),
    )

    initial_started_at = runner.state.runner_started_at
    assert initial_started_at is None

    await runner.start()

    started_at = runner.state.runner_started_at
    assert started_at is not None
    assert runner.sensor_stream.tasks
    assert runner.controller_task is not None
    assert runner.actuator_task is not None
    assert runner.evaluator_task is not None

    sensor_tasks = runner.sensor_stream.tasks
    controller_task = runner.controller_task
    actuator_task = runner.actuator_task

    runner.switch_mode(RunMode.PAPER)

    assert runner.state.mode == RunMode.PAPER
    assert runner.sensor_stream.tasks == sensor_tasks
    assert runner.controller_task is controller_task
    assert runner.actuator_task is actuator_task

    await asyncio.wait_for(runner.stop(), timeout=5.0)

    assert all(task.done() for task in sensor_tasks)
    assert runner.tasks == ()


@pytest.mark.asyncio
async def test_backtest_end_to_end_fixture_produces_decisions_and_eval_records(
    tmp_path: Path,
) -> None:
    runner = Runner(
        config=_settings(RunMode.BACKTEST),
        historical_data_path=FIXTURE_PATH,
        eval_store=cast(EvalStore, InMemoryEvalStore()),
        feedback_store=cast(FeedbackStore, InMemoryFeedbackStore()),
    )

    await runner.start()
    await asyncio.wait_for(runner.wait_until_idle(), timeout=5.0)
    await asyncio.wait_for(runner.stop(), timeout=5.0)

    records = await cast(InMemoryEvalStore, runner.eval_store).all()

    assert len(runner.state.signals) == 100
    assert len(runner.state.decisions) >= 10
    assert {fill.resolved_outcome for fill in runner.state.fills} <= {0.0, 1.0}
    assert len(records) == len(runner.state.fills)
    assert all(0.0 <= record.brier_score <= 1.0 for record in records)
    assert {order.raw_status for order in runner.state.orders} <= {"matched", "ioc_unfilled"}
    locked_size = sum(fill.fill_notional_usdc for fill in runner.state.fills)
    assert runner.portfolio.locked_usdc == pytest.approx(locked_size)
    assert runner.portfolio.free_usdc == pytest.approx(1000.0 - locked_size)
    assert sum(
        position.locked_usdc for position in runner.portfolio.open_positions
    ) == pytest.approx(locked_size)
    assert sum(
        position.shares_held * position.avg_entry_price
        for position in runner.portfolio.open_positions
    ) == pytest.approx(locked_size)


@pytest.mark.asyncio
async def test_paper_runner_records_liquidity_rejections_in_order_state(
    tmp_path: Path,
) -> None:
    runner = Runner(
        config=_settings(RunMode.PAPER),
        sensors=[
            OneShotSensor(
                _signal(
                    market_id="paper-empty-book",
                    orderbook={"bids": [], "asks": []},
                    external_signal={"fair_value": 0.7, "resolved_outcome": 1.0},
                )
            )
        ],
        eval_store=cast(EvalStore, InMemoryEvalStore()),
        feedback_store=cast(FeedbackStore, InMemoryFeedbackStore()),
    )

    await runner.start()
    await asyncio.wait_for(runner.wait_until_idle(), timeout=5.0)
    await asyncio.wait_for(runner.stop(), timeout=5.0)

    assert len(runner.state.orders) == 1
    assert runner.state.orders[0].raw_status == "insufficient_liquidity"
    assert runner.state.fills == []


@pytest.mark.asyncio
async def test_paper_runner_fills_against_signal_orderbook_depth(
    tmp_path: Path,
) -> None:
    runner = Runner(
        config=_settings(RunMode.PAPER),
        sensors=[
            OneShotSensor(
                _signal(
                    market_id="paper-with-depth",
                    orderbook={
                        "bids": [{"price": 0.39, "size": 250.0}],
                        "asks": [{"price": 0.41, "size": 250.0}],
                    },
                    external_signal={"metaculus_prob": 0.9, "resolved_outcome": 1.0},
                )
            )
        ],
        eval_store=cast(EvalStore, InMemoryEvalStore()),
        feedback_store=cast(FeedbackStore, InMemoryFeedbackStore()),
    )

    await runner.start()
    await asyncio.wait_for(runner.wait_until_idle(), timeout=5.0)
    await asyncio.wait_for(runner.stop(), timeout=5.0)

    assert len(runner.state.orders) == 1
    assert runner.state.orders[0].raw_status == "matched"
    assert runner.state.orders[0].fill_price == pytest.approx(0.41)
    assert len(runner.state.fills) == 1
