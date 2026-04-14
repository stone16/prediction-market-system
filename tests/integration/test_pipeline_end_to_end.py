from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from pathlib import Path

import pytest

from pms.config import PMSSettings, RiskSettings
from pms.core.enums import MarketStatus, RunMode
from pms.core.models import MarketSignal
from pms.runner import Runner
from pms.storage.eval_store import EvalStore
from pms.storage.feedback_store import FeedbackStore


FIXTURE_PATH = Path("tests/fixtures/polymarket_7day_synthetic.jsonl")


class HoldingSensor:
    def __aiter__(self) -> AsyncIterator[MarketSignal]:
        return self._iterate()

    async def _iterate(self) -> AsyncIterator[MarketSignal]:
        while True:
            await asyncio.sleep(60.0)
            yield _signal()


def _settings(mode: RunMode) -> PMSSettings:
    return PMSSettings(
        mode=mode,
        risk=RiskSettings(
            max_position_per_market=1000.0,
            max_total_exposure=10_000.0,
        ),
    )


def _signal() -> MarketSignal:
    return MarketSignal(
        market_id="runner-hold",
        token_id="yes-token",
        venue="polymarket",
        title="Will the runner keep tasks alive?",
        yes_price=0.4,
        volume_24h=1000.0,
        resolves_at=datetime(2026, 4, 20, tzinfo=UTC),
        orderbook={
            "bids": [{"price": 0.39, "size": 100.0}],
            "asks": [{"price": 0.41, "size": 100.0}],
        },
        external_signal={"metaculus_prob": 0.7, "resolved_outcome": 1.0},
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
        eval_store=EvalStore(path=tmp_path / "eval_records.jsonl"),
        feedback_store=FeedbackStore(path=tmp_path / "feedback.jsonl"),
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
        eval_store=EvalStore(path=tmp_path / "eval_records.jsonl"),
        feedback_store=FeedbackStore(path=tmp_path / "feedback.jsonl"),
    )

    await runner.start()
    await asyncio.wait_for(runner.wait_until_idle(), timeout=5.0)
    await asyncio.wait_for(runner.stop(), timeout=5.0)

    records = runner.eval_store.all()

    assert len(runner.state.signals) == 100
    assert len(runner.state.decisions) >= 10
    assert len(records) >= 5
    assert all(0.0 <= record.brier_score <= 1.0 for record in records)


@pytest.mark.integration
@pytest.mark.skipif(
    os.environ.get("PMS_RUN_INTEGRATION") != "1",
    reason="set PMS_RUN_INTEGRATION=1 to run live Polymarket integration tests",
)
@pytest.mark.asyncio
async def test_paper_mode_live_polymarket_produces_market_signal(
    tmp_path: Path,
) -> None:
    runner = Runner(
        config=_settings(RunMode.PAPER),
        eval_store=EvalStore(path=tmp_path / "eval_records.jsonl"),
        feedback_store=FeedbackStore(path=tmp_path / "feedback.jsonl"),
    )

    await runner.start()
    try:
        await asyncio.wait_for(runner.wait_for_signals(1), timeout=60.0)
    finally:
        await asyncio.wait_for(runner.stop(), timeout=5.0)

    assert len(runner.state.signals) >= 1
