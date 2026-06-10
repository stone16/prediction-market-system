from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import asyncpg
import pytest

import pms.runner as runner_module
from pms.config import PMSSettings, RiskSettings
from pms.core.enums import MarketStatus, RunMode
from pms.core.models import MarketSignal, Portfolio, TradeDecision
from pms.factors.service import FactorService
from pms.runner import (
    WORKER_ACTUATOR,
    WORKER_CONTROLLER_DISPATCHER,
    WORKER_DECISION_EXPIRY,
    WORKER_FACTOR_SERVICE,
    WORKER_RUNTIME_HEARTBEAT,
    Runner,
)
from pms.sensor.stream import SignalSubscription


FIXTURE_PATH = Path("tests/fixtures/polymarket_7day_synthetic.jsonl")


class IdleSensor:
    def __aiter__(self) -> AsyncIterator[MarketSignal]:
        return self._iterate()

    async def _iterate(self) -> AsyncIterator[MarketSignal]:
        while True:
            await asyncio.sleep(60.0)
            yield _signal()


class NullController:
    async def decide(
        self,
        signal: MarketSignal,
        portfolio: Portfolio | None = None,
    ) -> TradeDecision | None:
        del signal, portfolio
        return None


def _settings() -> PMSSettings:
    return PMSSettings(
        mode=RunMode.PAPER,
        auto_migrate_default_v2=False,
        risk=RiskSettings(
            max_position_per_market=1000.0,
            max_total_exposure=10_000.0,
        ),
    )


def _runner() -> Runner:
    return Runner(
        config=_settings(),
        historical_data_path=FIXTURE_PATH,
        sensors=[IdleSensor()],
        controller=cast(Any, NullController()),
    )


def _signal(*, market_id: str = "market-supervised") -> MarketSignal:
    return MarketSignal(
        market_id=market_id,
        token_id=f"{market_id}-yes",
        venue="polymarket",
        title=f"Will {market_id} settle YES?",
        yes_price=0.41,
        volume_24h=1500.0,
        resolves_at=datetime(2026, 6, 30, tzinfo=UTC),
        orderbook={
            "bids": [{"price": 0.40, "size": 100.0}],
            "asks": [{"price": 0.41, "size": 100.0}],
        },
        external_signal={},
        fetched_at=datetime(2026, 6, 10, 10, 0, tzinfo=UTC),
        market_status=MarketStatus.OPEN.value,
    )


async def _wait_until(
    predicate: Callable[[], bool],
    *,
    timeout_s: float = 2.0,
) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_s
    while not predicate():
        if loop.time() >= deadline:
            msg = "timed out waiting for runner worker state"
            raise AssertionError(msg)
        await asyncio.sleep(0.005)


@pytest.mark.asyncio
async def test_start_registers_all_workers_with_supervisor() -> None:
    """Every runner-owned worker is supervised and every watch-only task
    (sensor consume, controller pipelines) is observed, so a dead worker
    can never again be invisible to monitoring."""
    runner = _runner()
    try:
        await runner.start()
        snapshot = runner.worker_health_snapshot()
        for name in (
            WORKER_CONTROLLER_DISPATCHER,
            WORKER_ACTUATOR,
            WORKER_FACTOR_SERVICE,
            WORKER_DECISION_EXPIRY,
            WORKER_RUNTIME_HEARTBEAT,
        ):
            assert snapshot[name].state == "running"
        assert any(name.startswith("sensor:") for name in snapshot)
        assert snapshot["controller-pipeline:default"].state == "running"
    finally:
        await runner.stop()

    snapshot = runner.worker_health_snapshot()
    assert all(
        health.state in {"stopped", "cancelled"}
        for health in snapshot.values()
    )


@pytest.mark.asyncio
async def test_dispatcher_transient_restart_preserves_pipelines(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A listed transient that escapes the dispatcher loop restarts the
    dispatcher under supervision; the wrapper task stays pending, so
    pipeline loops never observe dispatcher_done and keep running. Signal
    routing must resume after the restart."""
    monkeypatch.setattr(
        runner_module,
        "WORKER_RESTART_BACKOFF_INITIAL_S",
        0.01,
    )
    runner = _runner()

    routed: list[MarketSignal] = []
    failures = 0

    def flaky_remember(signal: MarketSignal) -> None:
        nonlocal failures
        if failures == 0:
            failures += 1
            msg = "transient os error"
            raise OSError(msg)
        routed.append(signal)

    monkeypatch.setattr(
        runner,
        "_remember_signal_for_decision_evidence",
        flaky_remember,
    )

    try:
        await runner.start()
        dispatcher_wrapper = runner.controller_task
        assert dispatcher_wrapper is not None
        pipeline_tasks = runner.controller_pipeline_tasks
        assert len(pipeline_tasks) == 1

        await runner.sensor_stream.queue.put(_signal())
        await _wait_until(
            lambda: runner.worker_health_snapshot()[
                WORKER_CONTROLLER_DISPATCHER
            ].restarts
            == 1
        )
        await _wait_until(
            lambda: runner.worker_health_snapshot()[
                WORKER_CONTROLLER_DISPATCHER
            ].state
            == "running"
        )

        assert runner.controller_task is dispatcher_wrapper
        assert not dispatcher_wrapper.done()
        assert runner.controller_pipeline_tasks == pipeline_tasks
        assert all(not task.done() for task in pipeline_tasks)

        await runner.sensor_stream.queue.put(_signal())
        await _wait_until(lambda: len(routed) >= 1)
    finally:
        await runner.stop()


@pytest.mark.asyncio
async def test_dispatcher_unlisted_failure_is_loud_and_cascade_preserved(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An unlisted exception keeps today's cascade semantics (wrapper task
    completes; pipelines and actuator wind down) but the failure is now
    recorded as `failed` instead of looking like a clean shutdown."""
    runner = _runner()

    def broken_remember(signal: MarketSignal) -> None:
        del signal
        msg = "unexpected bug"
        raise RuntimeError(msg)

    monkeypatch.setattr(
        runner,
        "_remember_signal_for_decision_evidence",
        broken_remember,
    )

    try:
        await runner.start()
        dispatcher_wrapper = runner.controller_task
        assert dispatcher_wrapper is not None
        pipeline_tasks = runner.controller_pipeline_tasks

        await runner.sensor_stream.queue.put(_signal())
        await _wait_until(lambda: dispatcher_wrapper.done())
        assert isinstance(dispatcher_wrapper.exception(), RuntimeError)

        snapshot = runner.worker_health_snapshot()
        assert snapshot[WORKER_CONTROLLER_DISPATCHER].state == "failed"
        assert (
            snapshot[WORKER_CONTROLLER_DISPATCHER].last_error_class
            == "RuntimeError"
        )

        # Cascade preserved: pipelines treat dispatcher-done as shutdown.
        await _wait_until(
            lambda: all(task.done() for task in pipeline_tasks)
        )
        actuator_wrapper = runner.actuator_task
        assert actuator_wrapper is not None
        await _wait_until(lambda: actuator_wrapper.done())
        assert runner.worker_health_snapshot()[WORKER_ACTUATOR].state == "stopped"
    finally:
        await runner.stop()


@pytest.mark.asyncio
async def test_factor_service_restart_rebuilds_instance_and_subscription(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """FactorService instances are single-use (_stream_exhausted): a
    supervised restart must build a fresh instance with a fresh
    sensor_stream.subscribe(), and close the dead subscription."""
    monkeypatch.setattr(
        runner_module,
        "WORKER_RESTART_BACKOFF_INITIAL_S",
        0.01,
    )
    runner = _runner()

    seen: list[FactorService] = []

    async def flaky_run(self: FactorService) -> None:
        seen.append(self)
        if len(seen) == 1:
            msg = "transient pg error"
            raise asyncpg.PostgresError(msg)
        await asyncio.Event().wait()

    monkeypatch.setattr(FactorService, "run", flaky_run)

    try:
        await runner.start()
        await _wait_until(lambda: len(seen) >= 2)

        assert seen[0] is not seen[1]
        first_stream = seen[0].signal_stream
        second_stream = seen[1].signal_stream
        assert first_stream is not second_stream
        assert isinstance(first_stream, SignalSubscription)
        assert first_stream._close_requested  # noqa: SLF001
        assert isinstance(second_stream, SignalSubscription)
        assert not second_stream._close_requested  # noqa: SLF001
        assert runner._factor_service is seen[1]  # noqa: SLF001

        health = runner.worker_health_snapshot()[WORKER_FACTOR_SERVICE]
        assert health.state == "running"
        assert health.restarts == 1
        assert health.last_error_class == "PostgresError"
    finally:
        await runner.stop()


@pytest.mark.asyncio
async def test_stop_during_dispatcher_backoff_blocks_respawn(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Runner.stop() must block respawn before teardown: a worker sitting
    in restart backoff exits without being restarted."""
    monkeypatch.setattr(
        runner_module,
        "WORKER_RESTART_BACKOFF_INITIAL_S",
        30.0,
    )
    runner = _runner()

    def always_transient(signal: MarketSignal) -> None:
        del signal
        msg = "transient os error"
        raise OSError(msg)

    monkeypatch.setattr(
        runner,
        "_remember_signal_for_decision_evidence",
        always_transient,
    )

    entries = 0
    original_loop = runner._controller_loop  # noqa: SLF001

    async def counting_loop() -> None:
        nonlocal entries
        entries += 1
        await original_loop()

    monkeypatch.setattr(runner, "_controller_loop", counting_loop)

    await runner.start()
    await runner.sensor_stream.queue.put(_signal())
    await _wait_until(
        lambda: runner.worker_health_snapshot()[
            WORKER_CONTROLLER_DISPATCHER
        ].state
        == "restarting"
    )

    await asyncio.wait_for(runner.stop(), timeout=2.0)

    assert entries == 1
    snapshot = runner.worker_health_snapshot()
    assert snapshot[WORKER_CONTROLLER_DISPATCHER].state in {
        "stopped",
        "cancelled",
    }
    assert runner.tasks == ()


@pytest.mark.asyncio
async def test_runner_restart_resets_supervision_state() -> None:
    runner = _runner()
    await runner.start()
    await runner.stop()

    await runner.start()
    try:
        snapshot = runner.worker_health_snapshot()
        assert snapshot[WORKER_CONTROLLER_DISPATCHER].state == "running"
        assert snapshot[WORKER_ACTUATOR].state == "running"
        assert snapshot[WORKER_CONTROLLER_DISPATCHER].restarts == 0
    finally:
        await runner.stop()
