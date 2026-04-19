from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

from pms.config import DatabaseSettings, PMSSettings, RiskSettings
from pms.core.enums import RunMode
from pms.core.models import MarketSignal
from pms.market_selection.merge import StrategyMarketSet
from pms.runner import Runner


FIXTURE_PATH = Path("tests/fixtures/polymarket_7day_synthetic.jsonl")


@dataclass
class FakePool:
    close_calls: int = 0
    closed: bool = False

    async def close(self) -> None:
        self.close_calls += 1
        self.closed = True


class IdleSensor:
    def __aiter__(self) -> AsyncIterator[MarketSignal]:
        return self._iterate()

    async def _iterate(self) -> AsyncIterator[MarketSignal]:
        while True:
            await asyncio.sleep(60.0)
            yield _signal()


@dataclass
class RecordingMarketDataSensor:
    asset_ids: list[str] = field(default_factory=list)
    updates: list[list[str]] = field(default_factory=list)
    update_started: asyncio.Event = field(default_factory=asyncio.Event)
    update_release: asyncio.Event = field(default_factory=asyncio.Event)
    close_calls: int = 0

    def __aiter__(self) -> AsyncIterator[MarketSignal]:
        return self._iterate()

    async def _iterate(self) -> AsyncIterator[MarketSignal]:
        while True:
            await asyncio.sleep(60.0)
            yield _signal()

    async def update_subscription(self, asset_ids: list[str]) -> None:
        self.asset_ids = list(asset_ids)
        self.updates.append(list(asset_ids))
        self.update_started.set()
        await self.update_release.wait()

    async def aclose(self) -> None:
        self.close_calls += 1


@dataclass
class RecordingDiscoverySensor:
    events: list[str]
    on_poll_complete: Callable[[], Any] | None = None
    poll_count: int = 0
    close_calls: int = 0
    poll_started: asyncio.Event = field(default_factory=asyncio.Event)
    poll_released: asyncio.Event = field(default_factory=asyncio.Event)
    poll_complete_called: asyncio.Event = field(default_factory=asyncio.Event)
    first_poll_can_finish: asyncio.Event = field(default_factory=asyncio.Event)

    def __aiter__(self) -> AsyncIterator[MarketSignal]:
        return self._iterate()

    async def _iterate(self) -> AsyncIterator[MarketSignal]:
        self.events.append("discovery-started")
        self.poll_started.set()
        await self.first_poll_can_finish.wait()
        self.poll_count += 1
        self.events.append("first-poll-completed")
        if self.on_poll_complete is not None:
            await self.on_poll_complete()
        self.poll_complete_called.set()
        await self.poll_released.wait()
        while True:
            await asyncio.sleep(60.0)
            yield _signal()

    async def aclose(self) -> None:
        self.close_calls += 1


@dataclass
class RecordingSelector:
    events: list[str]
    returned_asset_ids: list[str]
    calls: int = 0
    call_started: asyncio.Event = field(default_factory=asyncio.Event)
    call_release: asyncio.Event = field(default_factory=asyncio.Event)

    async def select(self) -> Any:
        self.calls += 1
        self.call_started.set()
        self.events.append("first-selection-complete")
        await self.call_release.wait()
        return type(
            "MergeResult",
            (),
            {"asset_ids": tuple(self.returned_asset_ids)},
        )()


@dataclass
class RecordingSubscriptionController:
    sink: RecordingMarketDataSensor
    events: list[str]
    calls: int = 0
    updates: list[list[str]] = field(default_factory=list)
    call_started: asyncio.Event = field(default_factory=asyncio.Event)
    call_release: asyncio.Event = field(default_factory=asyncio.Event)

    async def update(self, asset_ids: list[str]) -> bool:
        self.calls += 1
        self.updates.append(list(asset_ids))
        self.call_started.set()
        self.events.append("data-sensor-subscribed")
        await self.call_release.wait()
        await self.sink.update_subscription(asset_ids)
        return True


def _settings(mode: RunMode) -> PMSSettings:
    return PMSSettings(
        mode=mode,
        auto_migrate_default_v2=False,
        database=DatabaseSettings(
            dsn="postgresql://localhost/pms_test_runner",
            pool_min_size=2,
            pool_max_size=10,
        ),
        risk=RiskSettings(
            max_position_per_market=1000.0,
            max_total_exposure=10_000.0,
        ),
    )


def _runner(mode: RunMode, **kwargs: Any) -> Runner:
    return Runner(
        config=_settings(mode),
        historical_data_path=FIXTURE_PATH,
        **kwargs,
    )


def _signal() -> MarketSignal:
    return MarketSignal(
        market_id="runner-active-perception",
        token_id="yes-token",
        venue="polymarket",
        title="Will active perception tests pass?",
        yes_price=0.42,
        volume_24h=1_000.0,
        resolves_at=datetime(2026, 4, 30, tzinfo=UTC),
        orderbook={"bids": [], "asks": []},
        external_signal={"fair_value": 0.55},
        fetched_at=datetime(2026, 4, 16, tzinfo=UTC),
        market_status="open",
    )


@pytest.fixture(autouse=True)
def _stub_factor_catalog_sync(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _noop_ensure_factor_catalog(pool: object, *, factor_ids: object = None) -> None:
        del pool, factor_ids

    monkeypatch.setattr("pms.runner.ensure_factor_catalog", _noop_ensure_factor_catalog)


@pytest.fixture(autouse=True)
def _stub_factor_service(monkeypatch: pytest.MonkeyPatch) -> None:
    class _NoopFactorService:
        def __init__(self, **kwargs: Any) -> None:
            del kwargs

        async def run(self) -> None:
            return None

    monkeypatch.setattr("pms.runner.FactorService", _NoopFactorService)


def _install_live_doubles(
    monkeypatch: pytest.MonkeyPatch,
    *,
    events: list[str],
    returned_asset_ids: list[str],
) -> tuple[
    FakePool,
    RecordingDiscoverySensor,
    RecordingMarketDataSensor,
    RecordingSelector,
    RecordingSubscriptionController,
]:
    fake_pool = FakePool()
    discovery = RecordingDiscoverySensor(events=events)
    market_data = RecordingMarketDataSensor()
    selector = RecordingSelector(events=events, returned_asset_ids=returned_asset_ids)
    subscription_controller = RecordingSubscriptionController(
        sink=market_data,
        events=events,
    )

    async def fake_create_pool(*, dsn: str, min_size: int, max_size: int) -> FakePool:
        del dsn, min_size, max_size
        events.append("pool-ready")
        return fake_pool

    monkeypatch.setattr("pms.runner.asyncpg.create_pool", fake_create_pool)
    monkeypatch.setattr("pms.runner.MarketDiscoverySensor", lambda **kwargs: discovery)
    monkeypatch.setattr("pms.runner.MarketDataSensor", lambda **kwargs: market_data)
    monkeypatch.setattr("pms.runner.MarketSelector", lambda *args, **kwargs: selector)
    monkeypatch.setattr(
        "pms.runner.SensorSubscriptionController",
        lambda sink: subscription_controller,
    )
    return fake_pool, discovery, market_data, selector, subscription_controller


@pytest.mark.asyncio
async def test_runner_active_perception_boot_order_and_first_poll_subscription_update(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    _, discovery, market_data, selector, subscription_controller = _install_live_doubles(
        monkeypatch,
        events=events,
        returned_asset_ids=["no-10d", "yes-10d"],
    )

    runner = _runner(RunMode.LIVE)
    original_build_sensors = runner._build_sensors

    def wrapped_build_sensors() -> tuple[Any, ...]:
        events.append("sensors-built")
        return original_build_sensors()

    monkeypatch.setattr(runner, "_build_sensors", wrapped_build_sensors)
    start_task = asyncio.create_task(runner.start())
    try:
        await asyncio.wait_for(discovery.poll_started.wait(), timeout=2.0)
        assert events == ["pool-ready", "sensors-built", "discovery-started"]
        assert discovery.on_poll_complete is not None
        assert selector.calls == 0
        assert subscription_controller.calls == 0
        assert market_data.updates == []

        discovery.first_poll_can_finish.set()
        await asyncio.wait_for(selector.call_started.wait(), timeout=2.0)
        assert selector.calls == 1
        assert subscription_controller.calls == 0
        assert market_data.updates == []

        selector.call_release.set()
        await asyncio.wait_for(subscription_controller.call_started.wait(), timeout=2.0)
        assert subscription_controller.calls == 1
        assert market_data.updates == []

        subscription_controller.call_release.set()
        await asyncio.wait_for(market_data.update_started.wait(), timeout=2.0)
        assert market_data.asset_ids == ["no-10d", "yes-10d"]
        market_data.update_release.set()
        await asyncio.wait_for(start_task, timeout=2.0)
    finally:
        if not start_task.done():
            start_task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await start_task

    assert events == [
        "pool-ready",
        "sensors-built",
        "discovery-started",
        "first-poll-completed",
        "first-selection-complete",
        "data-sensor-subscribed",
    ]
    assert market_data.asset_ids == ["no-10d", "yes-10d"]

    discovery.poll_released.set()
    await runner.stop()


@pytest.mark.asyncio
async def test_runner_active_perception_backtest_skips_wiring(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_pool = FakePool()

    async def fake_create_pool(*, dsn: str, min_size: int, max_size: int) -> FakePool:
        del dsn, min_size, max_size
        return fake_pool

    monkeypatch.setattr("pms.runner.asyncpg.create_pool", fake_create_pool)

    runner = _runner(RunMode.BACKTEST, sensors=[IdleSensor()])
    await runner.start()

    assert runner._market_selector is None
    assert runner._subscription_controller is None
    assert runner._reselection_task is None

    await runner.stop()


@pytest.mark.asyncio
async def test_runner_active_perception_stop_cleans_up_references_and_sensors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    _, discovery, market_data, _, _ = _install_live_doubles(
        monkeypatch,
        events=events,
        returned_asset_ids=["no-10d", "yes-10d"],
    )

    runner = _runner(RunMode.LIVE)
    await runner.start()
    await runner.stop()

    assert runner._market_selector is None
    assert runner._subscription_controller is None
    assert runner._reselection_task is None
    assert discovery.close_calls == 1
    assert market_data.close_calls == 1


@pytest.mark.asyncio
async def test_runner_active_perception_reselection_is_serialized(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    _, discovery, market_data, selector, subscription_controller = _install_live_doubles(
        monkeypatch,
        events=events,
        returned_asset_ids=["no-10d", "yes-10d"],
    )

    runner = _runner(RunMode.LIVE)
    await runner.start()

    assert runner._reselection_task is not None

    first = asyncio.create_task(runner._reselect())
    second = asyncio.create_task(runner._reselect())
    await asyncio.wait_for(selector.call_started.wait(), timeout=2.0)
    assert selector.calls == 1
    assert subscription_controller.calls == 0

    selector.call_release.set()
    await asyncio.wait_for(subscription_controller.call_started.wait(), timeout=2.0)
    assert subscription_controller.calls == 1

    subscription_controller.call_release.set()
    market_data.update_release.set()
    await asyncio.wait_for(first, timeout=2.0)
    await asyncio.wait_for(second, timeout=2.0)

    assert selector.calls == 2
    assert subscription_controller.calls == 2

    discovery.poll_released.set()
    await runner.stop()


@pytest.mark.asyncio
async def test_refresh_subscription_updates_wait_for_reselection_lock() -> None:
    events: list[list[str]] = []

    @dataclass
    class SerializingSubscriptionController:
        active_calls: int = 0
        max_active_calls: int = 0
        first_call_started: asyncio.Event = field(default_factory=asyncio.Event)
        release_first_call: asyncio.Event = field(default_factory=asyncio.Event)

        async def update(self, asset_ids: list[str]) -> bool:
            self.active_calls += 1
            self.max_active_calls = max(self.max_active_calls, self.active_calls)
            events.append(list(asset_ids))
            if not self.first_call_started.is_set():
                self.first_call_started.set()
                await self.release_first_call.wait()
            self.active_calls -= 1
            return True

    @dataclass
    class StaticSelector:
        asset_ids: list[str]

        async def select(self) -> Any:
            return type("MergeResult", (), {"asset_ids": tuple(self.asset_ids)})()

    runner = _runner(RunMode.LIVE)
    subscription_controller = SerializingSubscriptionController()
    runner._market_selector = cast(Any, StaticSelector(["strategy-token"]))  # noqa: SLF001
    runner._subscription_controller = cast(Any, subscription_controller)  # noqa: SLF001
    runner._controller_runtimes = {  # noqa: SLF001
        "alpha": cast(Any, SimpleNamespace(asset_ids=frozenset({"refresh-token"})))
    }

    reselect_task = asyncio.create_task(runner._reselect())  # noqa: SLF001
    await asyncio.wait_for(subscription_controller.first_call_started.wait(), timeout=2.0)

    refresh_task = asyncio.create_task(runner._refresh_subscription_assets_locked())  # noqa: SLF001
    await asyncio.sleep(0)
    assert subscription_controller.max_active_calls == 1
    assert events == [["strategy-token"]]

    subscription_controller.release_first_call.set()
    await asyncio.wait_for(reselect_task, timeout=2.0)
    await asyncio.wait_for(refresh_task, timeout=2.0)

    assert subscription_controller.max_active_calls == 1
    assert events == [["strategy-token"], ["refresh-token"]]


@pytest.mark.asyncio
async def test_event_triggered_reselection_failure_does_not_kill_loop(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    events: list[str] = []
    _, discovery, _, _, _ = _install_live_doubles(
        monkeypatch,
        events=events,
        returned_asset_ids=["no-10d", "yes-10d"],
    )

    runner = _runner(RunMode.LIVE)
    await runner.start()
    try:
        reselection_task = runner._reselection_task
        assert reselection_task is not None
        assert not reselection_task.done()

        raising_selector_called = asyncio.Event()

        class RaisingSelector:
            calls = 0

            async def select(self) -> Any:
                RaisingSelector.calls += 1
                raising_selector_called.set()
                raise RuntimeError("transient postgres error")

            async def select_per_strategy(self) -> list[StrategyMarketSet]:
                raise RuntimeError("transient postgres error")

        runner._market_selector = RaisingSelector()

        caplog.set_level(logging.WARNING, logger="pms.runner")
        await runner._request_reselection()
        await asyncio.wait_for(raising_selector_called.wait(), timeout=2.0)
        for _ in range(10):
            await asyncio.sleep(0)

        assert not reselection_task.done(), (
            "reselection task died when event-triggered _reselect raised; "
            f"task state: done={reselection_task.done()}"
        )
        assert RaisingSelector.calls == 1
        assert "periodic reselection failed" in caplog.text
    finally:
        discovery.poll_released.set()
        await runner.stop()
