from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

from pms.config import DatabaseSettings, PMSSettings, RiskSettings
from pms.core.enums import RunMode
from pms.core.models import MarketSignal, Opportunity, Portfolio, TradeDecision
from pms.market_selection.merge import StrategyMarketSet
from pms.runner import ControllerReleaseCancelPoint, Runner
from pms.strategies.projections import (
    ActiveStrategy,
    EvalSpec,
    ForecasterSpec,
    MarketSelectionSpec,
    RiskParams,
    StrategyConfig,
)


FIXTURE_PATH = Path("tests/fixtures/polymarket_7day_synthetic.jsonl")


@dataclass
class FakePool:
    close_calls: int = 0

    async def close(self) -> None:
        self.close_calls += 1


@dataclass
class IdleDiscoverySensor:
    on_poll_complete: Callable[[], Awaitable[None]] | None = None

    def __aiter__(self) -> AsyncIterator[MarketSignal]:
        return self._iterate()

    async def _iterate(self) -> AsyncIterator[MarketSignal]:
        while True:
            await asyncio.sleep(60.0)
            yield _signal("idle-token")


@dataclass
class RecordingMarketDataSensor:
    asset_ids: list[str] = field(default_factory=list)
    updates: list[list[str]] = field(default_factory=list)

    def __aiter__(self) -> AsyncIterator[MarketSignal]:
        return self._iterate()

    async def _iterate(self) -> AsyncIterator[MarketSignal]:
        while True:
            await asyncio.sleep(60.0)
            yield _signal("market-data-idle")

    async def update_subscription(self, asset_ids: list[str]) -> None:
        self.asset_ids = list(asset_ids)
        self.updates.append(list(asset_ids))


@dataclass
class MutableSelector:
    selections: list[StrategyMarketSet]

    async def select(self) -> Any:
        asset_ids = sorted(
            {
                asset_id
                for selection in self.selections
                for asset_id in selection.asset_ids
            }
        )
        return SimpleNamespace(asset_ids=asset_ids)

    async def select_per_strategy(self) -> list[StrategyMarketSet]:
        return list(self.selections)


@dataclass
class MutableRegistry:
    active_strategies: list[ActiveStrategy]
    callbacks: list[Callable[[], Awaitable[None]]] = field(default_factory=list)

    def register_change_callback(
        self,
        callback: Callable[[], Awaitable[None]],
    ) -> None:
        if callback not in self.callbacks:
            self.callbacks.append(callback)

    def unregister_change_callback(
        self,
        callback: Callable[[], Awaitable[None]],
    ) -> None:
        if callback in self.callbacks:
            self.callbacks.remove(callback)

    async def list_active_strategies(self) -> list[ActiveStrategy]:
        return list(self.active_strategies)

    async def fire_change(self) -> None:
        for callback in tuple(self.callbacks):
            await callback()


@dataclass
class BlockingOpportunityController:
    strategy_id: str
    gate: asyncio.Event = field(default_factory=asyncio.Event)
    started: asyncio.Event = field(default_factory=asyncio.Event)
    completed: asyncio.Event = field(default_factory=asyncio.Event)
    cancelled: asyncio.Event = field(default_factory=asyncio.Event)
    calls: int = 0

    async def on_signal(
        self,
        signal: MarketSignal,
        portfolio: Portfolio | None = None,
    ) -> tuple[Opportunity, TradeDecision] | None:
        del portfolio
        self.calls += 1
        self.started.set()
        try:
            await self.gate.wait()
        except asyncio.CancelledError:
            self.cancelled.set()
            raise
        self.completed.set()
        return _opportunity_and_decision(self.strategy_id, signal)


@dataclass
class ImmediateOpportunityController:
    strategy_id: str
    calls: int = 0
    completed: asyncio.Event = field(default_factory=asyncio.Event)

    async def on_signal(
        self,
        signal: MarketSignal,
        portfolio: Portfolio | None = None,
    ) -> tuple[Opportunity, TradeDecision] | None:
        del portfolio
        self.calls += 1
        self.completed.set()
        return _opportunity_and_decision(self.strategy_id, signal)


@dataclass
class FailingOpportunityController:
    strategy_id: str
    started: asyncio.Event = field(default_factory=asyncio.Event)

    async def on_signal(
        self,
        signal: MarketSignal,
        portfolio: Portfolio | None = None,
    ) -> tuple[Opportunity, TradeDecision] | None:
        del signal, portfolio
        self.started.set()
        raise RuntimeError("controller boom")


@dataclass
class FakeControllerFactory:
    controllers: dict[str, Any]

    def build_many(
        self,
        strategies: list[ActiveStrategy],
    ) -> dict[str, Any]:
        return {
            strategy.strategy_id: self.controllers[strategy.strategy_id]
            for strategy in strategies
        }

    def build(self, strategy: ActiveStrategy) -> Any:
        return self.controllers[strategy.strategy_id]


def _settings() -> PMSSettings:
    return PMSSettings(
        mode=RunMode.LIVE,
        auto_migrate_default_v2=False,
        database=DatabaseSettings(
            dsn="postgresql://localhost/pms_test_runner_cp08",
            pool_min_size=2,
            pool_max_size=10,
        ),
        risk=RiskSettings(
            max_position_per_market=1000.0,
            max_total_exposure=10_000.0,
        ),
    )


def _active_strategy(strategy_id: str) -> ActiveStrategy:
    return ActiveStrategy(
        strategy_id=strategy_id,
        strategy_version_id=f"{strategy_id}-v1",
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
        eval_spec=EvalSpec(metrics=("brier",)),
        forecaster=ForecasterSpec(forecasters=(("rules", ()),)),
        market_selection=MarketSelectionSpec(
            venue="polymarket",
            resolution_time_max_horizon_days=7,
            volume_min_usdc=100.0,
        ),
    )


def _selection(strategy_id: str, asset_id: str) -> StrategyMarketSet:
    return StrategyMarketSet(
        strategy_id=strategy_id,
        strategy_version_id=f"{strategy_id}-v1",
        asset_ids=frozenset({asset_id}),
    )


def _signal(token_id: str) -> MarketSignal:
    return MarketSignal(
        market_id=f"market-{token_id}",
        token_id=token_id,
        venue="polymarket",
        title="Will CP08 lifecycle checks pass?",
        yes_price=0.42,
        volume_24h=1_000.0,
        resolves_at=datetime(2026, 4, 30, tzinfo=UTC),
        orderbook={"bids": [], "asks": []},
        external_signal={"fair_value": 0.55},
        fetched_at=datetime(2026, 4, 19, tzinfo=UTC),
        market_status="open",
    )


def _opportunity_and_decision(
    strategy_id: str,
    signal: MarketSignal,
) -> tuple[Opportunity, TradeDecision]:
    opportunity = Opportunity(
        opportunity_id=f"opp-{strategy_id}",
        market_id=signal.market_id,
        token_id=signal.token_id or f"{strategy_id}-token",
        side="yes",
        selected_factor_values={"fair_value": 0.55},
        expected_edge=0.13,
        rationale="cp08 test opportunity",
        target_size_usdc=5.0,
        expiry=signal.resolves_at,
        staleness_policy="test",
        strategy_id=strategy_id,
        strategy_version_id=f"{strategy_id}-v1",
        created_at=datetime(2026, 4, 19, tzinfo=UTC),
    )
    decision = TradeDecision(
        decision_id=f"decision-{strategy_id}",
        market_id=signal.market_id,
        token_id=signal.token_id,
        venue=signal.venue,
        side="BUY",
        price=signal.yes_price,
        size=5.0,
        order_type="limit",
        max_slippage_bps=10,
        stop_conditions=[],
        prob_estimate=0.55,
        expected_edge=0.13,
        time_in_force="GTC",
        opportunity_id=opportunity.opportunity_id,
        strategy_id=strategy_id,
        strategy_version_id=f"{strategy_id}-v1",
        model_id="rules",
    )
    return opportunity, decision


@pytest.fixture(autouse=True)
def _stub_factor_runtime(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _noop_ensure_factor_catalog(
        pool: object,
        *,
        factor_ids: object = None,
    ) -> None:
        del pool, factor_ids

    class _NoopFactorService:
        def __init__(self, **kwargs: Any) -> None:
            del kwargs

        async def run(self) -> None:
            return None

    monkeypatch.setattr("pms.runner.ensure_factor_catalog", _noop_ensure_factor_catalog)
    monkeypatch.setattr("pms.runner.FactorService", _NoopFactorService)


def _pipeline_task_count() -> int:
    return sum(
        1
        for task in asyncio.all_tasks()
        if not task.done() and task.get_name().startswith("controller-pipeline:")
    )


async def _wait_for(
    predicate: Callable[[], bool],
    *,
    timeout: float = 2.0,
) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while not predicate():
        if asyncio.get_running_loop().time() >= deadline:
            msg = "timed out waiting for predicate"
            raise AssertionError(msg)
        await asyncio.sleep(0.01)


def _install_cp08_runtime(
    monkeypatch: pytest.MonkeyPatch,
    *,
    registry: MutableRegistry,
    selector: MutableSelector,
    market_data: RecordingMarketDataSensor,
    discovery: IdleDiscoverySensor,
) -> FakePool:
    fake_pool = FakePool()

    async def fake_create_pool(*, dsn: str, min_size: int, max_size: int) -> FakePool:
        del dsn, min_size, max_size
        return fake_pool

    def fake_registry_cls(pool: object) -> MutableRegistry:
        del pool
        return registry

    monkeypatch.setattr("pms.runner.asyncpg.create_pool", fake_create_pool)
    monkeypatch.setattr("pms.runner.PostgresStrategyRegistry", fake_registry_cls)
    monkeypatch.setattr("pms.runner.MarketDiscoverySensor", lambda **kwargs: discovery)
    monkeypatch.setattr("pms.runner.MarketDataSensor", lambda **kwargs: market_data)
    monkeypatch.setattr("pms.runner.MarketSelector", lambda *args, **kwargs: selector)
    return fake_pool


@pytest.mark.asyncio
async def test_runner_unregisters_strategy_mid_dispatch_and_releases_resources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = MutableRegistry(
        active_strategies=[
            _active_strategy("strat-a"),
            _active_strategy("strat-b"),
            _active_strategy("strat-c"),
        ]
    )
    selector = MutableSelector(
        selections=[
            _selection("strat-a", "asset-a"),
            _selection("strat-b", "asset-b"),
            _selection("strat-c", "asset-c"),
        ]
    )
    market_data = RecordingMarketDataSensor()
    discovery = IdleDiscoverySensor()
    beta = BlockingOpportunityController("strat-b")
    controllers = {
        "strat-a": ImmediateOpportunityController("strat-a"),
        "strat-b": beta,
        "strat-c": ImmediateOpportunityController("strat-c"),
    }
    alpha = cast(ImmediateOpportunityController, controllers["strat-a"])
    gamma = cast(ImmediateOpportunityController, controllers["strat-c"])

    _install_cp08_runtime(
        monkeypatch,
        registry=registry,
        selector=selector,
        market_data=market_data,
        discovery=discovery,
    )

    runner = Runner(config=_settings(), historical_data_path=FIXTURE_PATH)
    runner._controller_factory = cast(Any, FakeControllerFactory(controllers))

    await runner.start()
    try:
        await runner._reselect()
        assert market_data.asset_ids == ["asset-a", "asset-b", "asset-c"]
        assert _pipeline_task_count() == 3

        await runner.sensor_stream.queue.put(_signal("asset-b"))
        await asyncio.wait_for(beta.started.wait(), timeout=2.0)

        registry.active_strategies = [
            _active_strategy("strat-a"),
            _active_strategy("strat-c"),
        ]
        selector.selections = [
            _selection("strat-a", "asset-a"),
            _selection("strat-c", "asset-c"),
        ]

        await registry.fire_change()
        await _wait_for(lambda: "strat-b" not in runner._controller_pipeline_tasks)
        await _wait_for(lambda: market_data.asset_ids == ["asset-a", "asset-c"])
        await asyncio.wait_for(beta.cancelled.wait(), timeout=2.0)

        assert _pipeline_task_count() == 2
        assert "strat-b" not in runner._controller_runtimes
        assert "strat-b" not in runner._controller_signal_queues
        assert "strat-b" not in runner._controller_pipeline_tasks
        assert runner.subscription_controller is not None
        assert runner.subscription_controller.current_asset_ids == frozenset(
            {"asset-a", "asset-c"}
        )

        await runner.sensor_stream.queue.put(_signal("asset-a"))
        await runner.sensor_stream.queue.put(_signal("asset-c"))
        await _wait_for(lambda: alpha.calls >= 1)
        await _wait_for(lambda: gamma.calls >= 1)
        assert beta.calls == 1
    finally:
        await runner.stop()


@pytest.mark.asyncio
async def test_controller_pipeline_runtime_error_triggers_cleanup(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    registry = MutableRegistry(active_strategies=[_active_strategy("strat-b")])
    selector = MutableSelector(selections=[_selection("strat-b", "asset-b")])
    market_data = RecordingMarketDataSensor()
    discovery = IdleDiscoverySensor()

    _install_cp08_runtime(
        monkeypatch,
        registry=registry,
        selector=selector,
        market_data=market_data,
        discovery=discovery,
    )

    runner = Runner(config=_settings(), historical_data_path=FIXTURE_PATH)
    runner._controller_factory = cast(
        Any,
        FakeControllerFactory(
        {"strat-b": FailingOpportunityController("strat-b")}
        ),
    )

    await runner.start()
    try:
        await runner._reselect()
        caplog.set_level(logging.WARNING, logger="pms.runner")
        await runner.sensor_stream.queue.put(_signal("asset-b"))
        await _wait_for(lambda: not runner._controller_runtimes)
        await _wait_for(lambda: market_data.asset_ids == [])

        assert "strat-b" not in runner._controller_signal_queues
        assert "strat-b" not in runner._controller_pipeline_tasks
        assert "controller pipeline failed for strat-b: controller boom" in caplog.text
    finally:
        await runner.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "cancel_point",
    [
        "before_first_cleanup_await",
        "between_cleanup_awaits",
        "after_last_cleanup_await",
    ],
)
async def test_release_controller_runtime_cancel_injection_still_cleans_up(
    monkeypatch: pytest.MonkeyPatch,
    cancel_point: ControllerReleaseCancelPoint,
) -> None:
    registry = MutableRegistry(
        active_strategies=[
            _active_strategy("strat-a"),
            _active_strategy("strat-b"),
        ]
    )
    selector = MutableSelector(
        selections=[
            _selection("strat-a", "asset-a"),
            _selection("strat-b", "asset-b"),
        ]
    )
    market_data = RecordingMarketDataSensor()
    discovery = IdleDiscoverySensor()
    beta = BlockingOpportunityController("strat-b")

    _install_cp08_runtime(
        monkeypatch,
        registry=registry,
        selector=selector,
        market_data=market_data,
        discovery=discovery,
    )

    runner = Runner(config=_settings(), historical_data_path=FIXTURE_PATH)
    runner._controller_factory = cast(
        Any,
        FakeControllerFactory(
        {
            "strat-a": ImmediateOpportunityController("strat-a"),
            "strat-b": beta,
        }
        ),
    )

    await runner.start()
    try:
        await runner._reselect()
        assert market_data.asset_ids == ["asset-a", "asset-b"]

        await runner.sensor_stream.queue.put(_signal("asset-b"))
        await asyncio.wait_for(beta.started.wait(), timeout=2.0)

        runner._controller_release_cancel_point = cancel_point
        with pytest.raises(asyncio.CancelledError):
            await runner._release_controller_runtime("strat-b")

        await _wait_for(lambda: market_data.asset_ids == ["asset-a"])
        await asyncio.wait_for(beta.cancelled.wait(), timeout=2.0)

        assert "strat-b" not in runner._controller_runtimes
        assert "strat-b" not in runner._controller_signal_queues
        assert "strat-b" not in runner._controller_pipeline_tasks
        assert runner.subscription_controller is not None
        assert runner.subscription_controller.current_asset_ids == frozenset({"asset-a"})
        assert _pipeline_task_count() == 1
    finally:
        await runner.stop()
