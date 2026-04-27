from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

from pms.actuator.executor import ActuatorExecutor
from pms.actuator.adapters.polymarket import (
    FileFirstLiveOrderGate,
    PolymarketActuator,
    PolymarketSDKClient,
)
from pms.config import (
    ControllerSettings,
    DatabaseSettings,
    PMSSettings,
    PolymarketSettings,
    RiskSettings,
)
from pms.core.enums import RunMode
from pms.core.models import (
    MarketSignal,
    OrderState,
    Portfolio,
    ReconciliationReport,
    VenueAccountSnapshot,
    VenueCredentials,
)
from pms.market_selection.merge import StrategyMarketSet
from pms.runner import Runner
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
    closed: bool = False

    async def close(self) -> None:
        self.closed = True


class MatchingVenueReconciler:
    async def snapshot(self, credentials: VenueCredentials) -> VenueAccountSnapshot:
        del credentials
        return VenueAccountSnapshot(balances={"USDC": 10_000.0}, open_orders=(), positions=())

    async def compare(
        self,
        db_portfolio: Portfolio,
        venue_snapshot: VenueAccountSnapshot,
    ) -> ReconciliationReport:
        del db_portfolio, venue_snapshot
        return ReconciliationReport(ok=True, mismatches=())


@pytest.fixture(autouse=True)
def _stub_live_venue_reconciler(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "pms.runner.PolymarketVenueAccountReconciler",
        MatchingVenueReconciler,
    )


class IdleDiscoverySensor:
    on_poll_complete: Any = None

    def __aiter__(self) -> AsyncIterator[MarketSignal]:
        return self._iterate()

    async def _iterate(self) -> AsyncIterator[MarketSignal]:
        while True:
            await asyncio.sleep(60.0)
            yield _signal()


class IdleMarketDataSensor:
    def __init__(self) -> None:
        self.updates: list[list[str]] = []

    def __aiter__(self) -> AsyncIterator[MarketSignal]:
        return self._iterate()

    async def _iterate(self) -> AsyncIterator[MarketSignal]:
        while True:
            await asyncio.sleep(60.0)
            yield _signal()

    async def update_subscription(self, asset_ids: list[str]) -> None:
        self.updates.append(list(asset_ids))


class StaticSelector:
    async def select(self) -> Any:
        return SimpleNamespace(asset_ids=["shared-token"])

    async def select_per_strategy(self) -> list[StrategyMarketSet]:
        return [
            StrategyMarketSet(
                strategy_id="alpha",
                strategy_version_id="alpha-v1",
                asset_ids=frozenset({"shared-token"}),
            ),
            StrategyMarketSet(
                strategy_id="beta",
                strategy_version_id="beta-v1",
                asset_ids=frozenset({"shared-token"}),
            ),
        ]


class FakeSubscriptionController:
    def __init__(self, sink: IdleMarketDataSensor) -> None:
        self.sink = sink

    async def update(self, asset_ids: list[str]) -> bool:
        await self.sink.update_subscription(asset_ids)
        return True


class FakeRegistry:
    def __init__(self, pool: FakePool) -> None:
        del pool

    def register_change_callback(self, callback: Any) -> None:
        del callback

    def unregister_change_callback(self, callback: Any) -> None:
        del callback

    async def list_active_strategies(self) -> list[ActiveStrategy]:
        return [
            _active_strategy(
                strategy_id="alpha",
                strategy_version_id="alpha-v1",
                forecaster_names=("rules", "stats"),
            ),
            _active_strategy(
                strategy_id="beta",
                strategy_version_id="beta-v1",
                forecaster_names=("rules", "stats", "llm"),
            ),
        ]


def _settings() -> PMSSettings:
    return PMSSettings(
        mode=RunMode.LIVE,
        live_trading_enabled=True,
        auto_migrate_default_v2=False,
        database=DatabaseSettings(
            dsn="postgresql://localhost/pms_test_runner_cp01",
            pool_min_size=2,
            pool_max_size=10,
        ),
        risk=RiskSettings(
            max_position_per_market=1000.0,
            max_total_exposure=10_000.0,
        ),
        controller=ControllerSettings(time_in_force="IOC"),
        polymarket=_live_polymarket_settings(),
    )


def test_runner_builds_live_polymarket_adapter_with_sdk_client_and_file_gate(
    tmp_path: Path,
) -> None:
    settings = PMSSettings(
        mode=RunMode.LIVE,
        live_trading_enabled=True,
        auto_migrate_default_v2=False,
        polymarket=PolymarketSettings(
            private_key="private-key",
            api_key="api-key",
            api_secret="api-secret",
            api_passphrase="passphrase",
            signature_type=1,
            funder_address="0xabc",
            first_live_order_approval_path=str(tmp_path / "approval.json"),
        ),
        controller=ControllerSettings(time_in_force="IOC"),
    )
    runner = Runner(config=settings, historical_data_path=FIXTURE_PATH)

    adapter = runner._build_adapter(RunMode.LIVE)  # noqa: SLF001

    assert isinstance(adapter, PolymarketActuator)
    assert isinstance(adapter.client, PolymarketSDKClient)
    assert isinstance(adapter.operator_gate, FileFirstLiveOrderGate)


def _live_polymarket_settings() -> PolymarketSettings:
    return PolymarketSettings(
        private_key="private-key",
        api_key="api-key",
        api_secret="api-secret",
        api_passphrase="passphrase",
        signature_type=1,
        funder_address="0xabc",
    )


def _signal() -> MarketSignal:
    return MarketSignal(
        market_id="runner-cp01",
        token_id="shared-token",
        venue="polymarket",
        title="Will runner fan out controller signals per strategy?",
        yes_price=0.42,
        volume_24h=1_000.0,
        resolves_at=datetime(2026, 4, 30, tzinfo=UTC),
        orderbook={"bids": [], "asks": []},
        external_signal={"fair_value": 0.55},
        fetched_at=datetime(2026, 4, 19, tzinfo=UTC),
        market_status="open",
    )


def _active_strategy(
    *,
    strategy_id: str,
    strategy_version_id: str,
    forecaster_names: tuple[str, ...],
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
        forecaster=ForecasterSpec(
            forecasters=tuple((name, ()) for name in forecaster_names)
        ),
        market_selection=MarketSelectionSpec(
            venue="polymarket",
            resolution_time_max_horizon_days=7,
            volume_min_usdc=500.0,
        ),
    )


async def _wait_for_decisions(runner: Runner, count: int) -> None:
    deadline = asyncio.get_running_loop().time() + 2.0
    while len(runner.state.decisions) < count:
        if asyncio.get_running_loop().time() >= deadline:
            msg = f"timed out waiting for {count} decisions"
            raise AssertionError(msg)
        await asyncio.sleep(0.01)


@pytest.mark.asyncio
async def test_runner_creates_one_controller_task_per_active_strategy_and_fans_out_shared_signal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    discovery = IdleDiscoverySensor()
    market_data = IdleMarketDataSensor()
    fake_pool = FakePool()

    async def fake_create_pool(*, dsn: str, min_size: int, max_size: int) -> FakePool:
        del dsn, min_size, max_size
        return fake_pool

    async def fake_ensure_factor_catalog(pool: object, *, factor_ids: object = None) -> None:
        del pool, factor_ids

    class _NoopFactorService:
        def __init__(self, **kwargs: Any) -> None:
            del kwargs

        async def run(self) -> None:
            return None

    async def fake_execute(
        decision: Any,
        portfolio: Portfolio | None = None,
        *,
        dedup_acquired: bool = False,
    ) -> OrderState:
        del portfolio, dedup_acquired
        return OrderState(
            order_id=f"order-{decision.decision_id}",
            decision_id=decision.decision_id,
            status="rejected",
            market_id=decision.market_id,
            token_id=decision.token_id,
            venue=decision.venue,
            requested_notional_usdc=decision.notional_usdc,
            filled_notional_usdc=0.0,
            remaining_notional_usdc=decision.notional_usdc,
            fill_price=None,
            submitted_at=datetime(2026, 4, 19, tzinfo=UTC),
            last_updated_at=datetime(2026, 4, 19, tzinfo=UTC),
            raw_status="rejected",
            strategy_id=decision.strategy_id,
            strategy_version_id=decision.strategy_version_id,
            filled_quantity=0.0,
        )

    monkeypatch.setattr("pms.runner.asyncpg.create_pool", fake_create_pool)
    monkeypatch.setattr("pms.runner.ensure_factor_catalog", fake_ensure_factor_catalog)
    monkeypatch.setattr("pms.runner.FactorService", _NoopFactorService)
    monkeypatch.setattr("pms.runner.PostgresStrategyRegistry", FakeRegistry)
    monkeypatch.setattr("pms.runner.MarketSelector", lambda *args, **kwargs: StaticSelector())
    monkeypatch.setattr(
        "pms.controller.forecasters.rules.RulesForecaster.predict",
        lambda self, signal: (0.65, 0.9, "test-rules"),
    )
    monkeypatch.setattr(
        "pms.controller.forecasters.statistical.StatisticalForecaster.predict",
        lambda self, signal: (0.65, 0.9, "test-stats"),
    )
    monkeypatch.setattr(
        "pms.controller.forecasters.llm.LLMForecaster.predict",
        lambda self, signal: (0.65, 0.9, "test-llm"),
    )
    monkeypatch.setattr(
        "pms.runner.SensorSubscriptionController",
        lambda sink: FakeSubscriptionController(sink),
    )

    runner = Runner(
        config=_settings(),
        historical_data_path=FIXTURE_PATH,
        sensors=[discovery, market_data],
    )
    runner.actuator_executor = cast(
        ActuatorExecutor,
        SimpleNamespace(execute=fake_execute),
    )

    try:
        await runner.start()

        assert len(runner.controller_pipeline_tasks) == 2
        active_names = {task.get_name() for task in runner.controller_pipeline_tasks}
        assert active_names == {
            "controller-pipeline:alpha",
            "controller-pipeline:beta",
        }

        await runner.sensor_stream.queue.put(_signal())
        await _wait_for_decisions(runner, 2)

        assert {
            (decision.strategy_id, decision.strategy_version_id)
            for decision in runner.state.decisions
        } == {
            ("alpha", "alpha-v1"),
            ("beta", "beta-v1"),
        }

        running_tasks = asyncio.all_tasks()
        assert all(task in running_tasks for task in runner.controller_pipeline_tasks)
    finally:
        await runner.stop()
