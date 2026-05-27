from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, cast

import pytest
from pydantic import SecretStr

from pms.config import (
    ControllerSettings,
    DatabaseSettings,
    DiscordSettings,
    PMSSettings,
    PolymarketSettings,
    RiskSettings,
)
from pms.core.enums import RunMode
from pms.core.models import (
    MarketSignal,
    Portfolio,
    ReconciliationReport,
    VenueAccountSnapshot,
    VenueCredentials,
)
from pms.market_selection.merge import StrategyMarketSet
from pms.runner import Runner
from pms.storage.strategy_registry import PostgresStrategyRegistry
from pms.strategies.aggregate import Strategy
from pms.strategies.projections import (
    EvalSpec,
    FactorCompositionStep,
    ForecasterSpec,
    MarketSelectionSpec,
    RiskParams,
    StrategyConfig,
)
from pms.strategies.versioning import (
    compute_strategy_version_id,
    serialize_strategy_config_json,
)
from tests.support.live_paths import (
    make_live_preflight_artifact_path,
    make_live_report_paths,
    make_private_live_paths,
)


FIXTURE_PATH = Path("tests/fixtures/polymarket_7day_synthetic.jsonl")


@dataclass
class FakeTransaction:
    entered: int = 0
    exited: int = 0

    async def __aenter__(self) -> None:
        self.entered += 1
        return None

    async def __aexit__(self, *_: object) -> None:
        self.exited += 1
        return None


@dataclass
class FakeConnection:
    fetchval_results: list[object] = field(default_factory=list)
    fetchrow_results: list[dict[str, object] | None] = field(default_factory=list)
    latest_book_snapshot_age_s: float | None = 30.0
    latest_usable_book_snapshot_age_s: float | None = 30.0
    missing_market_risk_metadata_count: int = 0
    execute_results: list[str] = field(default_factory=list)
    execute_calls: list[tuple[str, tuple[object, ...]]] = field(default_factory=list)
    fetchval_calls: list[tuple[str, tuple[object, ...]]] = field(default_factory=list)
    fetchrow_calls: list[tuple[str, tuple[object, ...]]] = field(default_factory=list)
    transaction_manager: FakeTransaction = field(default_factory=FakeTransaction)
    acquire_exit_calls: int = 0

    async def execute(self, query: str, *args: object) -> str:
        self.execute_calls.append((query, args))
        if self.execute_results:
            return self.execute_results.pop(0)
        return "UPDATE 1"

    async def fetchval(self, query: str, *args: object) -> object:
        self.fetchval_calls.append((query, args))
        if "missing_market_risk_metadata" in query:
            return self.missing_market_risk_metadata_count
        if "usable_book_snapshots" in query:
            return self.latest_usable_book_snapshot_age_s
        if "book_snapshots" in query:
            return self.latest_book_snapshot_age_s
        if not self.fetchval_results:
            msg = "fetchval called without a configured result"
            raise AssertionError(msg)
        return self.fetchval_results.pop(0)

    async def fetchrow(self, query: str, *args: object) -> dict[str, object] | None:
        self.fetchrow_calls.append((query, args))
        if not self.fetchrow_results:
            return None
        return self.fetchrow_results.pop(0)

    def transaction(self) -> FakeTransaction:
        return self.transaction_manager


class FakeAcquire:
    def __init__(self, connection: FakeConnection) -> None:
        self._connection = connection

    async def __aenter__(self) -> FakeConnection:
        return self._connection

    async def __aexit__(self, *_: object) -> None:
        self._connection.acquire_exit_calls += 1
        return None


class FakePool:
    def __init__(self, connection: FakeConnection) -> None:
        self._connection = connection

    def acquire(self) -> FakeAcquire:
        return FakeAcquire(self._connection)


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


@pytest.fixture(autouse=True)
def _stub_live_active_strategy_preflight(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _accept_active_strategy_preflight(
        settings: PMSSettings,
        registry: object,
    ) -> None:
        del settings, registry

    monkeypatch.setattr(
        "pms.runner.require_live_preflight_active_strategies_artifact",
        _accept_active_strategy_preflight,
    )


@dataclass
class RuntimePool:
    close_calls: int = 0

    async def close(self) -> None:
        self.close_calls += 1


@dataclass
class IdleDiscoverySensor:
    on_poll_complete: Callable[[], Awaitable[None]] | None = None
    close_calls: int = 0

    def __aiter__(self) -> AsyncIterator[MarketSignal]:
        return self._iterate()

    async def _iterate(self) -> AsyncIterator[MarketSignal]:
        while True:
            await asyncio.sleep(60.0)
            yield _signal()

    async def aclose(self) -> None:
        self.close_calls += 1


@dataclass
class RecordingMarketDataSensor:
    asset_ids: list[str] = field(default_factory=list)
    updates: list[list[str]] = field(default_factory=list)
    update_started: asyncio.Event = field(default_factory=asyncio.Event)
    update_release: asyncio.Event = field(default_factory=asyncio.Event)

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


@dataclass
class RecordingSelector:
    returned_asset_ids: list[str]
    calls: int = 0
    call_started: asyncio.Event = field(default_factory=asyncio.Event)
    call_release: asyncio.Event = field(default_factory=asyncio.Event)

    async def select(self) -> Any:
        self.calls += 1
        self.call_started.set()
        await self.call_release.wait()
        return type(
            "MergeResult",
            (),
            {"asset_ids": tuple(self.returned_asset_ids)},
        )()

    async def select_per_strategy(self) -> list[StrategyMarketSet]:
        return [
            StrategyMarketSet(
                strategy_id="default",
                strategy_version_id="default-v1",
                asset_ids=frozenset(self.returned_asset_ids),
            )
        ]


@dataclass
class RecordingSubscriptionController:
    sink: RecordingMarketDataSensor
    calls: int = 0
    call_started: asyncio.Event = field(default_factory=asyncio.Event)
    call_release: asyncio.Event = field(default_factory=asyncio.Event)

    async def update(self, asset_ids: list[str]) -> bool:
        self.calls += 1
        self.call_started.set()
        await self.call_release.wait()
        await self.sink.update_subscription(asset_ids)
        return True


def _strategy(strategy_id: str = "default") -> Strategy:
    return Strategy(
        config=StrategyConfig(
            strategy_id=strategy_id,
            factor_composition=(
                FactorCompositionStep(
                    factor_id="factor-a",
                    role="weighted",
                    param="",
                    weight=1.0,
                    threshold=None,
                ),
            ),
            metadata=(
                ("owner", "system"),
                ("live_allowed", "true"),
                ("alpha_source", "warehouse_flb_decile_model_v1"),
                ("edge_model_source", "paper_soak_net_edge_model_v1"),
                ("calibration_source", "paper_soak_eval_records_v1"),
                ("evidence_source", "paper_soak_go_report_v1"),
            ),
        ),
        risk=RiskParams(
            max_position_notional_usdc=100.0,
            max_daily_drawdown_pct=2.5,
            min_order_size_usdc=1.0,
        ),
        eval_spec=EvalSpec(metrics=("brier",)),
        forecaster=ForecasterSpec(forecasters=(("rules", (("threshold", "0.55"),)),)),
        market_selection=MarketSelectionSpec(
            venue="polymarket",
            resolution_time_max_horizon_days=30,
            volume_min_usdc=0.0,
        ),
    )


def _signal() -> MarketSignal:
    return MarketSignal(
        market_id="strategy-change-market",
        token_id="yes-token",
        venue="polymarket",
        title="Will CP06 pass?",
        yes_price=0.42,
        volume_24h=1_000.0,
        resolves_at=datetime(2026, 4, 30, tzinfo=UTC),
        orderbook={"bids": [], "asks": []},
        external_signal={"fair_value": 0.55},
        fetched_at=datetime(2026, 4, 18, tzinfo=UTC),
        market_status="open",
    )


def _settings(mode: RunMode) -> PMSSettings:
    approval_path: str | None = None
    emergency_audit_path = ".data/live-emergency-audit.jsonl"
    audit_path = ".data/first-order-audit.jsonl"
    if mode == RunMode.LIVE:
        approval_path, audit_path = make_private_live_paths(
            prefix="pms-strategy-reselect-"
        )
        emergency_audit_path = str(
            Path(approval_path).parent / "live-emergency-audit.jsonl"
        )
        paper_report_path, rehearsal_report_path = make_live_report_paths(
            prefix="pms-strategy-reselect-reports-"
        )
    else:
        paper_report_path = None
        rehearsal_report_path = None
    settings = PMSSettings(
        mode=mode,
        secret_source="fly" if mode == RunMode.LIVE else None,
        live_trading_enabled=mode == RunMode.LIVE,
        live_exit_criteria_ratified_by=(
            "test-operator" if mode == RunMode.LIVE else None
        ),
        live_exit_criteria_ratified_at=(
            datetime(2026, 5, 25, tzinfo=UTC) if mode == RunMode.LIVE else None
        ),
        live_compliance_reviewed_by=(
            "test-compliance" if mode == RunMode.LIVE else None
        ),
        live_compliance_reviewed_at=(
            datetime(2026, 5, 25, tzinfo=UTC) if mode == RunMode.LIVE else None
        ),
        live_compliance_jurisdiction="US" if mode == RunMode.LIVE else None,
        live_paper_soak_report_path=paper_report_path,
        live_operator_rehearsal_report_path=rehearsal_report_path,
        live_emergency_audit_path=emergency_audit_path,
        live_first_order_audit_path=audit_path,
        auto_migrate_default_v2=False,
        database=DatabaseSettings(
            dsn="postgresql://localhost/pms_test_runner",
            pool_min_size=2,
            pool_max_size=10,
        ),
        risk=RiskSettings(
            max_position_per_market=1000.0,
            max_total_exposure=10_000.0,
            max_drawdown_pct=20.0,
            max_daily_loss_usdc=20.0,
            max_open_positions=5,
            max_exposure_per_risk_group=5_000.0,
            max_quantity_shares=500.0,
        ),
        controller=ControllerSettings(time_in_force="IOC", quote_source="dual"),
        discord=(
            DiscordSettings(
                webhook_url=SecretStr(
                    "https://discord.example/webhooks/strategy-reselect/unit"
                ),
                alert_dir=str(Path(cast(str, approval_path)).parent / "discord-alerts"),
            )
            if mode == RunMode.LIVE
            else DiscordSettings()
        ),
        polymarket=(
            _live_polymarket_settings(approval_path)
            if mode == RunMode.LIVE
            else PolymarketSettings()
        ),
    )
    if mode == RunMode.LIVE:
        settings.live_preflight_artifact_path = make_live_preflight_artifact_path(
            prefix="pms-strategy-reselect-preflight-",
            settings=settings,
        )
    return settings


def _live_polymarket_settings(approval_path: str | None = None) -> PolymarketSettings:
    if approval_path is None:
        approval_path, _ = make_private_live_paths(
            prefix="pms-strategy-reselect-polymarket-"
        )
    return PolymarketSettings(
        private_key="private-key",
        api_key="api-key",
        api_secret="api-secret",
        api_passphrase="passphrase",
        signature_type=1,
        funder_address="0x1111111111111111111111111111111111111111",
        operator_approval_mode="every_order",
        first_live_order_approval_path=approval_path,
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


@pytest.mark.asyncio
async def test_create_version_fires_callback_after_write_context_exits() -> None:
    strategy = _strategy()
    created_at = datetime(2026, 4, 18, 10, 0, tzinfo=timezone(timedelta(hours=-5)))
    connection = FakeConnection(fetchval_results=[created_at])
    observed: dict[str, int] = {}

    async def on_strategy_change() -> None:
        observed["transaction_exited"] = connection.transaction_manager.exited
        observed["acquire_exited"] = connection.acquire_exit_calls

    registry = PostgresStrategyRegistry(FakePool(connection))
    registry.register_change_callback(on_strategy_change)

    version = await registry.create_version(strategy)

    assert version.strategy_id == strategy.config.strategy_id
    assert version.strategy_version_id == compute_strategy_version_id(*strategy.snapshot())
    assert version.created_at == created_at.astimezone(UTC)
    assert connection.fetchval_calls[0][1] == (
        version.strategy_version_id,
        strategy.config.strategy_id,
        serialize_strategy_config_json(*strategy.snapshot()),
    )
    assert observed == {"transaction_exited": 1, "acquire_exited": 1}


@pytest.mark.asyncio
async def test_set_active_fires_callback_after_acquire_exit() -> None:
    strategy = _strategy()
    strategy_version_id = compute_strategy_version_id(*strategy.snapshot())
    connection = FakeConnection(
        fetchrow_results=[
            {
                "strategy_id": "default",
                "strategy_version_id": strategy_version_id,
                "config_json": serialize_strategy_config_json(*strategy.snapshot()),
            }
        ]
    )
    observed: dict[str, int] = {}

    async def on_strategy_change() -> None:
        observed["acquire_exited"] = connection.acquire_exit_calls

    registry = PostgresStrategyRegistry(FakePool(connection))
    registry.register_change_callback(on_strategy_change)

    await registry.set_active("default", strategy_version_id)

    assert connection.execute_calls == [
        (
            """
        UPDATE strategies
        SET active_version_id = $2
        WHERE strategy_id = $1
          AND EXISTS (
              SELECT 1
              FROM strategy_versions
              WHERE strategy_versions.strategy_id = $1
                AND strategy_versions.strategy_version_id = $2
          )
        """,
            ("default", strategy_version_id),
        )
    ]
    assert observed == {"acquire_exited": 1}


@pytest.mark.asyncio
async def test_strategy_change_callback_error_is_logged_without_breaking_write(
    caplog: pytest.LogCaptureFixture,
) -> None:
    created_at = datetime(2026, 4, 18, 14, 0, tzinfo=UTC)
    connection = FakeConnection(fetchval_results=[created_at])

    async def broken_callback() -> None:
        raise RuntimeError("callback boom")

    registry = PostgresStrategyRegistry(FakePool(connection))
    registry.register_change_callback(broken_callback)

    caplog.set_level(logging.WARNING)
    version = await registry.create_version(_strategy())

    assert version.strategy_id == "default"
    assert "strategy change callback failed: callback boom" in caplog.text
    assert connection.transaction_manager.exited == 1
    assert connection.acquire_exit_calls == 1


@pytest.mark.asyncio
async def test_register_and_unregister_change_callbacks_are_idempotent() -> None:
    created_at = datetime(2026, 4, 18, 14, 30, tzinfo=UTC)
    connection = FakeConnection(fetchval_results=[created_at])
    registry = PostgresStrategyRegistry(FakePool(connection))
    observed: list[str] = []

    async def first_callback() -> None:
        observed.append("first")

    async def second_callback() -> None:
        observed.append("second")

    registry.register_change_callback(first_callback)
    registry.register_change_callback(first_callback)
    registry.register_change_callback(second_callback)

    strategy = _strategy()
    version = await registry.create_version(strategy)

    registry.unregister_change_callback(first_callback)
    registry.unregister_change_callback(first_callback)
    connection.fetchrow_results.append(
        {
            "strategy_id": "default",
            "strategy_version_id": version.strategy_version_id,
            "config_json": serialize_strategy_config_json(*strategy.snapshot()),
        }
    )
    await registry.set_active("default", version.strategy_version_id)

    assert observed == ["first", "second", "second"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("trigger_name", "trigger_args"),
    [
        ("create_version", (_strategy(),)),
        ("set_active", ("default", "default-v2")),
    ],
)
async def test_runner_strategy_change_trigger_triggers_reselection(
    monkeypatch: pytest.MonkeyPatch,
    trigger_name: str,
    trigger_args: tuple[object, ...],
) -> None:
    runtime_pool = RuntimePool()
    discovery = IdleDiscoverySensor()
    market_data = RecordingMarketDataSensor()
    selector = RecordingSelector(returned_asset_ids=["near-no", "near-yes"])
    subscription_controller = RecordingSubscriptionController(sink=market_data)
    registry_box: dict[str, object] = {}

    async def fake_create_pool(*, dsn: str, min_size: int, max_size: int) -> RuntimePool:
        del dsn, min_size, max_size
        return runtime_pool

    class CapturingRegistry:
        def __init__(self, pool: object) -> None:
            del pool
            self.callbacks: list[Callable[[], Awaitable[None]]] = []
            registry_box["instance"] = self

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

        async def create_version(self, *_: object) -> object:
            if not self.callbacks:
                msg = "strategy change callback was not wired"
                raise AssertionError(msg)
            for callback in tuple(self.callbacks):
                await callback()
            return object()

        async def set_active(self, *_: object) -> None:
            if not self.callbacks:
                msg = "strategy change callback was not wired"
                raise AssertionError(msg)
            for callback in tuple(self.callbacks):
                await callback()

    monkeypatch.setattr("pms.runner.asyncpg.create_pool", fake_create_pool)
    monkeypatch.setattr("pms.runner.MarketDiscoverySensor", lambda **kwargs: discovery)
    monkeypatch.setattr("pms.runner.MarketDataSensor", lambda **kwargs: market_data)
    monkeypatch.setattr("pms.runner.PostgresStrategyRegistry", CapturingRegistry)
    monkeypatch.setattr("pms.runner.MarketSelector", lambda *args, **kwargs: selector)
    monkeypatch.setattr(
        "pms.runner.SensorSubscriptionController",
        lambda sink: subscription_controller,
    )

    runner = Runner(
        config=_settings(RunMode.LIVE),
        historical_data_path=FIXTURE_PATH,
    )
    await runner.start()

    registry = registry_box["instance"]
    assert isinstance(registry, CapturingRegistry)
    trigger = getattr(registry, trigger_name)
    await trigger(*trigger_args)

    await asyncio.wait_for(selector.call_started.wait(), timeout=2.0)
    assert selector.calls == 1
    assert subscription_controller.calls == 0

    selector.call_release.set()
    await asyncio.wait_for(subscription_controller.call_started.wait(), timeout=2.0)
    assert subscription_controller.calls == 1

    subscription_controller.call_release.set()
    market_data.update_release.set()
    await asyncio.wait_for(market_data.update_started.wait(), timeout=2.0)

    assert market_data.asset_ids == ["near-no", "near-yes"]

    await runner.stop()


@dataclass
class _AcquirableClosablePool:
    connection: FakeConnection
    close_calls: int = 0

    def acquire(self) -> FakeAcquire:
        return FakeAcquire(self.connection)

    async def close(self) -> None:
        self.close_calls += 1


@pytest.mark.asyncio
async def test_runner_constructs_single_strategy_registry_with_callback_for_bootstrap_and_wiring(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = FakeConnection(fetchval_results=[0, "default-v1"])
    fake_pool = _AcquirableClosablePool(connection=connection)
    discovery = IdleDiscoverySensor()
    market_data = RecordingMarketDataSensor()
    selector = RecordingSelector(returned_asset_ids=[])
    subscription_controller = RecordingSubscriptionController(sink=market_data)

    constructed: list[dict[str, object]] = []

    class TrackingRegistry:
        def __init__(self, pool: object) -> None:
            constructed.append(
                {"pool_id": id(pool), "callbacks": []}
            )
            self._pool = pool
            self._callbacks: list[Callable[[], Awaitable[None]]] = []

        def register_change_callback(
            self,
            callback: Callable[[], Awaitable[None]],
        ) -> None:
            if callback not in self._callbacks:
                self._callbacks.append(callback)
            cast(list[Callable[[], Awaitable[None]]], constructed[-1]["callbacks"]).append(
                callback
            )

        def unregister_change_callback(
            self,
            callback: Callable[[], Awaitable[None]],
        ) -> None:
            if callback in self._callbacks:
                self._callbacks.remove(callback)

        async def get_by_id(self, strategy_id: str) -> None:
            del strategy_id
            return None

        async def list_market_selections(self) -> list[object]:
            return []

        async def set_active(self, strategy_id: str, version_id: str) -> None:
            del strategy_id, version_id
            for callback in tuple(self._callbacks):
                await callback()

    async def fake_create_pool(
        *, dsn: str, min_size: int, max_size: int
    ) -> _AcquirableClosablePool:
        del dsn, min_size, max_size
        return fake_pool

    monkeypatch.setattr("pms.runner.asyncpg.create_pool", fake_create_pool)
    monkeypatch.setattr("pms.runner.MarketDiscoverySensor", lambda **kwargs: discovery)
    monkeypatch.setattr("pms.runner.MarketDataSensor", lambda **kwargs: market_data)
    monkeypatch.setattr("pms.runner.PostgresStrategyRegistry", TrackingRegistry)
    monkeypatch.setattr("pms.runner.MarketSelector", lambda *args, **kwargs: selector)
    monkeypatch.setattr(
        "pms.runner.SensorSubscriptionController",
        lambda sink: subscription_controller,
    )

    approval_path, audit_path = make_private_live_paths(
        prefix="pms-strategy-reselect-start-"
    )
    paper_report_path, rehearsal_report_path = make_live_report_paths(
        prefix="pms-strategy-reselect-start-reports-"
    )
    settings = PMSSettings(
        mode=RunMode.LIVE,
        secret_source="fly",
        live_trading_enabled=True,
        live_exit_criteria_ratified_by="test-operator",
        live_exit_criteria_ratified_at=datetime(2026, 5, 25, tzinfo=UTC),
        live_compliance_reviewed_by="test-compliance",
        live_compliance_reviewed_at=datetime(2026, 5, 25, tzinfo=UTC),
        live_compliance_jurisdiction="US",
        live_paper_soak_report_path=paper_report_path,
        live_operator_rehearsal_report_path=rehearsal_report_path,
        live_emergency_audit_path=str(
            Path(approval_path).parent / "live-emergency-audit.jsonl"
        ),
        live_first_order_audit_path=audit_path,
        auto_migrate_default_v2=True,
        database=DatabaseSettings(
            dsn="postgresql://localhost/pms_test_runner",
            pool_min_size=2,
            pool_max_size=10,
        ),
        risk=RiskSettings(
            max_position_per_market=1000.0,
            max_total_exposure=10_000.0,
            max_drawdown_pct=20.0,
            max_daily_loss_usdc=20.0,
            max_open_positions=5,
            max_exposure_per_risk_group=5_000.0,
            max_quantity_shares=500.0,
        ),
        controller=ControllerSettings(time_in_force="IOC", quote_source="dual"),
        discord=DiscordSettings(
            webhook_url=SecretStr(
                "https://discord.example/webhooks/strategy-reselect/start"
            ),
            alert_dir=str(Path(approval_path).parent / "discord-alerts"),
        ),
        polymarket=_live_polymarket_settings(approval_path),
    )
    settings.live_preflight_artifact_path = make_live_preflight_artifact_path(
        prefix="pms-strategy-reselect-start-preflight-",
        settings=settings,
    )

    runner = Runner(config=settings, historical_data_path=FIXTURE_PATH)
    await runner.start()
    try:
        registry: Any = runner.strategy_registry
        assert registry is not None
        assert isinstance(registry, TrackingRegistry)

        assert len(constructed) == 1, (
            f"Expected a single PostgresStrategyRegistry construction, "
            f"got {len(constructed)}. Bootstrap migration and "
            "active-perception wiring must share one registry instance so "
            "change callbacks fire from every mutation site."
        )
        callbacks = cast(list[Callable[[], Awaitable[None]]], constructed[0]["callbacks"])
        assert callbacks == [
            runner._request_reselection,
            runner._sync_controller_runtimes,
        ]

        runner._reselection_requested.clear()
        await registry.set_active("default", "default-v2")
        assert runner._reselection_requested.is_set(), (
            "Mutation through the Runner-owned registry failed to set "
            "_reselection_requested; callback is not wired."
        )
    finally:
        await runner.stop()


@pytest.mark.asyncio
async def test_runner_start_stop_cycles_unregister_strategy_change_callbacks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime_pool = RuntimePool()
    discovery = IdleDiscoverySensor()
    market_data = RecordingMarketDataSensor()
    selector = RecordingSelector(returned_asset_ids=["near-no", "near-yes"])
    subscription_controller = RecordingSubscriptionController(sink=market_data)
    constructed: list["LifecycleRegistry"] = []

    async def fake_create_pool(*, dsn: str, min_size: int, max_size: int) -> RuntimePool:
        del dsn, min_size, max_size
        return runtime_pool

    class LifecycleRegistry:
        def __init__(self, pool: object) -> None:
            del pool
            self.callbacks: list[Callable[[], Awaitable[None]]] = []
            constructed.append(self)

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

        async def get_by_id(self, strategy_id: str) -> None:
            del strategy_id
            return None

        async def list_market_selections(self) -> list[object]:
            return []

        async def list_active_strategies(self) -> list[object]:
            return []

    monkeypatch.setattr("pms.runner.asyncpg.create_pool", fake_create_pool)
    monkeypatch.setattr("pms.runner.MarketDiscoverySensor", lambda **kwargs: discovery)
    monkeypatch.setattr("pms.runner.MarketDataSensor", lambda **kwargs: market_data)
    monkeypatch.setattr("pms.runner.PostgresStrategyRegistry", LifecycleRegistry)
    monkeypatch.setattr("pms.runner.MarketSelector", lambda *args, **kwargs: selector)
    monkeypatch.setattr(
        "pms.runner.SensorSubscriptionController",
        lambda sink: subscription_controller,
    )

    async def _runner_tasks() -> tuple[asyncio.Task[Any], ...]:
        await asyncio.sleep(0)
        return tuple(
            task
            for task in asyncio.all_tasks()
            if "Runner._periodic_reselection_loop"
            in getattr(task.get_coro(), "__qualname__", "")
            or task.get_name().startswith("controller-pipeline:")
        )

    baseline_tasks = await _runner_tasks()
    assert baseline_tasks == ()

    for _ in range(10):
        runner = Runner(
            config=_settings(RunMode.LIVE),
            historical_data_path=FIXTURE_PATH,
        )
        await runner.start()
        try:
            registry = constructed[-1]
            assert registry.callbacks == [
                runner._request_reselection,
                runner._sync_controller_runtimes,
            ]
        finally:
            await runner.stop()
        assert registry.callbacks == []
        assert await _runner_tasks() == ()
