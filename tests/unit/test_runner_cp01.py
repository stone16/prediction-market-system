from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest
from pydantic import SecretStr

from pms.actuator.executor import ActuatorExecutor
from pms.actuator.adapters.polymarket import (
    FileFirstLiveOrderGate,
    LiveOrderPreview,
    PolymarketActuator,
    PolymarketSDKClient,
)
from pms.storage.first_order_audit import JsonlFirstOrderAuditWriter
from pms.config import (
    ControllerSettings,
    DatabaseSettings,
    DiscordSettings,
    PMSSettings,
    PolymarketSettings,
    RiskSettings,
)
from pms.core.enums import RunMode, TimeInForce
from pms.core.models import (
    LiveTradingDisabledError,
    MarketSignal,
    OrderState,
    Portfolio,
    ReconciliationReport,
    TradeDecision,
    VenueAccountSnapshot,
    VenueCredentials,
)
from pms.live_preflight import live_preflight_active_strategies_fingerprint
from pms.live_preflight import require_live_preflight_active_strategies_artifact
from pms.market_selection.merge import StrategyMarketSet
from pms.runner import ActuatorWorkItem, Runner
from pms.strategies.projections import (
    ActiveStrategy,
    CalibrationSpec,
    EvalSpec,
    ForecasterSpec,
    MarketSelectionSpec,
    RiskParams,
    StrategyConfig,
)
from tests.support.live_paths import (
    make_live_preflight_artifact_path,
    make_live_report_paths,
    make_private_live_paths,
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
        return _fake_registry_active_strategies()


def _fake_registry_active_strategies() -> list[ActiveStrategy]:
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
    attested_at = datetime.now(tz=UTC)
    approval_path, audit_path = make_private_live_paths(prefix="pms-runner-cp01-")
    paper_report_path, rehearsal_report_path = make_live_report_paths(
        prefix="pms-runner-cp01-reports-"
    )
    settings = PMSSettings(
        mode=RunMode.LIVE,
        secret_source="fly",
        live_trading_enabled=True,
        api_token="live-api-token",
        auto_migrate_default_v2=False,
        live_exit_criteria_ratified_by="operator",
        live_exit_criteria_ratified_at=attested_at,
        live_compliance_reviewed_by="counsel",
        live_compliance_reviewed_at=attested_at,
        live_compliance_jurisdiction="US-operator-approved",
        live_paper_soak_report_path=paper_report_path,
        live_operator_rehearsal_report_path=rehearsal_report_path,
        live_emergency_audit_path=str(
            Path(approval_path).parent / "live-emergency-audit.jsonl"
        ),
        live_first_order_audit_path=audit_path,
        database=DatabaseSettings(
            dsn="postgresql://localhost/pms_test_runner_cp01",
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
            max_quantity_shares=10_000.0,
        ),
        controller=ControllerSettings(time_in_force="IOC", quote_source="dual"),
        discord=DiscordSettings(
            webhook_url=SecretStr("https://discord.example/webhooks/runner-cp01/unit"),
            alert_dir=str(Path(approval_path).parent / "discord-alerts"),
        ),
        polymarket=_live_polymarket_settings(approval_path),
    )
    settings.live_preflight_artifact_path = make_live_preflight_artifact_path(
        prefix="pms-runner-cp01-preflight-",
        settings=settings,
        active_strategies_fingerprint=live_preflight_active_strategies_fingerprint(
            _fake_registry_active_strategies()
        ),
    )
    return settings


@pytest.mark.asyncio
async def test_runner_builds_live_polymarket_adapter_with_sdk_client_and_file_gate(
    tmp_path: Path,
) -> None:
    attested_at = datetime.now(tz=UTC)
    paper_report_path, rehearsal_report_path = make_live_report_paths(
        prefix="pms-runner-cp01-build-reports-"
    )
    settings = PMSSettings(
        mode=RunMode.LIVE,
        secret_source="fly",
        live_trading_enabled=True,
        api_token="live-api-token",
        auto_migrate_default_v2=False,
        live_exit_criteria_ratified_by="operator",
        live_exit_criteria_ratified_at=attested_at,
        live_compliance_reviewed_by="counsel",
        live_compliance_reviewed_at=attested_at,
        live_compliance_jurisdiction="US-operator-approved",
        live_paper_soak_report_path=paper_report_path,
        live_operator_rehearsal_report_path=rehearsal_report_path,
        live_emergency_audit_path=str(tmp_path / "live-emergency-audit.jsonl"),
        live_first_order_audit_path=str(tmp_path / "first-order-audit.jsonl"),
        discord=DiscordSettings(
            webhook_url=SecretStr("https://discord.example/webhooks/runner-cp01/build"),
            alert_dir=str(tmp_path / "discord-alerts"),
        ),
        risk=RiskSettings(
            max_drawdown_pct=20.0,
            max_daily_loss_usdc=20.0,
            max_open_positions=5,
            max_exposure_per_risk_group=250.0,
            max_quantity_shares=500.0,
        ),
        polymarket=PolymarketSettings(
            private_key="private-key",
            api_key="api-key",
            api_secret="api-secret",
            api_passphrase="passphrase",
            signature_type=1,
            funder_address="0x1111111111111111111111111111111111111111",
            first_live_order_approval_path=str(tmp_path / "approval.json"),
            operator_approval_mode="every_order",
        ),
        controller=ControllerSettings(time_in_force="IOC", quote_source="dual"),
    )
    runner = Runner(config=settings, historical_data_path=FIXTURE_PATH)

    adapter = runner._build_adapter(RunMode.LIVE)  # noqa: SLF001

    assert isinstance(adapter, PolymarketActuator)
    assert isinstance(adapter.client, PolymarketSDKClient)
    assert isinstance(adapter.operator_gate, FileFirstLiveOrderGate)
    approval_path = tmp_path / "approval.json"
    approval_path.write_text(
        json.dumps(
            {
                "approved": True,
                "venue": "polymarket",
                "market_id": "m-runner-cp01",
                "token_id": "t-runner-cp01",
                "side": "BUY",
                "outcome": "YES",
                "max_notional_usdc": 1.0,
                "limit_price": 0.5,
                "max_slippage_bps": 50,
            }
        ),
        encoding="utf-8",
    )
    assert (
        await adapter.operator_gate.approve_first_order(
            LiveOrderPreview(
                max_notional_usdc=1.0,
                venue="polymarket",
                market_id="m-runner-cp01",
                token_id="t-runner-cp01",
                side="BUY",
                limit_price=0.5,
                max_slippage_bps=50,
                outcome="YES",
            )
        )
    ) is False
    assert isinstance(adapter.audit_writer, JsonlFirstOrderAuditWriter)
    assert adapter.audit_writer.path == tmp_path / "first-order-audit.jsonl"


def test_runner_live_adapter_carries_validated_active_strategy_fingerprint() -> None:
    settings = _settings()
    expected = live_preflight_active_strategies_fingerprint(
        _fake_registry_active_strategies()
    )
    runner = Runner(config=settings, historical_data_path=FIXTURE_PATH)
    runner._live_preflight_artifact_validated = True  # noqa: SLF001
    runner._live_preflight_active_strategies_fingerprint = expected  # noqa: SLF001

    adapter = runner._build_adapter(RunMode.LIVE)  # noqa: SLF001

    assert isinstance(adapter, PolymarketActuator)
    assert adapter.live_preflight_validated is True
    assert adapter.live_preflight_active_strategies_fingerprint == expected


def _live_polymarket_settings(approval_path: str | None = None) -> PolymarketSettings:
    if approval_path is None:
        approval_path, _ = make_private_live_paths(
            prefix="pms-runner-cp01-polymarket-"
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


@pytest.mark.asyncio
async def test_live_preflight_active_strategy_artifact_returns_verified_fingerprint() -> None:
    settings = _settings()
    expected = live_preflight_active_strategies_fingerprint(
        _fake_registry_active_strategies()
    )

    observed = await require_live_preflight_active_strategies_artifact(
        settings,
        FakeRegistry(FakePool()),
    )

    assert observed == expected


@pytest.mark.asyncio
async def test_live_runner_rejects_active_strategy_change_without_new_preflight() -> None:
    settings = _settings()
    expected = live_preflight_active_strategies_fingerprint(
        _fake_registry_active_strategies()
    )
    runner = Runner(config=settings, historical_data_path=FIXTURE_PATH)
    runner._live_preflight_artifact_validated = True  # noqa: SLF001
    runner._live_preflight_active_strategies_fingerprint = expected  # noqa: SLF001
    runner._strategy_registry = cast(Any, _ChangedRegistry())  # noqa: SLF001
    runner._controller_factory = cast(Any, _NoopControllerFactory())  # noqa: SLF001

    with pytest.raises(LiveTradingDisabledError, match="active strategies changed"):
        await runner._build_controller_runtimes()  # noqa: SLF001

    assert runner.live_trading_suspended is True
    assert runner._live_preflight_artifact_validated is False  # noqa: SLF001
    assert runner._live_preflight_active_strategies_fingerprint is None  # noqa: SLF001


@pytest.mark.asyncio
async def test_live_runner_blocks_orders_after_active_strategy_preflight_mismatch() -> None:
    settings = _settings()
    runner = Runner(config=settings, historical_data_path=FIXTURE_PATH)
    runner._live_trading_suspended_reason = (  # noqa: SLF001
        "active_strategy_preflight_mismatch"
    )
    submitted: list[object] = []

    async def fake_execute(
        decision: TradeDecision,
        **kwargs: object,
    ) -> None:
        submitted.append((decision, kwargs))

    runner.actuator_executor = cast(  # noqa: SLF001
        ActuatorExecutor,
        SimpleNamespace(execute=fake_execute),
    )
    runner._stop_event.set()  # noqa: SLF001
    await runner._decision_queue.put(  # noqa: SLF001
        ActuatorWorkItem(decision=_decision(), signal=None)
    )

    await asyncio.wait_for(runner._actuator_loop(), timeout=1.0)  # noqa: SLF001

    assert submitted == []
    assert runner.live_trading_suspended is True


@pytest.mark.asyncio
async def test_live_runner_suspends_on_strategy_change_without_active_perception_sensors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _settings()
    fake_pool = FakePool()
    registry_box: dict[str, object] = {}

    async def fake_create_pool(*, dsn: str, min_size: int, max_size: int) -> FakePool:
        del dsn, min_size, max_size
        return fake_pool

    async def fake_ensure_factor_catalog(
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

    class MutableRegistry:
        def __init__(self, pool: object) -> None:
            del pool
            self.active_strategies = _fake_registry_active_strategies()
            self.callbacks: list[Any] = []
            registry_box["instance"] = self

        def register_change_callback(self, callback: Any) -> None:
            if callback not in self.callbacks:
                self.callbacks.append(callback)

        def unregister_change_callback(self, callback: Any) -> None:
            if callback in self.callbacks:
                self.callbacks.remove(callback)

        async def list_active_strategies(self) -> list[ActiveStrategy]:
            return list(self.active_strategies)

    monkeypatch.setattr("pms.runner.asyncpg.create_pool", fake_create_pool)
    monkeypatch.setattr("pms.runner.ensure_factor_catalog", fake_ensure_factor_catalog)
    monkeypatch.setattr("pms.runner.FactorService", _NoopFactorService)
    monkeypatch.setattr("pms.runner.PostgresStrategyRegistry", MutableRegistry)

    runner = Runner(
        config=settings,
        historical_data_path=FIXTURE_PATH,
        sensors=[],
    )
    runner._controller_factory = cast(Any, _NoopControllerFactory())  # noqa: SLF001

    await runner.start()
    try:
        registry = registry_box["instance"]
        assert isinstance(registry, MutableRegistry)
        assert registry.callbacks == [
            runner._request_reselection,  # noqa: SLF001
            runner._sync_controller_runtimes,  # noqa: SLF001
        ]
        assert set(runner._controller_runtimes) == {"alpha", "beta"}  # noqa: SLF001

        registry.active_strategies = [
            _active_strategy(
                strategy_id="gamma",
                strategy_version_id="gamma-v1",
                forecaster_names=("rules",),
            )
        ]
        for callback in tuple(registry.callbacks):
            try:
                await callback()
            except LiveTradingDisabledError:
                pass

        assert runner.live_trading_suspended is True
        assert runner._live_preflight_artifact_validated is False  # noqa: SLF001
        assert runner._live_preflight_active_strategies_fingerprint is None  # noqa: SLF001
    finally:
        await runner.stop()


class _ChangedRegistry:
    async def list_active_strategies(self) -> list[ActiveStrategy]:
        return [
            _active_strategy(
                strategy_id="gamma",
                strategy_version_id="gamma-v1",
                forecaster_names=("rules",),
            )
        ]


class _NoopControllerFactory:
    def build(self, active_strategy: ActiveStrategy) -> object:
        del active_strategy
        return object()

    def build_many(
        self,
        active_strategies: list[ActiveStrategy],
    ) -> dict[str, object]:
        return {strategy.strategy_id: object() for strategy in active_strategies}


def _decision() -> TradeDecision:
    return TradeDecision(
        decision_id="runner-cp01-live-decision",
        market_id="runner-cp01",
        token_id="shared-token",
        venue="polymarket",
        side="BUY",
        notional_usdc=10.0,
        order_type="limit",
        max_slippage_bps=50,
        stop_conditions=[],
        prob_estimate=0.65,
        expected_edge=0.1,
        time_in_force=TimeInForce.IOC,
        opportunity_id="opp-runner-cp01",
        strategy_id="gamma",
        strategy_version_id="gamma-v1",
        limit_price=0.4,
        action="BUY",
        outcome="YES",
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
        external_signal={"event_id": "runner-cp01-event", "fair_value": 0.55},
        fetched_at=datetime(2026, 4, 19, tzinfo=UTC),
        market_status="open",
    )


def test_runner_only_caches_market_alias_for_market_level_paper_orderbooks() -> None:
    runner = Runner(
        config=PMSSettings(mode=RunMode.PAPER, auto_migrate_default_v2=False),
        historical_data_path=FIXTURE_PATH,
    )
    token_orderbook = {
        "bids": [{"price": 0.38, "size": 10.0}],
        "asks": [{"price": 0.42, "size": 10.0}],
    }
    market_orderbook = {
        "bids": [{"price": 0.39, "size": 10.0}],
        "asks": [{"price": 0.41, "size": 10.0}],
    }

    runner._remember_paper_orderbook(  # noqa: SLF001
        replace(_signal(), token_id="no-token", orderbook=token_orderbook)
    )

    assert runner._paper_orderbooks["no-token"] is token_orderbook  # noqa: SLF001
    assert "runner-cp01" not in runner._paper_orderbooks  # noqa: SLF001

    runner._remember_paper_orderbook(  # noqa: SLF001
        replace(_signal(), token_id=None, orderbook=market_orderbook)
    )

    assert runner._paper_orderbooks["runner-cp01"] is market_orderbook  # noqa: SLF001


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
            metadata=(
                ("owner", "test"),
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
        eval_spec=EvalSpec(metrics=("brier", "pnl")),
        forecaster=ForecasterSpec(
            forecasters=tuple((name, ()) for name in forecaster_names)
        ),
        market_selection=MarketSelectionSpec(
            venue="polymarket",
            resolution_time_max_horizon_days=7,
            volume_min_usdc=500.0,
        ),
        calibration=CalibrationSpec(enabled=True),
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
