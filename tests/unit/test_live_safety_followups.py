from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
import sys
from types import SimpleNamespace
from typing import Any, Literal, cast

import asyncpg
import pytest
import httpx

from pms.api.app import create_app
from pms.actuator.adapters.polymarket import (
    FileFirstLiveOrderGate,
    LiveOrderPreview,
    LivePreSubmitQuote,
    LiveVenueBook,
    PolymarketActuator,
    PolymarketBookQuoteProvider,
    PolymarketDirectQuoteProvider,
    PolymarketOrderRequest,
    PolymarketOrderResult,
    PolymarketRoutingQuoteProvider,
    PolymarketSDKClient,
    PolymarketSubmissionUnknownError,
    PolymarketVenueAccountReconciler,
)
from pms.config import (
    ControllerSettings,
    PMSSettings,
    PolymarketSettings,
    RiskSettings,
    validate_live_mode_ready,
)
from pms.controller.calibrators.netcal import NetcalCalibrator
from pms.controller.factor_snapshot import FactorSnapshot
from pms.controller.pipeline import ControllerPipeline
from pms.controller.router import Router
from pms.core.enums import MarketStatus, OrderStatus, RunMode, Side, TimeInForce
from pms.core.models import (
    BookLevel,
    BookSnapshot,
    LiveTradingDisabledError,
    Market,
    MarketSignal,
    OrderState,
    Portfolio,
    Position,
    TradeDecision,
    VenueCredentials,
)
from pms.runner import (
    ReconciliationReport,
    Runner,
    VenueAccountSnapshot,
)
from pms.storage.decision_store import DECISION_STATUSES, validate_decision_status_transition
from pms.storage.live_reconciliation import SubmissionUnknownReconciliationStore
from pms.live_cli import build_parser
from pms.strategies.projections import (
    ActiveStrategy,
    EvalSpec,
    FactorCompositionStep,
    ForecasterSpec,
    MarketSelectionSpec,
    RiskParams,
    StrategyConfig,
)


class ConstantForecaster:
    def __init__(self, probability: float) -> None:
        self.probability = probability

    def predict(self, signal: MarketSignal) -> tuple[float, float, str]:
        del signal
        return self.probability, 0.0, "constant"

    async def forecast(self, signal: MarketSignal) -> float:
        del signal
        return self.probability


class FixedSizer:
    def size(self, *, prob: float, market_price: float, portfolio: Portfolio) -> float:
        del prob, market_price, portfolio
        return 10.0


@dataclass(frozen=True)
class SnapshotReader:
    snapshot_value: FactorSnapshot

    async def snapshot(
        self,
        *,
        market_id: str,
        as_of: datetime,
        required: Sequence[FactorCompositionStep],
        strategy_id: str,
        strategy_version_id: str,
    ) -> FactorSnapshot:
        del market_id, as_of, required, strategy_id, strategy_version_id
        return self.snapshot_value


def _strategy(
    factor_id: str = "metaculus_prior",
) -> ActiveStrategy:
    return ActiveStrategy(
        strategy_id="alpha",
        strategy_version_id="alpha-v1",
        config=StrategyConfig(
            strategy_id="alpha",
            factor_composition=(
                FactorCompositionStep(
                    factor_id=factor_id,
                    role="weighted",
                    param="",
                    weight=1.0,
                    threshold=None,
                    freshness_sla_s=300.0,
                ),
            ),
            metadata=(),
        ),
        risk=RiskParams(
            max_position_notional_usdc=1000.0,
            max_daily_drawdown_pct=2.5,
            min_order_size_usdc=1.0,
        ),
        eval_spec=EvalSpec(metrics=("brier",)),
        forecaster=ForecasterSpec(forecasters=(("rules", ()),)),
        market_selection=MarketSelectionSpec(
            venue="polymarket",
            resolution_time_max_horizon_days=7,
            volume_min_usdc=0.0,
        ),
    )


def _signal(
    *,
    external_signal: dict[str, Any] | None = None,
    fetched_at: datetime | None = None,
) -> MarketSignal:
    return MarketSignal(
        market_id="m-live-safety",
        token_id="t-yes",
        venue="polymarket",
        title="Will live safety pass?",
        yes_price=0.4,
        volume_24h=10_000.0,
        resolves_at=datetime(2026, 5, 1, tzinfo=UTC),
        orderbook={
            "bids": [{"price": 0.39, "size": 100.0}],
            "asks": [{"price": 0.41, "size": 100.0}],
        },
        external_signal=external_signal or {},
        fetched_at=fetched_at or datetime(2026, 4, 27, tzinfo=UTC),
        market_status=MarketStatus.OPEN.value,
    )


def _portfolio() -> Portfolio:
    return Portfolio(
        total_usdc=1000.0,
        free_usdc=1000.0,
        locked_usdc=0.0,
        open_positions=[],
    )


def _live_settings(**overrides: object) -> PMSSettings:
    values: dict[str, Any] = {
        "mode": RunMode.LIVE,
        "live_trading_enabled": True,
        "auto_migrate_default_v2": False,
        "controller": ControllerSettings(time_in_force="IOC", min_volume=0.0),
        "risk": RiskSettings(
            max_position_per_market=1000.0,
            max_total_exposure=10_000.0,
            min_order_usdc=1.0,
        ),
        "polymarket": PolymarketSettings(
            private_key="private-key",
            api_key="api-key",
            api_secret="api-secret",
            api_passphrase="passphrase",
            signature_type=1,
            funder_address="0xabc",
        ),
    }
    values.update(overrides)
    return PMSSettings(**values)


def test_live_mode_ready_requires_account_reconciliation_gate() -> None:
    settings = _live_settings(live_account_reconciliation_required=False)

    with pytest.raises(LiveTradingDisabledError, match="account reconciliation"):
        validate_live_mode_ready(settings)


def _decision(
    *,
    outcome: Literal["YES", "NO"] = "YES",
    time_in_force: TimeInForce = TimeInForce.IOC,
) -> TradeDecision:
    return TradeDecision(
        decision_id=f"d-{outcome.lower()}",
        market_id="m-live-safety",
        token_id="t-yes" if outcome == "YES" else "t-no",
        venue="polymarket",
        side=Side.BUY.value,
        notional_usdc=10.0,
        order_type="limit",
        max_slippage_bps=50,
        stop_conditions=["unit-test"],
        prob_estimate=0.7,
        expected_edge=0.3,
        time_in_force=time_in_force,
        opportunity_id=f"op-{outcome.lower()}",
        strategy_id="alpha",
        strategy_version_id="alpha-v1",
        limit_price=0.4,
        action=Side.BUY.value,
        outcome=outcome,
        intent_key="intent-live-safety",
    )


@dataclass
class FakeBookStore:
    market: Market | None
    snapshot: BookSnapshot | None
    levels: list[BookLevel]

    async def read_market(self, market_id: str) -> Market | None:
        del market_id
        return self.market

    async def read_latest_snapshot(
        self,
        market_id: str,
        token_id: str,
    ) -> BookSnapshot | None:
        del market_id, token_id
        return self.snapshot

    async def read_levels_for_snapshot(self, snapshot_id: int) -> list[BookLevel]:
        del snapshot_id
        return self.levels


@dataclass
class FakeVenueBookClient:
    book: LiveVenueBook
    calls: int = 0

    async def read_order_book(
        self,
        order: PolymarketOrderRequest,
        credentials: object,
    ) -> LiveVenueBook:
        del order, credentials
        self.calls += 1
        return self.book


@pytest.mark.asyncio
async def test_polymarket_book_quote_provider_uses_fresh_book_depth() -> None:
    now = datetime(2026, 4, 27, 12, 0, tzinfo=UTC)
    provider = PolymarketBookQuoteProvider(
        store=FakeBookStore(
            market=Market(
                condition_id="m-live-safety",
                slug="live-safety",
                question="Will live safety pass?",
                venue="polymarket",
                resolves_at=now + timedelta(days=1),
                created_at=now - timedelta(days=1),
                last_seen_at=now,
            ),
            snapshot=BookSnapshot(
                id=11,
                market_id="m-live-safety",
                token_id="t-yes",
                ts=now - timedelta(milliseconds=250),
                hash="book-hash",
                source="subscribe",
            ),
            levels=[
                BookLevel(
                    snapshot_id=11,
                    market_id="m-live-safety",
                    side="BUY",
                    price=0.39,
                    size=20.0,
                ),
                BookLevel(
                    snapshot_id=11,
                    market_id="m-live-safety",
                    side="SELL",
                    price=0.40,
                    size=10.0,
                ),
                BookLevel(
                    snapshot_id=11,
                    market_id="m-live-safety",
                    side="SELL",
                    price=0.41,
                    size=100.0,
                ),
            ],
        ),
        clock=lambda: now,
    )

    quote = await provider.quote(
        PolymarketOrderRequest(
            market_id="m-live-safety",
            token_id="t-yes",
            side="BUY",
            price=0.40,
            size=25.0,
            notional_usdc=10.0,
            estimated_quantity=25.0,
            order_type="limit",
            time_in_force="IOC",
            max_slippage_bps=50,
        ),
        _live_settings().polymarket.credentials(),
    )

    assert quote.market_status == "open"
    assert quote.book_age_ms == pytest.approx(250.0)
    assert quote.best_executable_price == pytest.approx(0.40)
    assert quote.executable_notional_usdc == pytest.approx(4.0)
    assert quote.spread_bps > 0.0
    assert quote.quote_hash == "book-hash"


@pytest.mark.asyncio
async def test_polymarket_direct_quote_provider_uses_venue_book() -> None:
    now = datetime(2026, 4, 27, 12, 0, tzinfo=UTC)
    client = FakeVenueBookClient(
        LiveVenueBook(
            market_id="m-live-safety",
            token_id="t-yes",
            bids=(
                BookLevel(
                    snapshot_id=0,
                    market_id="m-live-safety",
                    side=Side.BUY.value,
                    price=0.39,
                    size=100.0,
                ),
            ),
            asks=(
                BookLevel(
                    snapshot_id=0,
                    market_id="m-live-safety",
                    side=Side.SELL.value,
                    price=0.40,
                    size=40.0,
                ),
            ),
            book_ts=now - timedelta(milliseconds=100),
            quote_hash="venue-book",
            market_status="open",
        )
    )
    provider = PolymarketDirectQuoteProvider(book_client=client, clock=lambda: now)

    quote = await provider.quote(
        PolymarketOrderRequest(
            market_id="m-live-safety",
            token_id="t-yes",
            side="BUY",
            price=0.40,
            size=25.0,
            notional_usdc=10.0,
            estimated_quantity=25.0,
            order_type="limit",
            time_in_force="IOC",
            max_slippage_bps=50,
        ),
        _live_settings().polymarket.credentials(),
    )

    assert client.calls == 1
    assert quote.source == "venue_direct"
    assert quote.quote_hash == "venue-book"
    assert quote.book_age_ms == pytest.approx(100.0)
    assert quote.executable_notional_usdc == pytest.approx(16.0)


@pytest.mark.asyncio
async def test_polymarket_sdk_direct_quote_honors_venue_accepting_orders_flag(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 4, 27, 12, 0, tzinfo=UTC)

    class FakeApiCreds:
        def __init__(self, **kwargs: object) -> None:
            del kwargs

    class FakeClobClient:
        def __init__(self, **kwargs: object) -> None:
            del kwargs

        def get_order_book(self, token_id: str) -> dict[str, object]:
            assert token_id == "t-yes"
            return {
                "timestamp": now.isoformat(),
                "accepting_orders": False,
                "bids": [{"price": 0.39, "size": 100.0}],
                "asks": [{"price": 0.40, "size": 100.0}],
            }

    monkeypatch.setitem(
        sys.modules,
        "py_clob_client_v2",
        SimpleNamespace(ApiCreds=FakeApiCreds, ClobClient=FakeClobClient),
    )

    provider = PolymarketDirectQuoteProvider(
        book_client=PolymarketSDKClient(),
        clock=lambda: now,
    )

    quote = await provider.quote(
        PolymarketOrderRequest(
            market_id="m-live-safety",
            token_id="t-yes",
            side="BUY",
            price=0.40,
            size=25.0,
            notional_usdc=10.0,
            estimated_quantity=25.0,
            order_type="limit",
            time_in_force="IOC",
            max_slippage_bps=50,
        ),
        _live_settings().polymarket.credentials(),
    )

    assert quote.market_status == "not_accepting_orders"


@pytest.mark.asyncio
async def test_routing_quote_provider_dual_mode_fails_on_material_mismatch() -> None:
    now = datetime(2026, 4, 27, 12, 0, tzinfo=UTC)
    snapshot_provider = PolymarketBookQuoteProvider(
        store=FakeBookStore(
            market=Market(
                condition_id="m-live-safety",
                slug="live-safety",
                question="Will live safety pass?",
                venue="polymarket",
                resolves_at=now + timedelta(days=1),
                created_at=now - timedelta(days=1),
                last_seen_at=now,
            ),
            snapshot=BookSnapshot(
                id=24,
                market_id="m-live-safety",
                token_id="t-yes",
                ts=now - timedelta(milliseconds=100),
                hash="snapshot-book",
                source="subscribe",
            ),
            levels=[
                BookLevel(
                    snapshot_id=24,
                    market_id="m-live-safety",
                    side=Side.SELL.value,
                    price=0.40,
                    size=100.0,
                ),
            ],
        ),
        clock=lambda: now,
    )
    direct_provider = PolymarketDirectQuoteProvider(
        book_client=FakeVenueBookClient(
            LiveVenueBook(
                market_id="m-live-safety",
                token_id="t-yes",
                bids=(),
                asks=(
                    BookLevel(
                        snapshot_id=0,
                        market_id="m-live-safety",
                        side=Side.SELL.value,
                        price=0.44,
                        size=100.0,
                    ),
                ),
                book_ts=now - timedelta(milliseconds=100),
                quote_hash="direct-book",
                market_status="open",
            )
        ),
        clock=lambda: now,
    )
    settings = _live_settings(
        controller=ControllerSettings(
            time_in_force="IOC",
            min_volume=0.0,
            quote_source="dual",
            dual_quote_max_price_delta_bps=25.0,
        )
    )
    provider = PolymarketRoutingQuoteProvider(
        snapshot_provider=snapshot_provider,
        direct_provider=direct_provider,
        settings=settings,
    )

    with pytest.raises(LiveTradingDisabledError, match="dual quote mismatch"):
        await provider.quote(
            PolymarketOrderRequest(
                market_id="m-live-safety",
                token_id="t-yes",
                side="BUY",
                price=0.45,
                size=25.0,
                notional_usdc=10.0,
                estimated_quantity=25.0,
                order_type="limit",
                time_in_force="IOC",
                max_slippage_bps=50,
            ),
            settings.polymarket.credentials(),
        )


@pytest.mark.asyncio
async def test_routing_quote_provider_uses_direct_quote_for_first_live_order() -> None:
    now = datetime(2026, 4, 27, 12, 0, tzinfo=UTC)
    direct_client = FakeVenueBookClient(
        LiveVenueBook(
            market_id="m-live-safety",
            token_id="t-yes",
            bids=(),
            asks=(
                BookLevel(
                    snapshot_id=0,
                    market_id="m-live-safety",
                    side=Side.SELL.value,
                    price=0.40,
                    size=100.0,
                ),
            ),
            book_ts=now - timedelta(milliseconds=100),
            quote_hash="first-direct",
            market_status="open",
        )
    )
    provider = PolymarketRoutingQuoteProvider(
        snapshot_provider=PolymarketBookQuoteProvider(
            store=FakeBookStore(market=None, snapshot=None, levels=[]),
            clock=lambda: now,
        ),
        direct_provider=PolymarketDirectQuoteProvider(
            book_client=direct_client,
            clock=lambda: now,
        ),
        settings=_live_settings(),
    )

    quote = await provider.quote_for_order(
        PolymarketOrderRequest(
            market_id="m-live-safety",
            token_id="t-yes",
            side="BUY",
            price=0.40,
            size=25.0,
            notional_usdc=10.0,
            estimated_quantity=25.0,
            order_type="limit",
            time_in_force="IOC",
            max_slippage_bps=50,
        ),
        _live_settings().polymarket.credentials(),
        first_live_order=True,
    )

    assert quote.source == "venue_direct"
    assert quote.quote_hash == "first-direct"
    assert direct_client.calls == 1


@pytest.mark.asyncio
async def test_future_dated_book_snapshot_fails_quote_guard() -> None:
    now = datetime(2026, 4, 27, 12, 0, tzinfo=UTC)
    provider = PolymarketBookQuoteProvider(
        store=FakeBookStore(
            market=Market(
                condition_id="m-live-safety",
                slug="live-safety",
                question="Will live safety pass?",
                venue="polymarket",
                resolves_at=now + timedelta(days=1),
                created_at=now - timedelta(days=1),
                last_seen_at=now,
            ),
            snapshot=BookSnapshot(
                id=22,
                market_id="m-live-safety",
                token_id="t-yes",
                ts=now + timedelta(seconds=5),
                hash="future-book",
                source="subscribe",
            ),
            levels=[
                BookLevel(
                    snapshot_id=22,
                    market_id="m-live-safety",
                    side=Side.SELL.value,
                    price=0.40,
                    size=100.0,
                ),
            ],
        ),
        clock=lambda: now,
    )

    with pytest.raises(LiveTradingDisabledError, match="timestamp is in the future"):
        await provider.quote(
            PolymarketOrderRequest(
                market_id="m-live-safety",
                token_id="t-yes",
                side="BUY",
                price=0.40,
                size=25.0,
                notional_usdc=10.0,
                estimated_quantity=25.0,
                order_type="limit",
                time_in_force="IOC",
                max_slippage_bps=50,
            ),
            _live_settings().polymarket.credentials(),
        )


def test_runner_live_adapter_uses_real_quote_provider_when_live_enabled() -> None:
    runner = Runner(config=_live_settings())
    runner._pg_pool = cast(asyncpg.Pool, object())  # noqa: SLF001

    adapter = runner._build_adapter(RunMode.LIVE)  # noqa: SLF001

    assert isinstance(adapter, PolymarketActuator)
    assert isinstance(adapter.quote_provider, PolymarketRoutingQuoteProvider)
    assert isinstance(adapter.quote_provider.snapshot_provider, PolymarketBookQuoteProvider)
    assert isinstance(adapter.quote_provider.direct_provider, PolymarketDirectQuoteProvider)


@pytest.mark.asyncio
async def test_external_signal_numeric_key_does_not_satisfy_required_raw_factor() -> None:
    pipeline = ControllerPipeline(
        strategy=_strategy("metaculus_prior"),
        factor_reader=SnapshotReader(
            FactorSnapshot(
                values={},
                missing_factors=(("metaculus_prior", ""),),
                snapshot_hash="missing-metaculus",
            )
        ),
        forecasters=(ConstantForecaster(0.7),),
        calibrator=NetcalCalibrator(),
        sizer=FixedSizer(),
        router=Router(ControllerSettings(min_volume=0.0)),
        settings=PMSSettings(
            mode=RunMode.LIVE,
            controller=ControllerSettings(min_volume=0.0),
        ),
    )

    emission = await pipeline.on_signal(
        _signal(external_signal={"metaculus_prior": 0.9}),
        portfolio=_portfolio(),
    )

    assert emission is None
    assert pipeline.last_diagnostic is not None
    assert pipeline.last_diagnostic.code == "missing_required_factors"


@pytest.mark.asyncio
async def test_stale_snapshot_factor_cannot_be_unstaled_by_untrusted_external_signal() -> None:
    pipeline = ControllerPipeline(
        strategy=_strategy("metaculus_prior"),
        factor_reader=SnapshotReader(
            FactorSnapshot(
                values={("metaculus_prior", ""): 0.5},
                stale_factors=(("metaculus_prior", ""),),
                snapshot_hash="stale-metaculus",
            )
        ),
        forecasters=(ConstantForecaster(0.7),),
        calibrator=NetcalCalibrator(),
        sizer=FixedSizer(),
        router=Router(ControllerSettings(min_volume=0.0)),
        settings=PMSSettings(
            mode=RunMode.LIVE,
            controller=ControllerSettings(min_volume=0.0),
        ),
    )

    emission = await pipeline.on_signal(
        _signal(external_signal={"metaculus_prior": 0.9}),
        portfolio=_portfolio(),
    )

    assert emission is None
    assert pipeline.last_diagnostic is not None
    assert pipeline.last_diagnostic.code == "stale_required_factors"


@pytest.mark.asyncio
async def test_paper_mode_strict_factor_gates_by_default() -> None:
    pipeline = ControllerPipeline(
        strategy=_strategy("metaculus_prior"),
        factor_reader=SnapshotReader(
            FactorSnapshot(
                values={},
                missing_factors=(("metaculus_prior", ""),),
                snapshot_hash="paper-missing",
            )
        ),
        forecasters=(ConstantForecaster(0.7),),
        calibrator=NetcalCalibrator(),
        sizer=FixedSizer(),
        router=Router(ControllerSettings(min_volume=0.0)),
        settings=PMSSettings(
            mode=RunMode.PAPER,
            controller=ControllerSettings(min_volume=0.0),
        ),
    )

    emission = await pipeline.on_signal(
        _signal(external_signal={"resolved_outcome": 1.0}),
        portfolio=_portfolio(),
    )

    assert emission is None
    assert pipeline.last_diagnostic is not None
    assert pipeline.last_diagnostic.code == "missing_required_factors"


@pytest.mark.asyncio
async def test_backtest_can_disable_strict_factor_gates_for_exploration() -> None:
    pipeline = ControllerPipeline(
        strategy=_strategy("metaculus_prior"),
        factor_reader=SnapshotReader(
            FactorSnapshot(
                values={},
                missing_factors=(("metaculus_prior", ""),),
                snapshot_hash="backtest-missing",
            )
        ),
        forecasters=(ConstantForecaster(0.7),),
        calibrator=NetcalCalibrator(),
        sizer=FixedSizer(),
        router=Router(ControllerSettings(min_volume=0.0, strict_factor_gates=False)),
        settings=PMSSettings(
            mode=RunMode.BACKTEST,
            controller=ControllerSettings(min_volume=0.0, strict_factor_gates=False),
        ),
    )

    emission = await pipeline.on_signal(
        _signal(external_signal={"resolved_outcome": 1.0}),
        portfolio=_portfolio(),
    )

    assert emission is not None


@dataclass
class AllowFirstOrderGate:
    previews: list[LiveOrderPreview] = field(default_factory=list)

    async def approve_first_order(self, preview: LiveOrderPreview) -> bool:
        self.previews.append(preview)
        return True

    async def consume(self, preview: LiveOrderPreview) -> None:
        del preview


@dataclass
class AllowQuoteProvider:
    async def quote(
        self,
        order: PolymarketOrderRequest,
        credentials: object,
    ) -> LivePreSubmitQuote:
        del credentials
        return LivePreSubmitQuote(
            market_status="open",
            book_age_ms=10.0,
            executable_notional_usdc=order.notional_usdc,
            best_executable_price=order.price,
            spread_bps=5.0,
            quote_hash="quote-ok",
            book_ts=datetime(2026, 4, 27, tzinfo=UTC),
        )


@dataclass
class RestingNonGtcClient:
    async def submit_order(
        self,
        order: PolymarketOrderRequest,
        credentials: object,
    ) -> PolymarketOrderResult:
        del credentials
        return PolymarketOrderResult(
            order_id="pm-resting-ioc",
            status=OrderStatus.LIVE.value,
            raw_status="live",
            filled_notional_usdc=2.0,
            remaining_notional_usdc=8.0,
            fill_price=order.price,
            filled_quantity=5.0,
        )


@pytest.mark.asyncio
async def test_live_ioc_order_returning_live_with_remaining_triggers_reconciliation_halt() -> None:
    actuator = PolymarketActuator(
        _live_settings(),
        client=RestingNonGtcClient(),
        operator_gate=AllowFirstOrderGate(),
        quote_provider=AllowQuoteProvider(),
    )

    with pytest.raises(PolymarketSubmissionUnknownError) as exc_info:
        await actuator.execute(_decision(time_in_force=TimeInForce.IOC), _portfolio())

    assert "Non-GTC live order appears resting" in str(exc_info.value)
    assert exc_info.value.order_state is not None
    assert exc_info.value.order_state.order_id == "pm-resting-ioc"


@dataclass
class MatchedClient:
    async def submit_order(
        self,
        order: PolymarketOrderRequest,
        credentials: object,
    ) -> PolymarketOrderResult:
        del credentials
        return PolymarketOrderResult(
            order_id="pm-preview-outcome",
            status=OrderStatus.MATCHED.value,
            raw_status="matched",
            filled_notional_usdc=order.notional_usdc,
            remaining_notional_usdc=0.0,
            fill_price=order.price,
            filled_quantity=order.estimated_quantity,
        )


@pytest.mark.asyncio
async def test_first_live_order_preview_and_approval_require_outcome(
    tmp_path: Any,
) -> None:
    approval_path = tmp_path / "approval.json"
    gate = FileFirstLiveOrderGate(approval_path)
    preview = LiveOrderPreview(
        max_notional_usdc=10.0,
        venue="polymarket",
        market_id="m-live-safety",
        token_id="t-no",
        side=Side.BUY.value,
        limit_price=0.4,
        max_slippage_bps=50,
        outcome="NO",
    )
    approval_path.write_text(
        '{"approved": true, "venue": "polymarket", "market_id": "m-live-safety", '
        '"token_id": "t-no", "side": "BUY", "max_notional_usdc": 10.0, '
        '"limit_price": 0.4, "max_slippage_bps": 50}',
        encoding="utf-8",
    )
    assert await gate.approve_first_order(preview) is False

    approval_path.write_text(
        '{"approved": true, "venue": "polymarket", "market_id": "m-live-safety", '
        '"token_id": "t-no", "side": "BUY", "outcome": "NO", '
        '"max_notional_usdc": 10.0, "limit_price": 0.4, "max_slippage_bps": 50}',
        encoding="utf-8",
    )
    assert await gate.approve_first_order(preview) is True

    recording_gate = AllowFirstOrderGate()
    actuator = PolymarketActuator(
        _live_settings(),
        client=MatchedClient(),
        operator_gate=recording_gate,
        quote_provider=AllowQuoteProvider(),
    )
    await actuator.execute(_decision(outcome="NO"), _portfolio())

    assert recording_gate.previews[0].outcome == "NO"

    now = datetime(2026, 4, 27, 12, 0, tzinfo=UTC)
    metadata_gate = AllowFirstOrderGate()
    metadata_store = FakeBookStore(
        market=Market(
            condition_id="m-live-safety",
            slug="live-safety",
            question="Will live safety pass?",
            venue="polymarket",
            resolves_at=now + timedelta(days=1),
            created_at=now - timedelta(days=1),
            last_seen_at=now,
        ),
        snapshot=BookSnapshot(
            id=31,
            market_id="m-live-safety",
            token_id="t-no",
            ts=now - timedelta(milliseconds=10),
            hash="book-preview",
            source="subscribe",
        ),
        levels=[
            BookLevel(
                snapshot_id=31,
                market_id="m-live-safety",
                side=Side.BUY.value,
                price=0.399,
                size=25.0,
            ),
            BookLevel(
                snapshot_id=31,
                market_id="m-live-safety",
                side=Side.SELL.value,
                price=0.40,
                size=25.0,
            ),
        ],
    )
    actuator_with_book = PolymarketActuator(
        _live_settings(),
        client=MatchedClient(),
        operator_gate=metadata_gate,
        quote_provider=PolymarketBookQuoteProvider(
            store=metadata_store,
            clock=lambda: now,
        ),
    )
    await actuator_with_book.execute(_decision(outcome="NO"), _portfolio())

    assert metadata_gate.previews[0].outcome == "NO"
    assert metadata_gate.previews[0].market_slug == "live-safety"
    assert metadata_gate.previews[0].question == "Will live safety pass?"


class _Acquire:
    def __init__(self, connection: "_Connection") -> None:
        self.connection = connection

    async def __aenter__(self) -> "_Connection":
        return self.connection

    async def __aexit__(self, *_: object) -> None:
        return None


class _Transaction:
    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, *_: object) -> None:
        return None


class _Connection:
    def __init__(self) -> None:
        self.fetchval_result: int = 0
        self.fetchrow_result: dict[str, object] | None = None
        self.fetchval_calls: list[tuple[str, tuple[object, ...]]] = []
        self.fetchrow_calls: list[tuple[str, tuple[object, ...]]] = []
        self.execute_calls: list[tuple[str, tuple[object, ...]]] = []

    async def fetchval(self, query: str, *args: object) -> int:
        self.fetchval_calls.append((query, args))
        return self.fetchval_result

    async def fetchrow(self, query: str, *args: object) -> dict[str, object] | None:
        self.fetchrow_calls.append((query, args))
        return self.fetchrow_result

    async def execute(self, query: str, *args: object) -> str:
        self.execute_calls.append((query, args))
        return "UPDATE 1"

    def transaction(self) -> _Transaction:
        return _Transaction()


class _Pool:
    def __init__(self, connection: _Connection) -> None:
        self.connection = connection

    def acquire(self) -> _Acquire:
        return _Acquire(self.connection)


@pytest.mark.asyncio
async def test_live_start_refuses_unresolved_submission_unknown() -> None:
    connection = _Connection()
    connection.fetchval_result = 1
    runner = Runner(config=_live_settings())
    runner._pg_pool = cast(asyncpg.Pool, _Pool(connection))  # noqa: SLF001

    with pytest.raises(RuntimeError, match="unresolved submission_unknown"):
        await runner._assert_no_unresolved_submission_unknown_incidents()  # noqa: SLF001

    query, _ = connection.fetchval_calls[0]
    assert "reconciled_at IS NULL" in query


@pytest.mark.asyncio
async def test_reconciled_submission_unknown_allows_live_restart() -> None:
    connection = _Connection()
    connection.fetchval_result = 0
    runner = Runner(config=_live_settings())
    runner._pg_pool = cast(asyncpg.Pool, _Pool(connection))  # noqa: SLF001

    await runner._assert_no_unresolved_submission_unknown_incidents()  # noqa: SLF001


@pytest.mark.asyncio
async def test_submission_unknown_reconciliation_store_marks_incident_resolved() -> None:
    connection = _Connection()
    connection.fetchrow_result = {"decision_id": "d-unknown"}
    store = SubmissionUnknownReconciliationStore(cast(asyncpg.Pool, _Pool(connection)))

    updated = await store.reconcile_submission_unknown(
        decision_id="d-unknown",
        venue_order_id="pm-123",
        status="filled",
        reconciled_by="operator",
        note="matched venue fill",
    )

    assert updated is True
    query, args = connection.fetchrow_calls[0]
    assert "reconciled_at = now()" in query
    assert "outcome = 'submission_unknown'" in query
    assert args == ("d-unknown", "pm-123", "filled", "operator", "matched venue fill")
    decision_query, decision_args = connection.execute_calls[0]
    assert "UPDATE decisions" in decision_query
    assert "status = 'reconciled'" in decision_query
    assert "status = 'submission_unknown'" in decision_query
    assert decision_args == ("d-unknown",)


@pytest.mark.asyncio
async def test_api_reconciles_submission_unknown_incident() -> None:
    connection = _Connection()
    connection.fetchrow_result = {"decision_id": "d-unknown"}
    runner = Runner(config=PMSSettings(auto_migrate_default_v2=False))
    runner._pg_pool = cast(asyncpg.Pool, _Pool(connection))  # noqa: SLF001
    app = create_app(runner, auto_start=False)
    transport = httpx.ASGITransport(app=app)

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/live/reconcile-submission-unknown",
            json={
                "decision_id": "d-unknown",
                "venue_order_id": "pm-123",
                "status": "filled",
                "reconciled_by": "operator",
                "note": "matched venue fill",
            },
        )

    assert response.status_code == 200
    assert response.json() == {
        "status": "reconciled",
        "decision_id": "d-unknown",
        "resolution": "filled",
    }


def test_pms_live_cli_parses_submission_unknown_reconcile_command() -> None:
    args = build_parser().parse_args(
        [
            "reconcile-submission-unknown",
            "--decision-id",
            "d-unknown",
            "--venue-order-id",
            "pm-123",
            "--status",
            "filled",
            "--reconciled-by",
            "operator",
            "--note",
            "matched venue fill",
        ]
    )

    assert args.command == "reconcile-submission-unknown"
    assert args.decision_id == "d-unknown"
    assert args.venue_order_id == "pm-123"
    assert args.status == "filled"
    assert args.reconciled_by == "operator"
    assert args.note == "matched venue fill"


def test_decision_lifecycle_statuses_cover_execution_and_reconciliation() -> None:
    assert "submitted" in DECISION_STATUSES
    assert "submission_unknown" in DECISION_STATUSES
    assert "reconciled" in DECISION_STATUSES
    validate_decision_status_transition("accepted", "queued")
    validate_decision_status_transition("queued", "submitted")
    validate_decision_status_transition("submitted", "rejected")
    validate_decision_status_transition("submitted", "submission_unknown")
    validate_decision_status_transition("submission_unknown", "reconciled")


class MatchingVenueReconciler:
    async def snapshot(self, credentials: object) -> VenueAccountSnapshot:
        del credentials
        return VenueAccountSnapshot(
            balances={"USDC": 1000.0},
            open_orders=(),
            positions=(),
        )

    async def compare(
        self,
        db_portfolio: Portfolio,
        venue_snapshot: VenueAccountSnapshot,
    ) -> ReconciliationReport:
        del db_portfolio, venue_snapshot
        return ReconciliationReport(ok=True, mismatches=())


class MismatchingVenueReconciler(MatchingVenueReconciler):
    async def compare(
        self,
        db_portfolio: Portfolio,
        venue_snapshot: VenueAccountSnapshot,
    ) -> ReconciliationReport:
        del db_portfolio, venue_snapshot
        return ReconciliationReport(ok=False, mismatches=("position mismatch",))


@dataclass
class FakeVenueAccountClient:
    snapshot_value: VenueAccountSnapshot
    calls: int = 0

    async def read_account_snapshot(
        self,
        credentials: VenueCredentials,
    ) -> VenueAccountSnapshot:
        assert credentials.venue == "polymarket"
        self.calls += 1
        return self.snapshot_value


def _venue_open_order() -> OrderState:
    now = datetime(2026, 4, 27, tzinfo=UTC)
    return OrderState(
        order_id="pm-open-1",
        decision_id="venue-open-pm-open-1",
        status=OrderStatus.LIVE.value,
        market_id="m-live-safety",
        token_id="t-yes",
        venue="polymarket",
        requested_notional_usdc=5.0,
        filled_notional_usdc=0.0,
        remaining_notional_usdc=5.0,
        fill_price=0.40,
        submitted_at=now,
        last_updated_at=now,
        raw_status="open",
        strategy_id="venue",
        strategy_version_id="venue",
    )


def _venue_position(*, shares: float = 25.0) -> Position:
    return Position(
        market_id="m-live-safety",
        token_id="t-yes",
        venue="polymarket",
        side=Side.BUY.value,
        shares_held=shares,
        avg_entry_price=0.4,
        unrealized_pnl=0.0,
        locked_usdc=shares * 0.4,
    )


@pytest.mark.asyncio
async def test_live_venue_account_reconciliation_blocks_mismatch() -> None:
    runner = Runner(
        config=_live_settings(live_account_reconciliation_required=True),
        venue_account_reconciler=MismatchingVenueReconciler(),
    )

    with pytest.raises(RuntimeError, match="venue account reconciliation mismatch"):
        await runner._reconcile_venue_account()  # noqa: SLF001


@pytest.mark.asyncio
async def test_live_venue_account_reconciliation_allows_matching_snapshot() -> None:
    runner = Runner(
        config=_live_settings(live_account_reconciliation_required=True),
        venue_account_reconciler=MatchingVenueReconciler(),
    )

    await runner._reconcile_venue_account()  # noqa: SLF001


@pytest.mark.asyncio
async def test_polymarket_venue_reconciler_reads_snapshot_client_and_blocks_open_orders() -> None:
    client = FakeVenueAccountClient(
        VenueAccountSnapshot(
            balances={"USDC": 1000.0},
            open_orders=(_venue_open_order(),),
            positions=(),
        )
    )
    reconciler = PolymarketVenueAccountReconciler(client=client)

    snapshot = await reconciler.snapshot(_live_settings().polymarket.credentials())
    report = await reconciler.compare(_portfolio(), snapshot)

    assert client.calls == 1
    assert report.ok is False
    assert report.mismatches == (
        "venue has 1 open orders; PMS has no durable live open-order ledger yet",
    )


@pytest.mark.asyncio
async def test_polymarket_venue_reconciler_blocks_when_venue_cash_below_db_free() -> None:
    snapshot = VenueAccountSnapshot(
        balances={"USDC": 999.0},
        open_orders=(),
        positions=(),
    )
    reconciler = PolymarketVenueAccountReconciler()

    report = await reconciler.compare(_portfolio(), snapshot)

    assert report.ok is False
    assert report.mismatches == (
        "venue USDC balance below PMS free cash: venue=999.00000000 DB=1000.00000000",
    )
