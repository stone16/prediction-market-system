from __future__ import annotations

import asyncio
import json
import inspect
import logging
import sys
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Literal, cast

import pytest

from pms.actuator import executor
from pms.actuator.adapters import backtest
from pms.actuator.adapters.backtest import BacktestActuator
from pms.actuator.adapters.paper import PaperActuator
from pms.actuator.adapters.polymarket import (
    FileFirstLiveOrderGate,
    LiveOrderPreview,
    OperatorApprovalRequiredError,
    PolymarketActuator,
    PolymarketOrderResult,
    PolymarketOrderRequest,
    PolymarketSDKClient,
    PolymarketSubmissionUnknownError,
)
from pms.actuator.feedback import ActuatorFeedback
from pms.actuator.risk import InsufficientLiquidityError, RiskManager
from pms.config import PMSSettings, PolymarketSettings, RiskSettings
from pms.core.enums import FeedbackSource, FeedbackTarget, OrderStatus, Side, TimeInForce
from pms.core.models import LiveTradingDisabledError, OrderState, Portfolio, TradeDecision
from pms.storage.dedup_store import InMemoryDedupStore
from pms.storage.feedback_store import FeedbackStore
from tests.support.fake_stores import InMemoryFeedbackStore


def _decision(
    *,
    decision_id: str = "d-cp06",
    market_id: str = "m-cp06",
    token_id: str | None = None,
    side: Literal["BUY", "SELL"] = Side.BUY.value,
    notional_usdc: float = 10.0,
    limit_price: float = 0.4,
    action: Literal["BUY", "SELL"] | None = None,
    outcome: Literal["YES", "NO"] = "YES",
) -> TradeDecision:
    return TradeDecision(
        decision_id=decision_id,
        market_id=market_id,
        token_id=token_id or ("t-yes" if outcome == "YES" else "t-no"),
        venue="polymarket",
        side=side,
        notional_usdc=notional_usdc,
        order_type="limit",
        max_slippage_bps=50,
        stop_conditions=["unit-test"],
        prob_estimate=0.6,
        expected_edge=0.2,
        time_in_force=TimeInForce.GTC,
        opportunity_id=f"op-{decision_id}",
        strategy_id="default",
        strategy_version_id="default-v1",
        action=action or side,
        limit_price=limit_price,
        outcome=outcome,
    )


def _portfolio(
    *,
    locked_usdc: float = 0.0,
    max_drawdown_pct: float | None = None,
) -> Portfolio:
    return Portfolio(
        total_usdc=1000.0,
        free_usdc=1000.0 - locked_usdc,
        locked_usdc=locked_usdc,
        open_positions=[],
        max_drawdown_pct=max_drawdown_pct,
    )


def _order_state(
    decision: TradeDecision,
    *,
    status: str = OrderStatus.INVALID.value,
    raw_status: str = "rejected",
    fill_price: float | None = None,
    filled_notional_usdc: float = 0.0,
) -> OrderState:
    now = datetime(2026, 4, 14, tzinfo=UTC)
    filled_quantity = 0.0
    if fill_price is not None and fill_price != 0.0:
        filled_quantity = filled_notional_usdc / fill_price
    return OrderState(
        order_id=f"order-{decision.decision_id}",
        decision_id=decision.decision_id,
        status=status,
        market_id=decision.market_id,
        token_id=decision.token_id,
        venue=decision.venue,
        requested_notional_usdc=decision.notional_usdc,
        filled_notional_usdc=filled_notional_usdc,
        remaining_notional_usdc=decision.notional_usdc - filled_notional_usdc,
        fill_price=fill_price,
        submitted_at=now,
        last_updated_at=now,
        raw_status=raw_status,
        strategy_id=decision.strategy_id,
        strategy_version_id=decision.strategy_version_id,
        filled_quantity=filled_quantity,
    )


@dataclass
class RecordingDedupStore:
    acquire_allowed: bool = True
    release_error: Exception | None = None
    acquire_calls: list[str] = field(default_factory=list)
    release_calls: list[tuple[str, str]] = field(default_factory=list)

    async def acquire(self, decision: TradeDecision) -> bool:
        self.acquire_calls.append(decision.decision_id)
        return self.acquire_allowed

    async def release(self, decision_id: str, outcome: str) -> None:
        self.release_calls.append((decision_id, outcome))
        if self.release_error is not None:
            raise self.release_error

    async def retention_scan(self, older_than: timedelta) -> int:
        del older_than
        return 0


@dataclass
class StaticAdapter:
    state: OrderState
    calls: int = 0

    async def execute(
        self,
        decision: TradeDecision,
        portfolio: Portfolio | None = None,
    ) -> OrderState:
        del decision, portfolio
        self.calls += 1
        return self.state


@dataclass
class FailingAdapter:
    error: RuntimeError
    calls: int = 0

    async def execute(
        self,
        decision: TradeDecision,
        portfolio: Portfolio | None = None,
    ) -> OrderState:
        del decision, portfolio
        self.calls += 1
        raise self.error


def test_backtest_adapter_documents_license_decision_before_internal_replay() -> None:
    source = inspect.getsource(backtest)

    assert "prediction-market-backtesting" in source
    assert "LGPL-3.0-or-later" in source
    assert "internal replay" in source


def test_risk_manager_position_breakpoint_exact_limit_and_plus_one() -> None:
    manager = RiskManager(
        RiskSettings(max_position_per_market=100.0, max_total_exposure=1000.0)
    )

    assert manager.check(_decision(notional_usdc=100.0), _portfolio()).approved is True
    rejected = manager.check(_decision(notional_usdc=101.0), _portfolio())

    assert rejected.approved is False
    assert rejected.reason == "max_position_per_market"


def test_risk_manager_rejects_total_exposure_and_drawdown() -> None:
    manager = RiskManager(
        RiskSettings(
            max_position_per_market=1000.0,
            max_total_exposure=1000.0,
            max_drawdown_pct=0.2,
        )
    )

    assert (
        manager.check(
            _decision(notional_usdc=501.0),
            _portfolio(locked_usdc=500.0),
        ).reason
        == "max_total_exposure"
    )
    assert (
        manager.check(_decision(notional_usdc=10.0), _portfolio(max_drawdown_pct=0.21)).reason
        == "drawdown_circuit_breaker"
    )


@pytest.mark.asyncio
async def test_paper_actuator_fills_buy_at_best_ask() -> None:
    actuator = PaperActuator(
        orderbooks={
            "m-cp06": {
                "bids": [{"price": 0.39, "size": 100.0}],
                "asks": [{"price": 0.41, "size": 100.0}],
            }
        }
    )

    state = await actuator.execute(_decision(notional_usdc=10.0), _portfolio())

    assert state.status == OrderStatus.MATCHED.value
    assert state.fill_price == 0.41
    assert state.filled_notional_usdc == pytest.approx(10.0)
    assert state.remaining_notional_usdc == 0.0


@pytest.mark.asyncio
async def test_paper_actuator_derives_no_fill_price_from_yes_bid() -> None:
    actuator = PaperActuator(
        orderbooks={
            "m-cp06": {
                "bids": [{"price": 0.62, "size": 100.0}],
                "asks": [{"price": 0.64, "size": 100.0}],
            }
        }
    )

    state = await actuator.execute(
        _decision(
            decision_id="d-no-cp06",
            notional_usdc=10.0,
            limit_price=0.38,
            outcome="NO",
        ),
        _portfolio(),
    )

    assert state.status == OrderStatus.MATCHED.value
    assert state.fill_price == pytest.approx(0.38)


@pytest.mark.asyncio
async def test_paper_actuator_empty_orderbook_raises_insufficient_liquidity() -> None:
    actuator = PaperActuator(orderbooks={"m-cp06": {"bids": [], "asks": []}})

    with pytest.raises(InsufficientLiquidityError):
        await actuator.execute(_decision(), _portfolio())


@pytest.mark.asyncio
async def test_backtest_actuator_replays_fill_from_fixture() -> None:
    fixture_path = Path("tests/fixtures/polymarket_7day_synthetic.jsonl")
    actuator = BacktestActuator(fixture_path)

    state = await actuator.execute(
        _decision(
            market_id="pm-synthetic-000",
            token_id="yes-token-000",
            notional_usdc=10.0,
            limit_price=0.31,
        ),
        _portfolio(),
    )

    assert state.status == OrderStatus.MATCHED.value
    assert state.fill_price == 0.31


@pytest.mark.asyncio
async def test_polymarket_actuator_raises_when_live_trading_disabled() -> None:
    actuator = PolymarketActuator(PMSSettings(live_trading_enabled=False))

    with pytest.raises(LiveTradingDisabledError):
        await actuator.execute(_decision(), _portfolio())


@dataclass
class RecordingPolymarketClient:
    submitted: list[PolymarketOrderRequest] = field(default_factory=list)

    async def submit_order(
        self,
        order: PolymarketOrderRequest,
        credentials: object,
    ) -> PolymarketOrderResult:
        del credentials
        self.submitted.append(order)
        return PolymarketOrderResult(
            order_id="pm-live-order-1",
            status=OrderStatus.MATCHED.value,
            raw_status="matched",
            filled_notional_usdc=10.0,
            remaining_notional_usdc=0.0,
            fill_price=0.4,
            filled_quantity=25.0,
        )


@dataclass
class RecordingOperatorGate:
    approved: bool
    previews: list[LiveOrderPreview] = field(default_factory=list)
    consumed: list[LiveOrderPreview] = field(default_factory=list)

    async def approve_first_order(self, preview: LiveOrderPreview) -> bool:
        self.previews.append(preview)
        return self.approved

    async def consume(self, preview: LiveOrderPreview) -> None:
        self.consumed.append(preview)


@dataclass
class BlockingOperatorGate:
    previews: list[LiveOrderPreview] = field(default_factory=list)
    entered: asyncio.Event = field(default_factory=asyncio.Event)
    release: asyncio.Event = field(default_factory=asyncio.Event)

    async def approve_first_order(self, preview: LiveOrderPreview) -> bool:
        self.previews.append(preview)
        self.entered.set()
        await self.release.wait()
        return True

    async def consume(self, preview: LiveOrderPreview) -> None:
        del preview


def _live_settings() -> PMSSettings:
    return PMSSettings(
        live_trading_enabled=True,
        polymarket=PolymarketSettings(
            private_key="private-key",
            api_key="api-key",
            api_secret="api-secret",
            api_passphrase="passphrase",
            signature_type=1,
            funder_address="0xabc",
        ),
    )


@pytest.mark.asyncio
async def test_polymarket_actuator_rejects_missing_live_credentials() -> None:
    actuator = PolymarketActuator(PMSSettings(live_trading_enabled=True))

    with pytest.raises(LiveTradingDisabledError, match="Missing Polymarket credential fields"):
        await actuator.execute(_decision(), _portfolio())


@pytest.mark.asyncio
async def test_polymarket_actuator_requires_operator_approval_for_first_live_order() -> None:
    client = RecordingPolymarketClient()
    gate = RecordingOperatorGate(approved=False)
    actuator = PolymarketActuator(_live_settings(), client=client, operator_gate=gate)

    with pytest.raises(OperatorApprovalRequiredError):
        await actuator.execute(_decision(), _portfolio())

    assert client.submitted == []
    assert len(gate.previews) == 1
    assert gate.previews[0].max_notional_usdc == 10.0
    assert gate.previews[0].venue == "polymarket"
    assert gate.previews[0].market_id == "m-cp06"
    assert gate.previews[0].token_id == "t-yes"
    assert gate.previews[0].side == Side.BUY.value
    assert gate.previews[0].limit_price == 0.4
    assert gate.previews[0].max_slippage_bps == 50


@pytest.mark.asyncio
async def test_polymarket_actuator_submits_mocked_live_order_after_first_order_gate() -> None:
    client = RecordingPolymarketClient()
    gate = RecordingOperatorGate(approved=True)
    actuator = PolymarketActuator(_live_settings(), client=client, operator_gate=gate)

    state = await actuator.execute(_decision(), _portfolio())

    assert state.order_id == "pm-live-order-1"
    assert state.status == OrderStatus.MATCHED.value
    assert state.filled_notional_usdc == 10.0
    assert state.remaining_notional_usdc == 0.0
    assert state.fill_price == 0.4
    assert state.filled_quantity == 25.0
    assert state.strategy_id == "default"
    assert state.strategy_version_id == "default-v1"
    assert len(client.submitted) == 1
    assert len(gate.previews) == 1
    # After a successful first submit, the gate's approval artefact must be
    # consumed so it cannot be replayed by a future restart or concurrent
    # task. Verifies the consume-on-success contract.
    assert len(gate.consumed) == 1
    assert gate.consumed[0] == gate.previews[0]


@pytest.mark.asyncio
async def test_polymarket_actuator_first_order_gate_is_serialized() -> None:
    # The lock around the gate must serialize gate calls — a second concurrent
    # task cannot observe an in-progress gate. After the live-hardening fix
    # the first-order flag is *not* set until after a successful submit, so
    # the second task may legitimately call the gate again once T1 exits the
    # critical section. What MUST hold:
    #   1. While T1 is blocked in the gate, T2 cannot have called it.
    #   2. Both tasks eventually submit (one operator approval is the floor,
    #      not the ceiling — this matches the original concurrent semantics).
    client = RecordingPolymarketClient()
    gate = BlockingOperatorGate()
    actuator = PolymarketActuator(_live_settings(), client=client, operator_gate=gate)

    first = asyncio.create_task(actuator.execute(_decision(decision_id="d-first"), _portfolio()))
    await asyncio.wait_for(gate.entered.wait(), timeout=1.0)
    second = asyncio.create_task(actuator.execute(_decision(decision_id="d-second"), _portfolio()))
    await asyncio.sleep(0)
    assert len(gate.previews) == 1

    gate.release.set()
    await asyncio.gather(first, second)

    # Lock invariant preserved (gate not entered concurrently). Whether T2's
    # gate call is observed is race-dependent — what matters is that T1 was
    # alone in the critical section while blocking.
    assert 1 <= len(gate.previews) <= 2
    assert len(client.submitted) == 2


@pytest.mark.asyncio
async def test_polymarket_actuator_converts_limit_notional_to_order_shares() -> None:
    client = RecordingPolymarketClient()
    gate = RecordingOperatorGate(approved=True)
    actuator = PolymarketActuator(_live_settings(), client=client, operator_gate=gate)

    await actuator.execute(_decision(notional_usdc=10.0, limit_price=0.4), _portfolio())

    assert client.submitted[0].size == pytest.approx(25.0)
    assert client.submitted[0].notional_usdc == pytest.approx(10.0)
    assert client.submitted[0].price == pytest.approx(0.4)


@pytest.mark.asyncio
async def test_polymarket_actuator_converts_market_sell_notional_to_shares() -> None:
    client = RecordingPolymarketClient()
    gate = RecordingOperatorGate(approved=True)
    actuator = PolymarketActuator(_live_settings(), client=client, operator_gate=gate)
    decision = _decision(
        side=Side.SELL.value,
        action=Side.SELL.value,
        notional_usdc=12.0,
        limit_price=0.3,
    )
    object.__setattr__(decision, "order_type", "market")

    await actuator.execute(decision, _portfolio())

    assert client.submitted[0].size == pytest.approx(40.0)
    assert client.submitted[0].notional_usdc == pytest.approx(12.0)
    assert client.submitted[0].order_type == "market"


@pytest.mark.asyncio
async def test_file_first_live_order_gate_requires_exact_preview(tmp_path: Path) -> None:
    path = tmp_path / "approval.json"
    gate = FileFirstLiveOrderGate(path)
    preview = LiveOrderPreview(
        max_notional_usdc=10.0,
        venue="polymarket",
        market_id="m-cp06",
        token_id="t-yes",
        side=Side.BUY.value,
        limit_price=0.4,
        max_slippage_bps=50,
    )
    path.write_text(
        json.dumps(
            {
                "approved": True,
                "max_notional_usdc": 10.0,
                "venue": "polymarket",
                "market_id": "m-cp06",
                "token_id": "t-yes",
                "side": Side.BUY.value,
                "limit_price": 0.4,
                "max_slippage_bps": 50,
            }
        ),
        encoding="utf-8",
    )

    assert await gate.approve_first_order(preview) is True

    path.write_text(
        json.dumps(
            {
                "approved": True,
                "max_notional_usdc": 11.0,
                "venue": "polymarket",
                "market_id": "m-cp06",
                "token_id": "t-yes",
                "side": Side.BUY.value,
                "limit_price": 0.4,
                "max_slippage_bps": 50,
            }
        ),
        encoding="utf-8",
    )

    assert await gate.approve_first_order(preview) is False


@pytest.mark.asyncio
async def test_polymarket_sdk_client_posts_limit_order_through_v2_sdk(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: dict[str, object] = {}

    @dataclass
    class FakeApiCreds:
        api_key: str
        api_secret: str
        api_passphrase: str

    @dataclass
    class FakeOrderArgs:
        token_id: str
        price: float
        side: object
        size: float

    @dataclass
    class FakeMarketOrderArgs:
        token_id: str
        amount: float
        side: object
        price: float
        order_type: object

    @dataclass
    class FakePartialCreateOrderOptions:
        tick_size: object | None = None
        neg_risk: object | None = None

    class FakeOrderType:
        GTC = "GTC"
        FOK = "FOK"
        FAK = "FAK"
        GTD = "GTD"

    class FakeSide:
        BUY = "SDK_BUY"
        SELL = "SDK_SELL"

    class FakeClobClient:
        def __init__(self, **kwargs: object) -> None:
            calls["init"] = kwargs

        def create_and_post_order(
            self,
            *,
            order_args: FakeOrderArgs,
            options: FakePartialCreateOrderOptions,
            order_type: object,
        ) -> dict[str, object]:
            calls["limit_order"] = order_args
            calls["limit_options"] = options
            calls["limit_order_type"] = order_type
            return {"orderID": "sdk-order-1", "status": "matched", "errorMsg": ""}

    fake_module = SimpleNamespace(
        ApiCreds=FakeApiCreds,
        ClobClient=FakeClobClient,
        OrderArgs=FakeOrderArgs,
        MarketOrderArgs=FakeMarketOrderArgs,
        OrderType=FakeOrderType,
        PartialCreateOrderOptions=FakePartialCreateOrderOptions,
        Side=FakeSide,
    )
    monkeypatch.setitem(sys.modules, "py_clob_client_v2", fake_module)

    request = PolymarketOrderRequest(
        market_id="m-cp06",
        token_id="t-yes",
        side=Side.BUY.value,
        price=0.4,
        size=25.0,
        notional_usdc=10.0,
        estimated_quantity=25.0,
        order_type="limit",
        time_in_force=TimeInForce.GTC.value,
        max_slippage_bps=50,
    )
    result = await PolymarketSDKClient().submit_order(
        request,
        _live_settings().polymarket.credentials(),
    )

    init = cast(dict[str, object], calls["init"])
    creds = cast(FakeApiCreds, init["creds"])
    order_args = cast(FakeOrderArgs, calls["limit_order"])
    assert init["host"] == "https://clob.polymarket.com"
    assert init["chain_id"] == 137
    assert init["key"] == "private-key"
    assert init["signature_type"] == 1
    assert init["funder"] == "0xabc"
    assert creds.api_key == "api-key"
    assert creds.api_secret == "api-secret"
    assert creds.api_passphrase == "passphrase"
    assert order_args.token_id == "t-yes"
    assert order_args.price == 0.4
    assert order_args.side == "SDK_BUY"
    assert order_args.size == 25.0
    assert calls["limit_order_type"] == "GTC"
    assert result.order_id == "sdk-order-1"
    assert result.status == OrderStatus.MATCHED.value
    assert result.filled_notional_usdc == pytest.approx(10.0)
    assert result.filled_quantity == pytest.approx(25.0)


@pytest.mark.asyncio
async def test_polymarket_sdk_client_posts_market_order_with_consistent_order_type(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: dict[str, object] = {}

    @dataclass
    class FakeApiCreds:
        api_key: str
        api_secret: str
        api_passphrase: str

    @dataclass
    class FakeOrderArgs:
        token_id: str
        price: float
        side: object
        size: float

    @dataclass
    class FakeMarketOrderArgs:
        token_id: str
        amount: float
        side: object
        price: float
        order_type: object

    class FakePartialCreateOrderOptions:
        pass

    class FakeOrderType:
        GTC = "GTC"
        FOK = "FOK"
        FAK = "FAK"
        GTD = "GTD"

    class FakeSide:
        BUY = "SDK_BUY"
        SELL = "SDK_SELL"

    class FakeClobClient:
        def __init__(self, **kwargs: object) -> None:
            del kwargs

        def create_and_post_market_order(
            self,
            *,
            order_args: FakeMarketOrderArgs,
            options: FakePartialCreateOrderOptions,
            order_type: object,
        ) -> dict[str, object]:
            del options
            calls["market_order"] = order_args
            calls["market_order_type"] = order_type
            return {"orderID": "sdk-market-order-1", "status": "live", "errorMsg": ""}

    fake_module = SimpleNamespace(
        ApiCreds=FakeApiCreds,
        ClobClient=FakeClobClient,
        OrderArgs=FakeOrderArgs,
        MarketOrderArgs=FakeMarketOrderArgs,
        OrderType=FakeOrderType,
        PartialCreateOrderOptions=FakePartialCreateOrderOptions,
        Side=FakeSide,
    )
    monkeypatch.setitem(sys.modules, "py_clob_client_v2", fake_module)

    await PolymarketSDKClient().submit_order(
        PolymarketOrderRequest(
            market_id="m-cp06",
            token_id="t-yes",
            side=Side.BUY.value,
            price=0.4,
            size=10.0,
            notional_usdc=10.0,
            estimated_quantity=25.0,
            order_type="market",
            time_in_force=TimeInForce.IOC.value,
            max_slippage_bps=50,
        ),
        _live_settings().polymarket.credentials(),
    )

    order_args = cast(FakeMarketOrderArgs, calls["market_order"])
    assert order_args.order_type == "FAK"
    assert calls["market_order_type"] == "FAK"


@pytest.mark.asyncio
async def test_polymarket_sdk_client_redacts_secrets_from_sdk_errors(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    class FakeApiCreds:
        def __init__(
            self,
            *,
            api_key: str,
            api_secret: str,
            api_passphrase: str,
        ) -> None:
            del api_key, api_secret, api_passphrase

    class FakeOrderArgs:
        def __init__(self, **kwargs: object) -> None:
            del kwargs

    class FakePartialCreateOrderOptions:
        pass

    class FakeOrderType:
        GTC = "GTC"

    class FakeSide:
        BUY = "SDK_BUY"

    class FakeClobClient:
        def __init__(self, **kwargs: object) -> None:
            del kwargs

        def create_and_post_order(self, **kwargs: object) -> object:
            del kwargs
            raise RuntimeError("venue rejected private-key api-secret passphrase")

    fake_module = SimpleNamespace(
        ApiCreds=FakeApiCreds,
        ClobClient=FakeClobClient,
        OrderArgs=FakeOrderArgs,
        MarketOrderArgs=FakeOrderArgs,
        OrderType=FakeOrderType,
        PartialCreateOrderOptions=FakePartialCreateOrderOptions,
        Side=FakeSide,
    )
    monkeypatch.setitem(sys.modules, "py_clob_client_v2", fake_module)
    caplog.set_level(logging.WARNING, logger="pms.actuator.adapters.polymarket")

    with pytest.raises(LiveTradingDisabledError) as exc_info:
        await PolymarketSDKClient().submit_order(
            PolymarketOrderRequest(
                market_id="m-cp06",
                token_id="t-yes",
                side=Side.BUY.value,
                price=0.4,
                size=25.0,
                notional_usdc=10.0,
                estimated_quantity=25.0,
                order_type="limit",
                time_in_force=TimeInForce.GTC.value,
                max_slippage_bps=50,
            ),
            _live_settings().polymarket.credentials(),
        )

    message = str(exc_info.value)
    assert "private-key" not in message
    assert "api-secret" not in message
    assert "passphrase" not in message
    assert "Polymarket live order submission failed" in message
    rendered_logs = "\n".join(record.getMessage() for record in caplog.records)
    assert "venue rejected" in rendered_logs
    assert "private-key" not in rendered_logs
    assert "api-secret" not in rendered_logs
    assert "passphrase" not in rendered_logs


@pytest.mark.asyncio
async def test_actuator_feedback_appends_controller_feedback() -> None:
    store = cast(FeedbackStore, InMemoryFeedbackStore())
    generator = ActuatorFeedback(store)
    decision = _decision()

    feedback = await generator.generate(
        _order_state(decision, raw_status="insufficient_liquidity"),
        reason="insufficient_liquidity",
    )

    assert feedback.source == FeedbackSource.ACTUATOR.value
    assert feedback.target == FeedbackTarget.CONTROLLER.value
    assert feedback.category == "insufficient_liquidity"
    assert await cast(InMemoryFeedbackStore, store).all() == [feedback]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status", "raw_status", "expected_outcome"),
    [
        (OrderStatus.MATCHED.value, "matched", "matched"),
        ("partial", "matched", "matched"),
        ("cancelled", "ttl", "cancelled_ttl"),
        ("canceled", "limit_invalidated", "cancelled_limit_invalidated"),
        ("cancelled", "session_end", "cancelled_session_end"),
        (
            OrderStatus.CANCELED_MARKET_RESOLVED.value,
            "market_resolved_before_execution",
            "cancelled_market_resolved",
        ),
    ],
)
async def test_executor_releases_mapped_outcome_from_returned_order_state(
    status: str,
    raw_status: str,
    expected_outcome: str,
) -> None:
    store = cast(FeedbackStore, InMemoryFeedbackStore())
    decision = _decision(decision_id=f"d-{status}-{raw_status}".replace("_", "-"))
    dedup_store = RecordingDedupStore()
    returned_state = _order_state(
        decision,
        status=status,
        raw_status=raw_status,
        fill_price=decision.limit_price,
        filled_notional_usdc=decision.notional_usdc,
    )
    actuator = executor.ActuatorExecutor(
        adapter=StaticAdapter(returned_state),
        risk=RiskManager(
            RiskSettings(max_position_per_market=100.0, max_total_exposure=1000.0)
        ),
        feedback=ActuatorFeedback(store),
        dedup_store=dedup_store,
    )

    state = await actuator.execute(decision, _portfolio())

    assert state == returned_state
    assert dedup_store.acquire_calls == [decision.decision_id]
    assert dedup_store.release_calls == [(decision.decision_id, expected_outcome)]


@pytest.mark.asyncio
async def test_executor_releases_invalid_for_risk_rejection() -> None:
    store = cast(FeedbackStore, InMemoryFeedbackStore())
    dedup_store = RecordingDedupStore()
    actuator = executor.ActuatorExecutor(
        adapter=StaticAdapter(_order_state(_decision(), status=OrderStatus.MATCHED.value)),
        risk=RiskManager(
            RiskSettings(max_position_per_market=5.0, max_total_exposure=1000.0)
        ),
        feedback=ActuatorFeedback(store),
        dedup_store=dedup_store,
    )

    state = await actuator.execute(_decision(), _portfolio())

    assert state.status == OrderStatus.INVALID.value
    assert state.raw_status == "max_position_per_market"
    assert dedup_store.release_calls == [("d-cp06", "invalid")]
    assert (await cast(InMemoryFeedbackStore, store).all())[-1].category == (
        "max_position_per_market"
    )


@pytest.mark.asyncio
async def test_executor_releases_rejected_for_insufficient_liquidity() -> None:
    store = cast(FeedbackStore, InMemoryFeedbackStore())
    dedup_store = RecordingDedupStore()
    actuator = executor.ActuatorExecutor(
        adapter=PaperActuator(orderbooks={"m-cp06": {"bids": [], "asks": []}}),
        risk=RiskManager(
            RiskSettings(max_position_per_market=100.0, max_total_exposure=1000.0)
        ),
        feedback=ActuatorFeedback(store),
        dedup_store=dedup_store,
    )

    state = await actuator.execute(_decision(decision_id="d-fail"), _portfolio())

    assert state.status == OrderStatus.INVALID.value
    assert state.raw_status == "insufficient_liquidity"
    assert dedup_store.release_calls == [("d-fail", "rejected")]
    assert (await cast(InMemoryFeedbackStore, store).all())[-1].category == (
        "insufficient_liquidity"
    )


@pytest.mark.asyncio
async def test_executor_release_failure_logs_without_masking_success(
    caplog: pytest.LogCaptureFixture,
) -> None:
    store = cast(FeedbackStore, InMemoryFeedbackStore())
    decision = _decision(decision_id="d-release-success")
    dedup_store = RecordingDedupStore(release_error=RuntimeError("release broke"))
    returned_state = _order_state(
        decision,
        status=OrderStatus.MATCHED.value,
        raw_status="matched",
        fill_price=decision.limit_price,
        filled_notional_usdc=decision.notional_usdc,
    )
    actuator = executor.ActuatorExecutor(
        adapter=StaticAdapter(returned_state),
        risk=RiskManager(
            RiskSettings(max_position_per_market=100.0, max_total_exposure=1000.0)
        ),
        feedback=ActuatorFeedback(store),
        dedup_store=dedup_store,
    )

    with caplog.at_level(logging.ERROR):
        state = await actuator.execute(decision, _portfolio())

    assert state == returned_state
    assert dedup_store.release_calls == [(decision.decision_id, "matched")]
    assert "Failed to release dedup state" in caplog.text


@pytest.mark.asyncio
async def test_executor_release_failure_logs_without_masking_original_adapter_error(
    caplog: pytest.LogCaptureFixture,
) -> None:
    store = cast(FeedbackStore, InMemoryFeedbackStore())
    decision = _decision(decision_id="d-release-error")
    dedup_store = RecordingDedupStore(release_error=RuntimeError("release broke"))
    actuator = executor.ActuatorExecutor(
        adapter=FailingAdapter(RuntimeError("venue rejected order")),
        risk=RiskManager(
            RiskSettings(max_position_per_market=100.0, max_total_exposure=1000.0)
        ),
        feedback=ActuatorFeedback(store),
        dedup_store=dedup_store,
    )

    with caplog.at_level(logging.ERROR), pytest.raises(
        RuntimeError,
        match="venue rejected order",
    ):
        await actuator.execute(decision, _portfolio())

    assert dedup_store.release_calls == [(decision.decision_id, "venue_rejection")]
    assert "Failed to release dedup state" in caplog.text


@pytest.mark.asyncio
async def test_executor_soft_release_keeps_decision_blocked_until_retention_scan() -> None:
    store = cast(FeedbackStore, InMemoryFeedbackStore())
    dedup_store = InMemoryDedupStore()
    actuator = executor.ActuatorExecutor(
        adapter=PaperActuator(orderbooks={"m-cp06": {"bids": [], "asks": []}}),
        risk=RiskManager(
            RiskSettings(max_position_per_market=100.0, max_total_exposure=1000.0)
        ),
        feedback=ActuatorFeedback(store),
        dedup_store=dedup_store,
    )

    first = await actuator.execute(_decision(decision_id="d-soft-release"), _portfolio())
    second = await actuator.execute(_decision(decision_id="d-soft-release"), _portfolio())

    assert first.raw_status == "insufficient_liquidity"
    assert second.raw_status == "duplicate_decision"
    assert dedup_store.contains("d-soft-release") is True


# --- live-readiness hardening tests (added by fix/live-readiness-hardening) ---


@dataclass
class _FailThenSucceedClient:
    """Polymarket client that raises on the first call and succeeds after."""

    fail_count: int = 1
    submitted: list[PolymarketOrderRequest] = field(default_factory=list)
    error: Exception = field(
        default_factory=lambda: RuntimeError("simulated network drop")
    )

    async def submit_order(
        self,
        order: PolymarketOrderRequest,
        credentials: object,
    ) -> PolymarketOrderResult:
        del credentials
        if self.fail_count > 0:
            self.fail_count -= 1
            raise self.error
        self.submitted.append(order)
        return PolymarketOrderResult(
            order_id="pm-live-recovered",
            status=OrderStatus.MATCHED.value,
            raw_status="matched",
            filled_notional_usdc=order.notional_usdc,
            remaining_notional_usdc=0.0,
            fill_price=order.price,
            filled_quantity=order.estimated_quantity,
        )


@pytest.mark.asyncio
async def test_polymarket_actuator_does_not_mark_approved_when_submit_fails() -> None:
    """SECURITY: a failed first submit must not permanently bypass the gate.

    Pre-fix: `_approval_state.approved = True` was set inside the gate flow,
    so a network failure on the first venue call left the gate permanently
    open — every subsequent decision skipped operator approval.
    """
    client = _FailThenSucceedClient(fail_count=1)
    gate = RecordingOperatorGate(approved=True)
    actuator = PolymarketActuator(_live_settings(), client=client, operator_gate=gate)

    with pytest.raises(RuntimeError, match="simulated network drop"):
        await actuator.execute(_decision(decision_id="d-fail"), _portfolio())

    # Gate was consulted, but submit failed — the floodgate must remain shut.
    assert len(gate.previews) == 1
    assert actuator._first_order_approved() is False
    # And no consume happened, since no submit succeeded.
    assert gate.consumed == []

    # Next attempt MUST go through the gate again.
    state = await actuator.execute(_decision(decision_id="d-retry"), _portfolio())
    assert state.status == OrderStatus.MATCHED.value
    assert len(gate.previews) == 2
    assert actuator._first_order_approved() is True
    # Now the gate has been consumed.
    assert len(gate.consumed) == 1


@pytest.mark.asyncio
async def test_file_first_live_order_gate_consumes_file_after_first_success(
    tmp_path: Path,
) -> None:
    """The approval JSON file must be removed after a successful first submit
    so an attacker / stale file cannot replay it."""
    approval_path = tmp_path / "approval.json"
    preview_payload = {
        "approved": True,
        "max_notional_usdc": 10.0,
        "venue": "polymarket",
        "market_id": "m-cp06",
        "token_id": "t-yes",
        "side": Side.BUY.value,
        "limit_price": 0.4,
        "max_slippage_bps": 50,
    }
    approval_path.write_text(json.dumps(preview_payload), encoding="utf-8")

    client = RecordingPolymarketClient()
    actuator = PolymarketActuator(
        _live_settings(),
        client=client,
        operator_gate=FileFirstLiveOrderGate(approval_path),
    )

    state = await actuator.execute(_decision(), _portfolio())

    assert state.status == OrderStatus.MATCHED.value
    assert approval_path.exists() is False, "approval file must be unlinked after use"


@pytest.mark.asyncio
async def test_file_first_live_order_gate_uses_isclose_for_floats(
    tmp_path: Path,
) -> None:
    """Approval JSON copied from preview values may pick up float
    representation drift (e.g. 0.1+0.2). The gate must accept tiny drift,
    while still rejecting any meaningful difference."""
    path = tmp_path / "approval.json"
    gate = FileFirstLiveOrderGate(path)
    preview = LiveOrderPreview(
        max_notional_usdc=0.1 + 0.2,  # 0.30000000000000004
        venue="polymarket",
        market_id="m-cp06",
        token_id="t-yes",
        side=Side.BUY.value,
        limit_price=0.4,
        max_slippage_bps=50,
    )
    # Operator wrote the rounded value 0.3, the actuator computed 0.1 + 0.2.
    path.write_text(
        json.dumps(
            {
                "approved": True,
                "max_notional_usdc": 0.3,
                "venue": "polymarket",
                "market_id": "m-cp06",
                "token_id": "t-yes",
                "side": Side.BUY.value,
                "limit_price": 0.4,
                "max_slippage_bps": 50,
            }
        ),
        encoding="utf-8",
    )
    assert await gate.approve_first_order(preview) is True

    # But a meaningful difference is still rejected.
    path.write_text(
        json.dumps(
            {
                "approved": True,
                "max_notional_usdc": 0.31,
                "venue": "polymarket",
                "market_id": "m-cp06",
                "token_id": "t-yes",
                "side": Side.BUY.value,
                "limit_price": 0.4,
                "max_slippage_bps": 50,
            }
        ),
        encoding="utf-8",
    )
    assert await gate.approve_first_order(preview) is False


@pytest.mark.asyncio
async def test_polymarket_sdk_client_propagates_timeout_as_unknown_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A timeout in the SDK call means the order MAY have reached the venue.
    Wrapping it as `LiveTradingDisabledError` ('disabled, nothing sent') is
    misleading. Surface as `PolymarketSubmissionUnknownError` so operators
    know to reconcile."""

    class FakeApiCreds:
        def __init__(self, **kwargs: object) -> None:
            del kwargs

    class FakeOrderArgs:
        def __init__(self, **kwargs: object) -> None:
            del kwargs

    class FakePartialCreateOrderOptions:
        pass

    class FakeOrderType:
        GTC = "GTC"

    class FakeSide:
        BUY = "SDK_BUY"

    class FakeClobClient:
        def __init__(self, **kwargs: object) -> None:
            del kwargs

        def create_and_post_order(self, **kwargs: object) -> object:
            del kwargs
            raise asyncio.TimeoutError()

    fake_module = SimpleNamespace(
        ApiCreds=FakeApiCreds,
        ClobClient=FakeClobClient,
        OrderArgs=FakeOrderArgs,
        MarketOrderArgs=FakeOrderArgs,
        OrderType=FakeOrderType,
        PartialCreateOrderOptions=FakePartialCreateOrderOptions,
        Side=FakeSide,
    )
    monkeypatch.setitem(sys.modules, "py_clob_client_v2", fake_module)

    with pytest.raises(PolymarketSubmissionUnknownError) as exc_info:
        await PolymarketSDKClient().submit_order(
            PolymarketOrderRequest(
                market_id="m-cp06",
                token_id="t-yes",
                side=Side.BUY.value,
                price=0.4,
                size=25.0,
                notional_usdc=10.0,
                estimated_quantity=25.0,
                order_type="limit",
                time_in_force=TimeInForce.GTC.value,
                max_slippage_bps=50,
            ),
            _live_settings().polymarket.credentials(),
        )

    message = str(exc_info.value)
    assert "reconcile" in message.lower()


@pytest.mark.asyncio
async def test_polymarket_sdk_client_maps_limit_ioc_to_fak(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A limit order with TIF=IOC must map to FAK, not silently demote to
    GTC (which would rest in the book instead of being killed unfilled)."""
    calls: dict[str, object] = {}

    class FakeApiCreds:
        def __init__(self, **kwargs: object) -> None:
            del kwargs

    @dataclass
    class FakeOrderArgs:
        token_id: str
        price: float
        side: object
        size: float

    class FakePartialCreateOrderOptions:
        pass

    class FakeOrderType:
        GTC = "GTC"
        FOK = "FOK"
        FAK = "FAK"
        GTD = "GTD"

    class FakeSide:
        BUY = "SDK_BUY"

    class FakeClobClient:
        def __init__(self, **kwargs: object) -> None:
            del kwargs

        def create_and_post_order(
            self,
            *,
            order_args: FakeOrderArgs,
            options: FakePartialCreateOrderOptions,
            order_type: object,
        ) -> dict[str, object]:
            del options
            calls["order_args"] = order_args
            calls["order_type"] = order_type
            return {"orderID": "x", "status": "matched", "errorMsg": ""}

    fake_module = SimpleNamespace(
        ApiCreds=FakeApiCreds,
        ClobClient=FakeClobClient,
        OrderArgs=FakeOrderArgs,
        MarketOrderArgs=FakeOrderArgs,
        OrderType=FakeOrderType,
        PartialCreateOrderOptions=FakePartialCreateOrderOptions,
        Side=FakeSide,
    )
    monkeypatch.setitem(sys.modules, "py_clob_client_v2", fake_module)

    await PolymarketSDKClient().submit_order(
        PolymarketOrderRequest(
            market_id="m-cp06",
            token_id="t-yes",
            side=Side.BUY.value,
            price=0.4,
            size=25.0,
            notional_usdc=10.0,
            estimated_quantity=25.0,
            order_type="limit",
            time_in_force=TimeInForce.IOC.value,
            max_slippage_bps=50,
        ),
        _live_settings().polymarket.credentials(),
    )

    assert calls["order_type"] == "FAK", "limit + IOC must map to FAK"


def test_risk_manager_rejects_above_max_quantity_shares() -> None:
    """RiskSettings.max_quantity_shares clamps the low-price-token blow-up
    case (e.g. limit_price=0.001 → notional/$10 = 10,000 shares)."""
    manager = RiskManager(
        RiskSettings(
            max_position_per_market=100.0,
            max_total_exposure=1000.0,
            max_quantity_shares=5_000.0,
        )
    )

    # 10 USDC at price 0.001 = 10,000 shares — above the 5,000 cap.
    over_cap = manager.check(
        _decision(notional_usdc=10.0, limit_price=0.001),
        _portfolio(),
    )
    assert over_cap.approved is False
    assert over_cap.reason == "max_quantity_shares"

    # Same notional at a normal price stays under the cap.
    in_cap = manager.check(
        _decision(notional_usdc=10.0, limit_price=0.4),
        _portfolio(),
    )
    assert in_cap.approved is True


def test_risk_manager_quantity_cap_disabled_by_default() -> None:
    """Default RiskSettings has no max_quantity_shares — preserves
    backward-compatible behaviour for users who haven't opted in."""
    manager = RiskManager(
        RiskSettings(max_position_per_market=100.0, max_total_exposure=1000.0)
    )
    decision = _decision(notional_usdc=10.0, limit_price=0.001)
    assert manager.check(decision, _portfolio()).approved is True


# --- review-loop round-1 follow-ups (codex findings f2 and f3) ---


@pytest.mark.asyncio
async def test_executor_categorizes_submission_unknown_distinctly_from_venue_rejection() -> None:
    """Codex finding f2: a `PolymarketSubmissionUnknownError` (timeout)
    must be released distinctly from `venue_rejection` so retries do not
    green-light a duplicate submission of an order that may already be
    on the venue."""
    from pms.actuator.adapters.polymarket import PolymarketSubmissionUnknownError

    @dataclass
    class _UnknownTimeoutAdapter:
        calls: int = 0

        async def execute(
            self,
            decision: TradeDecision,
            portfolio: Portfolio | None = None,
        ) -> OrderState:
            del decision, portfolio
            self.calls += 1
            raise PolymarketSubmissionUnknownError(
                "Polymarket live order submission timed out; reconcile"
            )

    in_memory_store = InMemoryFeedbackStore()
    store = cast(FeedbackStore, in_memory_store)
    dedup = InMemoryDedupStore()
    timeout_adapter = _UnknownTimeoutAdapter()
    actuator = executor.ActuatorExecutor(
        adapter=cast(executor.ActuatorAdapter, timeout_adapter),
        risk=RiskManager(
            RiskSettings(max_position_per_market=100.0, max_total_exposure=1000.0)
        ),
        feedback=ActuatorFeedback(store),
        dedup_store=dedup,
    )

    with pytest.raises(PolymarketSubmissionUnknownError):
        await actuator.execute(_decision(decision_id="d-unknown"), _portfolio())

    feedbacks = await in_memory_store.all()
    submission_unknown = [
        f for f in feedbacks if getattr(f, "category", None) == "submission_unknown"
    ]
    venue_rejection = [
        f for f in feedbacks if getattr(f, "category", None) == "venue_rejection"
    ]
    # The point of this test: the unknown timeout must NOT be logged as
    # venue_rejection; it gets its own category.
    assert len(submission_unknown) == 1
    assert len(venue_rejection) == 0


@pytest.mark.asyncio
async def test_polymarket_sdk_parses_partial_fill_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex finding f3: an SDK response with explicit `filled_notional`
    and status != MATCHED (e.g. IOC limit with partial match) must
    surface positive fill values — not get silently dropped."""

    class FakeApiCreds:
        def __init__(self, **kwargs: object) -> None:
            del kwargs

    class FakeOrderArgs:
        def __init__(self, **kwargs: object) -> None:
            del kwargs

    class FakePartialCreateOrderOptions:
        pass

    class FakeOrderType:
        FAK = "FAK"
        FOK = "FOK"
        GTC = "GTC"
        GTD = "GTD"

    class FakeSide:
        BUY = "SDK_BUY"

    class FakeClobClient:
        def __init__(self, **kwargs: object) -> None:
            del kwargs

        def create_and_post_order(self, **kwargs: object) -> dict[str, object]:
            del kwargs
            # IOC limit that filled $4 of a $10 order, half a share at 0.5,
            # and was then cancelled. Status is `live`/`cancelled` post-IOC.
            return {
                "orderID": "sdk-partial-1",
                "status": "live",
                "errorMsg": "",
                "filled_notional_usdc": 4.0,
                "filled_quantity": 8.0,
                "fill_price": 0.5,
            }

    fake_module = SimpleNamespace(
        ApiCreds=FakeApiCreds,
        ClobClient=FakeClobClient,
        OrderArgs=FakeOrderArgs,
        MarketOrderArgs=FakeOrderArgs,
        OrderType=FakeOrderType,
        PartialCreateOrderOptions=FakePartialCreateOrderOptions,
        Side=FakeSide,
    )
    monkeypatch.setitem(sys.modules, "py_clob_client_v2", fake_module)

    result = await PolymarketSDKClient().submit_order(
        PolymarketOrderRequest(
            market_id="m-cp06",
            token_id="t-yes",
            side=Side.BUY.value,
            price=0.5,
            size=20.0,
            notional_usdc=10.0,
            estimated_quantity=20.0,
            order_type="limit",
            time_in_force=TimeInForce.IOC.value,
            max_slippage_bps=50,
        ),
        _live_settings().polymarket.credentials(),
    )

    # Pre-fix: filled_notional would be 0 because status != "matched".
    assert result.filled_notional_usdc == pytest.approx(4.0)
    assert result.filled_quantity == pytest.approx(8.0)
    assert result.fill_price == pytest.approx(0.5)
    assert result.remaining_notional_usdc == pytest.approx(6.0)
    assert result.status == "live"


def test_fill_from_order_emits_fill_for_partial_status() -> None:
    """Codex finding f3, runner side: `_fill_from_order` must emit a
    FillRecord whenever filled_notional > 0 AND fill_price > 0 — not
    only on status == MATCHED."""
    from pms.runner import _fill_from_order

    decision = _decision(notional_usdc=10.0)
    partial_fill_state = OrderState(
        order_id="sdk-partial-1",
        decision_id=decision.decision_id,
        status="live",  # partial fill, not matched
        market_id=decision.market_id,
        token_id=decision.token_id,
        venue=decision.venue,
        requested_notional_usdc=decision.notional_usdc,
        filled_notional_usdc=4.0,
        remaining_notional_usdc=6.0,
        fill_price=0.5,
        submitted_at=datetime(2026, 4, 25, tzinfo=UTC),
        last_updated_at=datetime(2026, 4, 25, tzinfo=UTC),
        raw_status="live",
        strategy_id=decision.strategy_id,
        strategy_version_id=decision.strategy_version_id,
        filled_quantity=8.0,
    )

    fill = _fill_from_order(partial_fill_state, decision, None)

    assert fill is not None, "partial fill must produce a FillRecord"
    assert fill.fill_notional_usdc == pytest.approx(4.0)
    assert fill.fill_quantity == pytest.approx(8.0)
    assert fill.fill_price == pytest.approx(0.5)
    assert fill.status == "live"


def test_fill_from_order_drops_zero_filled_state() -> None:
    """Sanity: status=live with NO fill data must still drop (no fill
    record for a resting limit order that hasn't matched anything)."""
    from pms.runner import _fill_from_order

    decision = _decision(notional_usdc=10.0)
    no_fill_state = OrderState(
        order_id="sdk-resting-1",
        decision_id=decision.decision_id,
        status="live",
        market_id=decision.market_id,
        token_id=decision.token_id,
        venue=decision.venue,
        requested_notional_usdc=decision.notional_usdc,
        filled_notional_usdc=0.0,
        remaining_notional_usdc=decision.notional_usdc,
        fill_price=None,
        submitted_at=datetime(2026, 4, 25, tzinfo=UTC),
        last_updated_at=datetime(2026, 4, 25, tzinfo=UTC),
        raw_status="live",
        strategy_id=decision.strategy_id,
        strategy_version_id=decision.strategy_version_id,
        filled_quantity=0.0,
    )

    assert _fill_from_order(no_fill_state, decision, None) is None


# --- review-loop round-2 follow-ups (codex findings f7 and f8) ---


def test_coerce_float_or_none_rejects_nan_and_infinity() -> None:
    """Codex finding f8: a venue response surfacing `NaN` / `inf` (in
    floats or strings) must produce `None`, not corrupt the persisted
    fill with a non-finite numeric value."""
    from pms.actuator.adapters.polymarket import _coerce_float_or_none

    assert _coerce_float_or_none(float("nan")) is None
    assert _coerce_float_or_none(float("inf")) is None
    assert _coerce_float_or_none(float("-inf")) is None
    assert _coerce_float_or_none("nan") is None
    assert _coerce_float_or_none("inf") is None
    assert _coerce_float_or_none("Infinity") is None
    # Sanity: well-formed values still pass through.
    assert _coerce_float_or_none(0.0) == 0.0
    assert _coerce_float_or_none(0.5) == 0.5
    assert _coerce_float_or_none("0.5") == 0.5
    assert _coerce_float_or_none(True) is None  # bool subclass of int
    assert _coerce_float_or_none("nope") is None


def _partial_fill_request() -> PolymarketOrderRequest:
    return PolymarketOrderRequest(
        market_id="m-cp06",
        token_id="t-yes",
        side=Side.BUY.value,
        price=0.5,
        size=20.0,
        notional_usdc=10.0,
        estimated_quantity=20.0,
        order_type="limit",
        time_in_force=TimeInForce.GTC.value,
        max_slippage_bps=50,
    )


@pytest.mark.asyncio
async def test_polymarket_partial_fill_rejects_overfilled_notional(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex finding f8: filled_notional > requested_notional must be
    rejected — the venue cannot fill more than was ordered."""
    _install_partial_fill_sdk(
        monkeypatch,
        response={
            "orderID": "x",
            "status": "live",
            "filled_notional_usdc": 15.0,  # > 10.0 requested
            "filled_quantity": 30.0,
            "fill_price": 0.5,
        },
    )

    with pytest.raises(LiveTradingDisabledError, match="exceeds requested"):
        await PolymarketSDKClient().submit_order(
            _partial_fill_request(),
            _live_settings().polymarket.credentials(),
        )


@pytest.mark.asyncio
async def test_polymarket_partial_fill_rejects_negative_filled_quantity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex finding f8: negative filled_quantity is malformed and must
    be rejected."""
    _install_partial_fill_sdk(
        monkeypatch,
        response={
            "orderID": "x",
            "status": "live",
            "filled_notional_usdc": 4.0,
            "filled_quantity": -8.0,
            "fill_price": 0.5,
        },
    )

    with pytest.raises(LiveTradingDisabledError, match="negative filled_quantity"):
        await PolymarketSDKClient().submit_order(
            _partial_fill_request(),
            _live_settings().polymarket.credentials(),
        )


@pytest.mark.asyncio
async def test_polymarket_partial_fill_rejects_out_of_range_fill_price(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex finding f8: fill_price outside (0, 1] is invalid for a
    Polymarket probability market and must be rejected."""
    _install_partial_fill_sdk(
        monkeypatch,
        response={
            "orderID": "x",
            "status": "live",
            "filled_notional_usdc": 4.0,
            "filled_quantity": 8.0,
            "fill_price": 1.5,  # impossible probability
        },
    )

    with pytest.raises(LiveTradingDisabledError, match="outside"):
        await PolymarketSDKClient().submit_order(
            _partial_fill_request(),
            _live_settings().polymarket.credentials(),
        )


# --- review-loop round-3 follow-ups (codex findings f9 and f10) ---


@pytest.mark.asyncio
async def test_polymarket_matched_status_rejects_invalid_explicit_quantity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex finding f9: even when status is `matched` (the backwards-
    compat fallback), a venue response with an invalid explicit field
    must be rejected — not silently persisted via the fallback path."""
    _install_partial_fill_sdk(
        monkeypatch,
        response={
            "orderID": "x",
            "status": "matched",
            # No explicit_filled_notional → matched fallback would
            # synthesize a full fill, but the explicit (negative)
            # quantity must still trigger validation.
            "filled_quantity": -8.0,
            "fill_price": 0.5,
        },
    )

    with pytest.raises(LiveTradingDisabledError, match="negative filled_quantity"):
        await PolymarketSDKClient().submit_order(
            _partial_fill_request(),
            _live_settings().polymarket.credentials(),
        )


@pytest.mark.asyncio
async def test_polymarket_matched_status_rejects_invalid_explicit_price(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex finding f9: matched fallback must also reject an explicit
    out-of-range fill_price."""
    _install_partial_fill_sdk(
        monkeypatch,
        response={
            "orderID": "x",
            "status": "matched",
            "fill_price": 1.5,  # impossible probability
        },
    )

    with pytest.raises(LiveTradingDisabledError, match="outside"):
        await PolymarketSDKClient().submit_order(
            _partial_fill_request(),
            _live_settings().polymarket.credentials(),
        )


@pytest.mark.asyncio
async def test_polymarket_matched_status_rejects_negative_explicit_notional(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex finding f9: matched fallback must reject negative explicit
    filled_notional (which previously fell through the `> 0.0` filter
    into the matched-fallback branch and was silently ignored)."""
    _install_partial_fill_sdk(
        monkeypatch,
        response={
            "orderID": "x",
            "status": "matched",
            "filled_notional_usdc": -4.0,
        },
    )

    with pytest.raises(LiveTradingDisabledError, match="negative filled_notional"):
        await PolymarketSDKClient().submit_order(
            _partial_fill_request(),
            _live_settings().polymarket.credentials(),
        )


# --- review-loop round-4 follow-ups (codex finding f11) ---


@pytest.mark.asyncio
async def test_polymarket_partial_fill_derives_quantity_when_only_notional_and_price(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex finding f11: a venue response with filled_notional > 0 and
    fill_price > 0 but NO filled_quantity must derive quantity from
    notional/price — not silently persist filled_quantity=0 (which
    would corrupt share accounting)."""
    _install_partial_fill_sdk(
        monkeypatch,
        response={
            "orderID": "x",
            "status": "live",
            "filled_notional_usdc": 4.0,
            "fill_price": 0.5,
            # filled_quantity intentionally omitted
        },
    )

    result = await PolymarketSDKClient().submit_order(
        _partial_fill_request(),
        _live_settings().polymarket.credentials(),
    )

    assert result.filled_notional_usdc == pytest.approx(4.0)
    # 4.0 / 0.5 = 8.0 shares — derived, not silent zero.
    assert result.filled_quantity == pytest.approx(8.0)
    assert result.fill_price == pytest.approx(0.5)


@pytest.mark.asyncio
async def test_polymarket_partial_fill_rejects_zero_quantity_with_positive_notional(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex finding f11: an explicit `filled_quantity == 0` paired
    with positive `filled_notional` is contradictory (a $4 fill of zero
    shares) and must be rejected, not persisted."""
    _install_partial_fill_sdk(
        monkeypatch,
        response={
            "orderID": "x",
            "status": "live",
            "filled_notional_usdc": 4.0,
            "filled_quantity": 0.0,  # contradicts positive notional
            "fill_price": 0.5,
        },
    )

    with pytest.raises(LiveTradingDisabledError, match="filled_quantity == 0"):
        await PolymarketSDKClient().submit_order(
            _partial_fill_request(),
            _live_settings().polymarket.credentials(),
        )


# --- review-loop round-5 follow-ups (codex finding f12) ---


@pytest.mark.asyncio
async def test_polymarket_matched_status_rejects_explicit_zero_notional(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex finding f12: status=matched with `filled_notional_usdc: 0`
    is contradictory — the matched-fallback heuristic must NOT
    synthesize a $10 / fully-filled response from an explicit zero."""
    _install_partial_fill_sdk(
        monkeypatch,
        response={
            "orderID": "x",
            "status": "matched",
            "filled_notional_usdc": 0.0,
        },
    )

    with pytest.raises(LiveTradingDisabledError, match="inconsistent"):
        await PolymarketSDKClient().submit_order(
            _partial_fill_request(),
            _live_settings().polymarket.credentials(),
        )


@pytest.mark.asyncio
async def test_polymarket_matched_status_rejects_partial_fields_without_notional(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex finding f12: status=matched with explicit
    filled_quantity / fill_price but NO filled_notional must be
    rejected — the matched-fallback would otherwise synthesize a
    notional from the order while using the explicit zero quantity,
    persisting a $10 fill of 0 shares."""
    _install_partial_fill_sdk(
        monkeypatch,
        response={
            "orderID": "x",
            "status": "matched",
            "filled_quantity": 0.0,
            "fill_price": 0.5,
        },
    )

    with pytest.raises(LiveTradingDisabledError, match="inconsistent"):
        await PolymarketSDKClient().submit_order(
            _partial_fill_request(),
            _live_settings().polymarket.credentials(),
        )


@pytest.mark.asyncio
async def test_polymarket_matched_status_synthesizes_full_fill_only_when_no_explicit_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex finding f12 (positive case): status=matched WITHOUT any
    explicit fill fields IS the legitimate backwards-compat path — the
    full-fill heuristic should still fire to support venues that
    surface only `status: matched` without per-fill detail."""
    _install_partial_fill_sdk(
        monkeypatch,
        response={
            "orderID": "x",
            "status": "matched",
            # No explicit fill fields at all.
        },
    )

    result = await PolymarketSDKClient().submit_order(
        _partial_fill_request(),
        _live_settings().polymarket.credentials(),
    )

    assert result.filled_notional_usdc == pytest.approx(10.0)
    assert result.filled_quantity == pytest.approx(20.0)  # 10 / 0.5
    assert result.fill_price == pytest.approx(0.5)


# --- review-loop round-6 follow-ups (codex finding f13) ---


@pytest.mark.asyncio
async def test_polymarket_matched_status_rejects_malformed_filled_notional(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex finding f13: a venue response with a PRESENT-BUT-MALFORMED
    fill field (e.g. `"nan"`, `"inf"`, `null`) must be rejected — not
    treated as 'no explicit fields' and silently routed into the
    matched-fallback full-fill synthesis path."""
    _install_partial_fill_sdk(
        monkeypatch,
        response={
            "orderID": "x",
            "status": "matched",
            "filled_notional_usdc": "nan",  # present but unparseable
        },
    )

    with pytest.raises(LiveTradingDisabledError, match="unparseable filled_notional"):
        await PolymarketSDKClient().submit_order(
            _partial_fill_request(),
            _live_settings().polymarket.credentials(),
        )


@pytest.mark.asyncio
async def test_polymarket_matched_status_rejects_malformed_filled_quantity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex finding f13: malformed filled_quantity (here `"inf"`)
    must be rejected, not silently dropped into the matched fallback."""
    _install_partial_fill_sdk(
        monkeypatch,
        response={
            "orderID": "x",
            "status": "matched",
            "filled_quantity": "inf",
        },
    )

    with pytest.raises(LiveTradingDisabledError, match="unparseable filled_quantity"):
        await PolymarketSDKClient().submit_order(
            _partial_fill_request(),
            _live_settings().polymarket.credentials(),
        )


@pytest.mark.asyncio
async def test_polymarket_matched_status_rejects_null_fill_price(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex finding f13: a JSON null in a present field is also
    'present but unparseable' and must be rejected."""
    _install_partial_fill_sdk(
        monkeypatch,
        response={
            "orderID": "x",
            "status": "matched",
            "fill_price": None,  # JSON null
        },
    )

    with pytest.raises(LiveTradingDisabledError, match="unparseable fill_price"):
        await PolymarketSDKClient().submit_order(
            _partial_fill_request(),
            _live_settings().polymarket.credentials(),
        )


def _install_partial_fill_sdk(
    monkeypatch: pytest.MonkeyPatch,
    *,
    response: dict[str, object],
) -> None:
    """Wires a fake `py_clob_client_v2` whose limit-order endpoint
    returns the given response. Used by the f8 validation tests."""

    class FakeApiCreds:
        def __init__(self, **kwargs: object) -> None:
            del kwargs

    class FakeOrderArgs:
        def __init__(self, **kwargs: object) -> None:
            del kwargs

    class FakePartialCreateOrderOptions:
        pass

    class FakeOrderType:
        FAK = "FAK"
        FOK = "FOK"
        GTC = "GTC"
        GTD = "GTD"

    class FakeSide:
        BUY = "SDK_BUY"

    class FakeClobClient:
        def __init__(self, **kwargs: object) -> None:
            del kwargs

        def create_and_post_order(self, **kwargs: object) -> dict[str, object]:
            del kwargs
            return response

    fake_module = SimpleNamespace(
        ApiCreds=FakeApiCreds,
        ClobClient=FakeClobClient,
        OrderArgs=FakeOrderArgs,
        MarketOrderArgs=FakeOrderArgs,
        OrderType=FakeOrderType,
        PartialCreateOrderOptions=FakePartialCreateOrderOptions,
        Side=FakeSide,
    )
    monkeypatch.setitem(sys.modules, "py_clob_client_v2", fake_module)
