from __future__ import annotations

import inspect
import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Literal, cast

import pytest

from pms.actuator import executor
from pms.actuator.adapters import backtest
from pms.actuator.adapters.backtest import BacktestActuator
from pms.actuator.adapters.paper import PaperActuator
from pms.actuator.adapters.polymarket import PolymarketActuator
from pms.actuator.feedback import ActuatorFeedback
from pms.actuator.risk import InsufficientLiquidityError, RiskManager
from pms.config import PMSSettings, RiskSettings
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
