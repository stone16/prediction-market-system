from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from pms.actuator.executor import ActuatorExecutor
from pms.actuator.feedback import ActuatorFeedback
from pms.actuator.risk import HaltState, RiskManager, RiskTradeResult
from pms.config import RiskSettings
from pms.controller.pipeline import _default_portfolio
from pms.core.enums import TimeInForce
from pms.core.models import OrderState, Portfolio, TradeDecision


NOW = datetime(2026, 5, 3, 8, 0, tzinfo=UTC)


def _portfolio(*, max_drawdown_pct: float | None = None) -> Portfolio:
    return replace(_default_portfolio(), max_drawdown_pct=max_drawdown_pct)


def _decision() -> TradeDecision:
    return TradeDecision(
        decision_id="decision-auto-halt",
        market_id="market-auto-halt",
        token_id="token-auto-halt",
        venue="polymarket",
        side="BUY",
        notional_usdc=5.0,
        order_type="limit",
        max_slippage_bps=25,
        stop_conditions=["unit-test"],
        prob_estimate=0.6,
        expected_edge=0.1,
        time_in_force=TimeInForce.GTC,
        opportunity_id="opportunity-auto-halt",
        strategy_id="ripple",
        strategy_version_id="ripple-v1",
        limit_price=0.5,
    )


def _filled_order() -> OrderState:
    return OrderState(
        order_id="order-auto-halt",
        decision_id="decision-auto-halt",
        status="filled",
        market_id="market-auto-halt",
        token_id="token-auto-halt",
        venue="polymarket",
        requested_notional_usdc=5.0,
        filled_notional_usdc=5.0,
        remaining_notional_usdc=0.0,
        fill_price=0.5,
        submitted_at=NOW,
        last_updated_at=NOW,
        raw_status="filled",
        strategy_id="ripple",
        strategy_version_id="ripple-v1",
        filled_quantity=10.0,
    )


class _RecordingAdapter:
    def __init__(self) -> None:
        self.calls = 0

    async def execute(
        self,
        decision: TradeDecision,
        portfolio: Portfolio | None = None,
    ) -> OrderState:
        del decision, portfolio
        self.calls += 1
        return _filled_order()


@pytest.mark.parametrize(
    ("status_code", "expected_kind"),
    [(401, "credential_failure"), (403, "credential_failure")],
)
def test_auto_halt_triggers_on_credential_failure(
    status_code: int,
    expected_kind: str,
) -> None:
    manager = RiskManager()

    manager.record_api_error(status_code, at=NOW, trace_id="trace-credential")
    halt = manager.check_auto_halt(_portfolio(), now=NOW)

    assert halt.halted is True
    assert halt.trigger_kind == expected_kind
    assert halt.reason == "api_credential_failure"
    assert manager.halt_events[-1].trace_id == "trace-credential"


def test_auto_halt_triggers_on_drawdown_circuit_breaker() -> None:
    manager = RiskManager(RiskSettings(max_drawdown_pct=20.0))

    halt = manager.check_auto_halt(
        _portfolio(max_drawdown_pct=21.0),
        now=NOW,
        trace_id="trace-drawdown",
    )

    assert halt == HaltState(
        halted=True,
        reason="drawdown_circuit_breaker",
        triggered_at=NOW,
        trigger_kind="drawdown_circuit_breaker",
    )
    assert manager.halt_events[-1].trace_id == "trace-drawdown"


def test_auto_halt_triggers_after_five_consecutive_losses() -> None:
    manager = RiskManager()
    for index in range(5):
        manager.record_trade_result(
            RiskTradeResult(
                pnl=-1.0,
                slippage_bps=10.0,
                filled_at=NOW + timedelta(seconds=index),
            )
        )

    halt = manager.check_auto_halt(_portfolio(), now=NOW + timedelta(seconds=5))

    assert halt.halted is True
    assert halt.trigger_kind == "consecutive_losses"
    assert halt.reason == "five_consecutive_losses"


def test_auto_halt_triggers_on_average_slippage_spike() -> None:
    manager = RiskManager()
    for index in range(10):
        manager.record_trade_result(
            RiskTradeResult(
                pnl=1.0,
                slippage_bps=125.0,
                filled_at=NOW + timedelta(seconds=index),
            )
        )

    halt = manager.check_auto_halt(_portfolio(), now=NOW + timedelta(seconds=10))

    assert halt.halted is True
    assert halt.trigger_kind == "slippage_spike"
    assert halt.reason == "avg_slippage_above_100bps"


def test_auto_halt_triggers_on_order_without_fill_after_thirty_minutes() -> None:
    manager = RiskManager()

    manager.record_order_placed("order-stale", at=NOW)
    halt = manager.check_auto_halt(_portfolio(), now=NOW + timedelta(minutes=31))

    assert halt.halted is True
    assert halt.trigger_kind == "order_without_fill"
    assert halt.reason == "order_without_fill_30m"


def test_auto_halt_clears_order_without_fill_after_fill() -> None:
    manager = RiskManager()

    manager.record_order_placed("order-filled", at=NOW)
    manager.record_order_filled("order-filled")
    halt = manager.check_auto_halt(_portfolio(), now=NOW + timedelta(minutes=31))

    assert halt.halted is False


def test_auto_halt_triggers_on_three_rate_limits_in_ten_minutes() -> None:
    manager = RiskManager()
    for minutes in (0, 4, 9):
        manager.record_api_error(429, at=NOW + timedelta(minutes=minutes))

    halt = manager.check_auto_halt(_portfolio(), now=NOW + timedelta(minutes=9))

    assert halt.halted is True
    assert halt.trigger_kind == "rate_limit_exceeded"
    assert halt.reason == "three_rate_limits_10m"


def test_auto_halt_is_reversible_via_clear_halt() -> None:
    manager = RiskManager()
    manager.record_api_error(401, at=NOW)

    assert manager.check_auto_halt(_portfolio(), now=NOW).halted is True
    manager.clear_halt()

    assert manager.check_auto_halt(_portfolio(), now=NOW).halted is False


@pytest.mark.asyncio
async def test_actuator_executor_respects_auto_halt_before_order_risk() -> None:
    manager = RiskManager()
    manager.record_api_error(401, at=NOW)
    adapter = _RecordingAdapter()
    executor = ActuatorExecutor(
        adapter=adapter,
        risk=manager,
        feedback=ActuatorFeedback(),
    )

    order = await executor.execute(_decision(), _portfolio())

    assert adapter.calls == 0
    assert order.status == "rejected"
    assert order.raw_status == "credential_failure"
