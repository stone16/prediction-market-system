from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from typing import cast

import pytest

from pms.actuator.executor import ActuatorExecutor
from pms.actuator.feedback import ActuatorFeedback
from pms.actuator.risk import RiskManager
from pms.config import RiskSettings
from pms.controller.pipeline import _default_portfolio
from pms.core.enums import OrderStatus, TimeInForce
from pms.core.models import OrderState, Portfolio, TradeDecision
from pms.event_stream import RuntimeEventBus
from pms.storage.feedback_store import FeedbackStore
from tests.support.fake_stores import InMemoryFeedbackStore


NOW = datetime(2026, 5, 6, 8, 0, tzinfo=UTC)


class RecordingAdapter:
    calls = 0

    async def execute(
        self,
        decision: TradeDecision,
        portfolio: Portfolio | None = None,
    ) -> OrderState:
        del decision, portfolio
        self.calls += 1
        raise AssertionError("adapter should not be called after auto-halt")


def _decision() -> TradeDecision:
    return TradeDecision(
        decision_id="decision-halt-publish",
        market_id="market-halt-publish",
        token_id="token-halt-publish",
        venue="polymarket",
        side="BUY",
        notional_usdc=5.0,
        order_type="limit",
        max_slippage_bps=25,
        stop_conditions=["halt-publish-test"],
        prob_estimate=0.6,
        expected_edge=0.1,
        time_in_force=TimeInForce.GTC,
        opportunity_id="opportunity-halt-publish",
        strategy_id="strategy",
        strategy_version_id="strategy-v1",
        limit_price=0.5,
    )


def _portfolio() -> Portfolio:
    return replace(
        _default_portfolio(),
        total_usdc=100.0,
        free_usdc=100.0,
        locked_usdc=0.0,
        max_drawdown_pct=21.0,
    )


@pytest.mark.asyncio
async def test_executor_publishes_halt_event_from_real_auto_halt_path() -> None:
    bus = RuntimeEventBus()
    replay, queue = await bus.subscribe()
    assert replay == []
    manager = RiskManager(RiskSettings(max_drawdown_pct=20.0))
    executor = ActuatorExecutor(
        adapter=RecordingAdapter(),
        risk=manager,
        feedback=ActuatorFeedback(cast(FeedbackStore, InMemoryFeedbackStore())),
        event_bus=bus,
    )

    state = await executor.execute(_decision(), _portfolio())
    event = queue.get_nowait()

    assert state.status == OrderStatus.INVALID.value
    assert state.raw_status == "drawdown_circuit_breaker"
    assert manager.halt_events[-1].state.reason == "drawdown_circuit_breaker"
    assert event.event_type == "pms.halt.drawdown_circuit_breaker"
    assert event.market_id == "market-halt-publish"
    assert event.decision_id == "decision-halt-publish"
