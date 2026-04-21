from __future__ import annotations

from datetime import UTC, datetime
from typing import cast

import pytest

from pms.actuator.executor import ActuatorExecutor
from pms.actuator.feedback import ActuatorFeedback
from pms.actuator.risk import RiskManager
from pms.config import RiskSettings
from pms.core.enums import OrderStatus
from pms.core.models import OrderState, Portfolio, TradeDecision
from pms.storage.dedup_store import InMemoryDedupStore
from pms.storage.feedback_store import FeedbackStore
from tests.support.fake_stores import InMemoryFeedbackStore


def _decision(*, notional_usdc: float = 0.5) -> TradeDecision:
    return TradeDecision(
        decision_id="decision-cp14",
        market_id="market-cp14",
        token_id="token-cp14",
        venue="polymarket",
        side="BUY",
        notional_usdc=notional_usdc,
        order_type="limit",
        max_slippage_bps=50,
        stop_conditions=["cp14"],
        prob_estimate=0.5,
        expected_edge=0.0,
        time_in_force="GTC",
        opportunity_id="opportunity-cp14",
        strategy_id="strategy-cp14",
        strategy_version_id="strategy-cp14-v1",
        limit_price=0.5,
    )


def _portfolio() -> Portfolio:
    return Portfolio(
        total_usdc=100.0,
        free_usdc=100.0,
        locked_usdc=0.0,
        open_positions=[],
    )


class RecordingAdapter:
    def __init__(self) -> None:
        self.calls = 0

    async def execute(
        self,
        decision: TradeDecision,
        portfolio: Portfolio | None = None,
    ) -> OrderState:
        del portfolio
        self.calls += 1
        now = datetime(2026, 4, 21, tzinfo=UTC)
        return OrderState(
            order_id="order-cp14",
            decision_id=decision.decision_id,
            status=OrderStatus.MATCHED.value,
            market_id=decision.market_id,
            token_id=decision.token_id,
            venue=decision.venue,
            requested_notional_usdc=decision.notional_usdc,
            filled_notional_usdc=decision.notional_usdc,
            remaining_notional_usdc=0.0,
            fill_price=decision.limit_price,
            submitted_at=now,
            last_updated_at=now,
            raw_status="matched",
            strategy_id=decision.strategy_id,
            strategy_version_id=decision.strategy_version_id,
            filled_quantity=decision.notional_usdc / decision.limit_price,
        )


@pytest.mark.asyncio
async def test_actuator_executor_rejects_sub_min_order_without_adapter_calls() -> None:
    adapter = RecordingAdapter()
    executor = ActuatorExecutor(
        adapter=adapter,
        risk=RiskManager(RiskSettings(min_order_usdc=1.0)),
        feedback=ActuatorFeedback(cast(FeedbackStore, InMemoryFeedbackStore())),
    )

    state = await executor.execute(_decision(), _portfolio())

    assert state.status == OrderStatus.INVALID.value
    assert state.raw_status == "min_order_usdc"
    assert adapter.calls == 0


@pytest.mark.asyncio
async def test_actuator_executor_default_dedup_store_soft_releases_rejections() -> None:
    adapter = RecordingAdapter()
    feedback_store = cast(FeedbackStore, InMemoryFeedbackStore())
    executor = ActuatorExecutor(
        adapter=adapter,
        risk=RiskManager(RiskSettings(min_order_usdc=1.0)),
        feedback=ActuatorFeedback(feedback_store),
    )

    first = await executor.execute(_decision(), _portfolio())
    second = await executor.execute(_decision(), _portfolio())

    assert isinstance(executor.dedup_store, InMemoryDedupStore)
    assert first.status == OrderStatus.INVALID.value
    assert first.raw_status == "min_order_usdc"
    assert second.status == OrderStatus.INVALID.value
    assert second.raw_status == "duplicate_decision"
    assert adapter.calls == 0
    assert executor.dedup_store.contains("decision-cp14") is True
