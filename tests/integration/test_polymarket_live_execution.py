from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Literal, cast

import pytest

from pms.actuator.adapters.polymarket import (
    LiveOrderPreview,
    PolymarketActuator,
    PolymarketOrderResult,
)
from pms.actuator.executor import ActuatorExecutor
from pms.actuator.feedback import ActuatorFeedback
from pms.actuator.risk import RiskManager
from pms.config import PMSSettings, PolymarketSettings, RiskSettings
from pms.core.enums import OrderStatus, RunMode, Side, TimeInForce
from pms.core.models import FillRecord, OrderState, TradeDecision
from pms.runner import Runner
from pms.storage.dedup_store import InMemoryDedupStore
from pms.storage.eval_store import EvalStore
from pms.storage.feedback_store import FeedbackStore
from pms.storage.fill_store import FillStore
from pms.storage.order_store import OrderStore
from tests.support.fake_stores import InMemoryEvalStore, InMemoryFeedbackStore


@dataclass
class RecordingOrderStore:
    inserted: list[OrderState] = field(default_factory=list)

    async def insert(self, order: OrderState) -> None:
        self.inserted.append(order)


@dataclass
class RecordingFillStore:
    inserted: list[FillRecord] = field(default_factory=list)

    async def insert(self, fill: FillRecord) -> None:
        self.inserted.append(fill)


@dataclass
class AllowFirstOrderGate:
    approvals: int = 0

    async def approve_first_order(self, preview: LiveOrderPreview) -> bool:
        del preview
        self.approvals += 1
        return True


@dataclass
class MockPolymarketClient:
    submitted: list[object] = field(default_factory=list)

    async def submit_order(
        self,
        order: object,
        credentials: object,
    ) -> PolymarketOrderResult:
        del credentials
        self.submitted.append(order)
        return PolymarketOrderResult(
            order_id="pm-live-integration-order",
            status=OrderStatus.MATCHED.value,
            raw_status="matched",
            filled_notional_usdc=12.0,
            remaining_notional_usdc=0.0,
            fill_price=0.48,
            filled_quantity=25.0,
        )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_live_polymarket_order_uses_runner_order_and_fill_persistence_paths() -> None:
    order_store = RecordingOrderStore()
    fill_store = RecordingFillStore()
    feedback_store = InMemoryFeedbackStore()
    settings = _live_settings()
    runner = Runner(
        config=settings,
        eval_store=cast(EvalStore, InMemoryEvalStore()),
        feedback_store=cast(FeedbackStore, feedback_store),
        order_store=cast(OrderStore, order_store),
        fill_store=cast(FillStore, fill_store),
    )
    client = MockPolymarketClient()
    gate = AllowFirstOrderGate()
    runner.actuator_executor = ActuatorExecutor(
        adapter=PolymarketActuator(settings, client=client, operator_gate=gate),
        risk=RiskManager(settings.risk),
        feedback=ActuatorFeedback(cast(FeedbackStore, feedback_store)),
        dedup_store=InMemoryDedupStore(),
    )

    await runner.enqueue_accepted_decision(_decision())
    task = asyncio.create_task(runner._actuator_loop())
    await runner._decision_queue.join()
    runner._stop_event.set()
    await task

    assert len(client.submitted) == 1
    assert gate.approvals == 1
    assert [order.order_id for order in order_store.inserted] == [
        "pm-live-integration-order"
    ]
    assert [fill.order_id for fill in fill_store.inserted] == [
        "pm-live-integration-order"
    ]
    assert fill_store.inserted[0].strategy_id == "default"
    assert fill_store.inserted[0].strategy_version_id == "default-v1"


def _live_settings() -> PMSSettings:
    return PMSSettings(
        mode=RunMode.LIVE,
        live_trading_enabled=True,
        auto_migrate_default_v2=False,
        risk=RiskSettings(
            max_position_per_market=1000.0,
            max_total_exposure=10_000.0,
        ),
        polymarket=PolymarketSettings(
            private_key="private-key",
            api_key="api-key",
            api_secret="api-secret",
            api_passphrase="passphrase",
            signature_type=1,
            funder_address="0xabc",
        ),
    )


def _decision(
    *,
    side: Literal["BUY", "SELL"] = Side.BUY.value,
) -> TradeDecision:
    return TradeDecision(
        decision_id="d-live-integration",
        market_id="m-live-integration",
        token_id="t-live-yes",
        venue="polymarket",
        side=side,
        notional_usdc=12.0,
        order_type="limit",
        max_slippage_bps=50,
        stop_conditions=["integration-test"],
        prob_estimate=0.6,
        expected_edge=0.2,
        time_in_force=TimeInForce.GTC,
        opportunity_id="op-live-integration",
        strategy_id="default",
        strategy_version_id="default-v1",
        action=side,
        limit_price=0.48,
        outcome="YES",
    )
