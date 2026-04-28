from __future__ import annotations

from dataclasses import FrozenInstanceError
from datetime import UTC, datetime, timedelta
from typing import Any, cast

import pytest

from pms.core.enums import TimeInForce
from pms.execution.planner import ExecutionPlanner, ExecutionPlan
from pms.execution.quotes import ExecutableQuote
from pms.strategies.intents import TradeIntent


NOW = datetime(2026, 4, 28, 9, 0, tzinfo=UTC)


class FakeQuoteProvider:
    def __init__(self, quote: ExecutableQuote | None) -> None:
        self.quote = quote
        self.calls: list[TradeIntent] = []

    async def quote_for_intent(self, intent: TradeIntent) -> ExecutableQuote | None:
        self.calls.append(intent)
        return self.quote


def _intent(**overrides: object) -> TradeIntent:
    data: dict[str, object] = {
        "intent_id": "intent-1",
        "strategy_id": "ripple",
        "strategy_version_id": "ripple-v1",
        "candidate_id": "candidate-1",
        "market_id": "market-1",
        "token_id": "token-yes",
        "venue": "polymarket",
        "side": "BUY",
        "outcome": "YES",
        "limit_price": 0.60,
        "notional_usdc": 25.0,
        "expected_price": 0.70,
        "expected_edge": 0.16,
        "max_slippage_bps": 50,
        "time_in_force": TimeInForce.GTC,
        "evidence_refs": ("judgement-1",),
        "created_at": NOW,
    }
    data.update(overrides)
    return TradeIntent(**cast(Any, data))


def _quote(**overrides: object) -> ExecutableQuote:
    data: dict[str, object] = {
        "market_id": "market-1",
        "token_id": "token-yes",
        "venue": "polymarket",
        "side": "BUY",
        "best_price": 0.54,
        "available_size": 100.0,
        "book_timestamp": NOW,
        "quote_hash": "quote-hash-1",
        "min_order_size_usdc": 5.0,
        "tick_size": 0.01,
        "fee_bps": 0,
    }
    data.update(overrides)
    return ExecutableQuote(**cast(Any, data))


@pytest.mark.asyncio
async def test_planner_accepts_single_leg_intent_without_network_or_actuator() -> None:
    intent = _intent()
    provider = FakeQuoteProvider(_quote())
    plan = await ExecutionPlanner(provider).plan(intent, as_of=NOW)

    assert provider.calls == [intent]
    assert isinstance(plan, ExecutionPlan)
    assert plan.rejection_reason is None
    assert plan.quote_hash == "quote-hash-1"
    assert plan.strategy_id == intent.strategy_id
    assert plan.strategy_version_id == intent.strategy_version_id
    assert plan.audit_metadata["edge_after_cost"] > 0.0

    order = plan.planned_orders[0]
    assert order.intent_id == intent.intent_id
    assert order.intent_key == intent.intent_id
    assert order.market_id == intent.market_id
    assert order.token_id == intent.token_id
    assert order.venue == intent.venue
    assert order.side == intent.side
    assert order.outcome == intent.outcome
    assert order.notional_usdc == intent.notional_usdc
    assert order.limit_price == intent.limit_price
    assert order.expected_edge == pytest.approx(plan.audit_metadata["edge_after_cost"])
    assert order.time_in_force is TimeInForce.GTC

    with pytest.raises(FrozenInstanceError):
        setattr(plan, "plan_id", "mutated")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("quote", "intent", "reason"),
    [
        (_quote(book_timestamp=NOW - timedelta(seconds=31)), _intent(), "stale_book"),
        (_quote(token_id=None), _intent(), "missing_token_id"),
        (_quote(available_size=10.0), _intent(), "insufficient_executable_notional"),
        (_quote(min_order_size_usdc=50.0), _intent(), "min_size_violation"),
        (_quote(best_price=1.0), _intent(), "impossible_price"),
        (_quote(tick_size=0.04), _intent(), "invalid_tick_size"),
        (_quote(best_price=0.62), _intent(limit_price=0.70, expected_price=0.62), "loss_of_edge_after_costs"),
        (None, _intent(), "quote_unavailable"),
    ],
)
async def test_planner_rejects_non_executable_quotes(
    quote: ExecutableQuote | None,
    intent: TradeIntent,
    reason: str,
) -> None:
    plan = await ExecutionPlanner(FakeQuoteProvider(quote)).plan(intent, as_of=NOW)

    assert plan.planned_orders == ()
    assert plan.rejection_reason == reason
    assert plan.intent_id == intent.intent_id
    assert plan.strategy_id == intent.strategy_id
    assert plan.strategy_version_id == intent.strategy_version_id
