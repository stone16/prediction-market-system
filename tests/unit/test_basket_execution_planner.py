from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any, cast

import pytest

from pms.core.enums import TimeInForce
from pms.execution.baskets import BasketExecutionPlanner
from pms.execution.planner import ExecutionPlanner, PlannedOrder
from pms.execution.quotes import ExecutableQuote
from pms.strategies.intents import BasketIntent, TradeIntent


NOW = datetime(2026, 4, 28, 10, 0, tzinfo=UTC)


class FakeQuoteProvider:
    def __init__(self, quotes: dict[str, ExecutableQuote | None]) -> None:
        self.quotes = quotes
        self.calls: list[str] = []

    async def quote_for_intent(self, intent: TradeIntent) -> ExecutableQuote | None:
        self.calls.append(intent.intent_id)
        return self.quotes.get(intent.intent_id)


def _intent(intent_id: str, **overrides: object) -> TradeIntent:
    data: dict[str, object] = {
        "intent_id": intent_id,
        "strategy_id": "ripple",
        "strategy_version_id": "ripple-v1",
        "candidate_id": f"candidate-{intent_id}",
        "market_id": f"market-{intent_id}",
        "token_id": f"token-{intent_id}",
        "venue": "polymarket",
        "side": "BUY",
        "outcome": "YES",
        "limit_price": 0.60,
        "notional_usdc": 20.0,
        "expected_price": 0.70,
        "expected_edge": 0.16,
        "max_slippage_bps": 50,
        "time_in_force": TimeInForce.GTC,
        "evidence_refs": (f"judgement-{intent_id}",),
        "created_at": NOW,
    }
    data.update(overrides)
    return TradeIntent(**cast(Any, data))


def _quote(intent: TradeIntent, **overrides: object) -> ExecutableQuote:
    data: dict[str, object] = {
        "market_id": intent.market_id,
        "token_id": intent.token_id,
        "venue": intent.venue,
        "side": intent.side,
        "best_price": 0.54,
        "available_size": 100.0,
        "book_timestamp": NOW,
        "quote_hash": f"quote-{intent.intent_id}",
        "min_order_size_usdc": 5.0,
        "tick_size": 0.01,
        "fee_bps": 0,
    }
    data.update(overrides)
    return ExecutableQuote(**cast(Any, data))


def _basket(*legs: TradeIntent, policy: str = "manual_review") -> BasketIntent:
    return BasketIntent(
        basket_id="basket-1",
        strategy_id="ripple",
        strategy_version_id="ripple-v1",
        legs=legs,
        execution_policy=cast(Any, policy),
        evidence_refs=("basket-evidence-1",),
        created_at=NOW,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("policy", ["manual_review", "all_or_none"])
async def test_basket_planner_accepts_executable_fixture_policies(policy: str) -> None:
    leg_a = _intent("leg-a")
    leg_b = _intent("leg-b")
    provider = FakeQuoteProvider(
        {leg.intent_id: _quote(leg) for leg in (leg_a, leg_b)}
    )

    plan = await BasketExecutionPlanner(ExecutionPlanner(provider)).plan(
        _basket(leg_a, leg_b, policy=policy),
        as_of=NOW,
    )

    assert provider.calls == ["leg-a", "leg-b"]
    assert plan.rejection_reason is None
    assert plan.execution_policy == policy
    assert plan.evidence_refs == ("basket-evidence-1",)
    assert [order.intent_id for order in plan.planned_orders] == ["leg-a", "leg-b"]
    assert all(isinstance(order, PlannedOrder) for order in plan.planned_orders)
    assert all(not hasattr(order, "decision_id") for order in plan.planned_orders)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("leg_b_quote", "reason"),
    [
        ("stale", "stale_book"),
        (None, "quote_unavailable"),
    ],
)
async def test_all_or_none_basket_rejects_without_partial_orders(
    leg_b_quote: str | None,
    reason: str,
) -> None:
    leg_a = _intent("leg-a")
    leg_b = _intent("leg-b")
    rejected_quote = (
        None
        if leg_b_quote is None
        else _quote(leg_b, book_timestamp=NOW - timedelta(seconds=31))
    )
    provider = FakeQuoteProvider(
        {
            "leg-a": _quote(leg_a),
            "leg-b": rejected_quote,
        }
    )

    plan = await BasketExecutionPlanner(ExecutionPlanner(provider)).plan(
        _basket(leg_a, leg_b, policy="all_or_none"),
        as_of=NOW,
    )

    assert plan.planned_orders == ()
    assert plan.rejection_reason == "basket_leg_rejected"
    assert plan.leg_rejection_reasons == {"leg-b": reason}
    assert plan.evidence_refs == ("basket-evidence-1",)


@pytest.mark.asyncio
async def test_basket_planner_rejects_unsupported_policy() -> None:
    leg_a = _intent("leg-a")
    leg_b = _intent("leg-b")

    plan = await BasketExecutionPlanner(ExecutionPlanner(FakeQuoteProvider({}))).plan(
        _basket(leg_a, leg_b, policy="sequential_with_hedge"),
        as_of=NOW,
    )

    assert plan.planned_orders == ()
    assert plan.rejection_reason == "unsupported_basket_policy"
    assert plan.execution_policy == "sequential_with_hedge"
    assert plan.leg_rejection_reasons == {"__basket__": "unsupported_basket_policy"}
    assert plan.evidence_refs == ("basket-evidence-1",)


def test_basket_intent_rejects_invalid_shapes_before_planning() -> None:
    leg_a = _intent("leg-a")

    with pytest.raises(ValueError, match="empty"):
        _basket(policy="manual_review")
    with pytest.raises(ValueError, match="single_leg_use_trade_intent"):
        _basket(leg_a, policy="single_leg_use_trade_intent")
    with pytest.raises(ValueError, match="mixed strategy identity"):
        _basket(leg_a, _intent("leg-b", strategy_id="other"))
