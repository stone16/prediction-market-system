from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, cast
from uuid import uuid4

from pms.actuator.risk import InsufficientLiquidityError
from pms.core.enums import OrderStatus, Side, Venue
from pms.core.exceptions import KalshiStubError
from pms.core.models import OrderState, Portfolio, TradeDecision
from pms.core.venue_support import kalshi_stub_error


@dataclass(frozen=True)
class PaperActuator:
    orderbooks: Mapping[str, dict[str, Any]] = field(default_factory=dict)

    async def execute(
        self,
        decision: TradeDecision,
        portfolio: Portfolio | None = None,
    ) -> OrderState:
        if decision.venue == Venue.KALSHI.value:
            raise kalshi_stub_error("PaperActuator.execute")
        orderbook = self.orderbooks.get(decision.market_id, {"bids": [], "asks": []})
        fill_price = _best_fill_price(orderbook, decision)
        return _matched_order_state(decision, fill_price, "paper")


def _best_fill_price(orderbook: dict[str, Any], decision: TradeDecision) -> float:
    if decision.outcome == "NO":
        side_key = "bids" if decision.action == Side.BUY.value else "asks"
    else:
        side_key = "asks" if decision.action == Side.BUY.value else "bids"
    levels = orderbook.get(side_key)
    if not isinstance(levels, list) or not levels:
        raise InsufficientLiquidityError(f"{side_key} depth is empty")
    best = levels[0]
    if not isinstance(best, dict):
        raise InsufficientLiquidityError(f"{side_key} depth is invalid")
    available_size = float(cast(str | int | float, best.get("size", 0.0)))
    if available_size <= 0.0:
        raise InsufficientLiquidityError(f"{side_key} depth is empty")
    if available_size < decision.size:
        raise InsufficientLiquidityError(f"{side_key} depth is insufficient")
    best_price = float(cast(str | int | float, best["price"]))
    if decision.outcome == "NO":
        return 1.0 - best_price
    return best_price


def _matched_order_state(
    decision: TradeDecision,
    fill_price: float,
    order_id_prefix: str,
) -> OrderState:
    now = datetime.now(tz=UTC)
    return OrderState(
        order_id=f"{order_id_prefix}-{uuid4().hex}",
        decision_id=decision.decision_id,
        status=OrderStatus.MATCHED.value,
        market_id=decision.market_id,
        token_id=decision.token_id,
        venue=decision.venue,
        requested_size=decision.size,
        filled_size=decision.size,
        remaining_size=0.0,
        fill_price=fill_price,
        submitted_at=now,
        last_updated_at=now,
        raw_status="matched",
        strategy_id=decision.strategy_id,
        strategy_version_id=decision.strategy_version_id,
    )
