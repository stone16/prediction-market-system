from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
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
        orderbook = _orderbook_for_decision(self.orderbooks, decision)
        fill_price, filled_quantity = _vwap_fill(orderbook, decision)
        return _matched_order_state(decision, fill_price, filled_quantity, "paper")


def _orderbook_for_decision(
    orderbooks: Mapping[str, dict[str, Any]],
    decision: TradeDecision,
) -> dict[str, Any]:
    if decision.token_id is not None and decision.token_id in orderbooks:
        return orderbooks[decision.token_id]
    if (
        decision.outcome == "YES" or decision.token_id is None
    ) and decision.market_id in orderbooks:
        return orderbooks[decision.market_id]
    raise InsufficientLiquidityError(
        f"missing paper orderbook for token={decision.token_id} market={decision.market_id}"
    )


def _vwap_fill(orderbook: dict[str, Any], decision: TradeDecision) -> tuple[float, float]:
    requested_notional_usdc = _positive_decimal(
        decision.notional_usdc,
        "decision notional must be positive",
    )
    limit_price = _finite_decimal(
        decision.limit_price,
        "decision limit price is invalid",
    )
    action = decision.action or decision.side
    is_buy = action == Side.BUY.value
    side_key = "asks" if is_buy else "bids"
    raw_levels = orderbook.get(side_key)
    if not isinstance(raw_levels, list) or not raw_levels:
        raise InsufficientLiquidityError(f"{side_key} depth is empty")

    levels: list[tuple[Decimal, Decimal]] = []
    for raw in raw_levels:
        if not isinstance(raw, dict):
            raise InsufficientLiquidityError(f"{side_key} depth is invalid")
        try:
            price = _finite_decimal(raw["price"], f"{side_key} depth is invalid")
            size = _finite_decimal(raw.get("size", 0.0), f"{side_key} depth is invalid")
        except (KeyError, TypeError) as exc:
            raise InsufficientLiquidityError(f"{side_key} depth is invalid") from exc
        if price <= 0 or size <= 0:
            continue
        levels.append((price, size))

    levels.sort(key=lambda item: item[0], reverse=not is_buy)
    remaining = requested_notional_usdc
    filled_notional = Decimal("0")
    filled_quantity = Decimal("0")
    epsilon = Decimal("1e-9")
    for price, size in levels:
        if is_buy and price > limit_price:
            break
        if not is_buy and price < limit_price:
            break
        take_notional = min(remaining, price * size)
        filled_notional += take_notional
        filled_quantity += take_notional / price
        remaining -= take_notional
        if remaining <= epsilon:
            break

    if remaining > epsilon or filled_quantity <= 0:
        raise InsufficientLiquidityError(
            f"{side_key} executable depth is insufficient at limit={decision.limit_price}"
        )
    return float(filled_notional / filled_quantity), float(filled_quantity)


def _matched_order_state(
    decision: TradeDecision,
    fill_price: float,
    filled_quantity: float,
    order_id_prefix: str,
) -> OrderState:
    now = datetime.now(tz=UTC)
    filled_notional_usdc = _decision_notional_usdc(decision)
    return OrderState(
        order_id=f"{order_id_prefix}-{uuid4().hex}",
        decision_id=decision.decision_id,
        status=OrderStatus.MATCHED.value,
        market_id=decision.market_id,
        token_id=decision.token_id,
        venue=decision.venue,
        requested_notional_usdc=filled_notional_usdc,
        filled_notional_usdc=filled_notional_usdc,
        remaining_notional_usdc=0.0,
        fill_price=fill_price,
        submitted_at=now,
        last_updated_at=now,
        raw_status="matched",
        strategy_id=decision.strategy_id,
        strategy_version_id=decision.strategy_version_id,
        filled_quantity=filled_quantity,
        action=decision.action,
        outcome=decision.outcome,
        time_in_force=decision.time_in_force.value,
        intent_key=decision.intent_key,
    )


def _decision_notional_usdc(decision: TradeDecision) -> float:
    notional_usdc = float(cast(str | int | float, decision.notional_usdc))
    if notional_usdc <= 0.0:
        raise InsufficientLiquidityError("decision notional must be positive")
    return notional_usdc


def _finite_decimal(value: object, message: str) -> Decimal:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise InsufficientLiquidityError(message) from exc
    if not parsed.is_finite():
        raise InsufficientLiquidityError(message)
    return parsed


def _positive_decimal(value: object, message: str) -> Decimal:
    parsed = _finite_decimal(value, message)
    if parsed <= 0:
        raise InsufficientLiquidityError(message)
    return parsed
