"""Order, OrderResult, and Position models."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Literal

OrderSide = Literal["buy", "sell"]
OrderType = Literal["limit", "market"]
OrderStatus = Literal["filled", "partial", "rejected", "error"]


@dataclass(frozen=True)
class Order:
    """A trading order targeting a specific market outcome."""

    order_id: str
    platform: str
    market_id: str
    outcome_id: str
    side: OrderSide
    price: Decimal
    size: Decimal
    order_type: OrderType


@dataclass(frozen=True)
class OrderResult:
    """Result of an order submission attempt.

    The ``raw`` field preserves the original platform response so callers can
    recover platform-specific status fields if needed.
    """

    order_id: str
    status: OrderStatus
    filled_size: Decimal
    filled_price: Decimal
    message: str
    raw: dict[str, Any]


@dataclass(frozen=True)
class Position:
    """Current position held in a market outcome."""

    platform: str
    market_id: str
    outcome_id: str
    size: Decimal
    avg_entry_price: Decimal
    unrealized_pnl: Decimal
