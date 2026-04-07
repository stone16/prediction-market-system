"""Market-related domain models.

All models are frozen dataclasses (immutable). Decimal is used for any
monetary or price quantity to avoid binary float rounding artifacts.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any


@dataclass(frozen=True)
class Outcome:
    """A single outcome of a prediction market (e.g. YES/NO leg)."""

    outcome_id: str
    title: str
    price: Decimal


@dataclass(frozen=True)
class Market:
    """Normalized prediction market across all platforms.

    The ``raw`` field preserves the original platform response so downstream
    code can recover platform-specific fields without re-fetching.
    """

    platform: str
    market_id: str
    title: str
    description: str
    outcomes: list[Outcome]
    volume: Decimal
    end_date: datetime | None
    category: str
    url: str
    status: str
    raw: dict[str, Any]


@dataclass(frozen=True)
class PriceLevel:
    """A single level in an order book (price + aggregated size)."""

    price: Decimal
    size: Decimal


@dataclass(frozen=True)
class OrderBook:
    """Snapshot of bids and asks for a market at a point in time."""

    platform: str
    market_id: str
    bids: list[PriceLevel]
    asks: list[PriceLevel]
    timestamp: datetime


@dataclass(frozen=True)
class PriceUpdate:
    """A streaming or polled price update for a single outcome."""

    platform: str
    market_id: str
    outcome_id: str
    bid: Decimal
    ask: Decimal
    last: Decimal
    timestamp: datetime
