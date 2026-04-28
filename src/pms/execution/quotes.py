"""Quote protocol and immutable quote snapshots for execution planning."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from pms.core.models import BookSide, Venue
from pms.strategies.intents import TradeIntent


@dataclass(frozen=True, slots=True)
class ExecutableQuote:
    market_id: str
    token_id: str | None
    venue: Venue
    side: BookSide
    best_price: float
    available_size: float
    book_timestamp: datetime
    quote_hash: str
    min_order_size_usdc: float
    tick_size: float
    fee_bps: int = 0

    @property
    def executable_notional_usdc(self) -> float:
        return self.best_price * self.available_size


class QuoteProvider(Protocol):
    async def quote_for_intent(self, intent: TradeIntent) -> ExecutableQuote | None: ...
