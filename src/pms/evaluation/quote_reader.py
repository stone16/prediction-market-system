from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from pms.core.models import BookLevel, BookSnapshot, BookSummary


class TokenBookStore(Protocol):
    async def read_latest_snapshot(
        self,
        market_id: str,
        token_id: str,
    ) -> BookSnapshot | None: ...

    async def read_levels_for_snapshot(self, snapshot_id: int) -> list[BookLevel]: ...


@dataclass(frozen=True)
class TokenBookQuoteReader:
    store: TokenBookStore

    async def latest_book_summary(
        self,
        market_id: str,
        token_id: str | None,
    ) -> BookSummary | None:
        if token_id is None:
            return None
        snapshot = await self.store.read_latest_snapshot(market_id, token_id)
        if snapshot is None:
            return None
        levels = await self.store.read_levels_for_snapshot(snapshot.id)
        bids = [(level.price, level.size) for level in levels if level.side == "BUY"]
        asks = [(level.price, level.size) for level in levels if level.side == "SELL"]
        if not bids or not asks:
            return None

        best_bid = max(price for price, _ in bids)
        best_ask = min(price for price, _ in asks)
        midpoint = (best_bid + best_ask) / 2.0
        if midpoint <= 0.0:
            return None
        top_bid_depth = sum(price * size for price, size in bids if price == best_bid)
        top_ask_depth = sum(price * size for price, size in asks if price == best_ask)
        return BookSummary(
            best_bid=best_bid,
            best_ask=best_ask,
            spread_bps=((best_ask - best_bid) / midpoint) * 10_000.0,
            depth_usdc=top_bid_depth + top_ask_depth,
            timestamp=snapshot.ts,
        )
