from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from typing import Protocol

from pydantic import BaseModel

from pms.storage.market_data_store import MarketCatalogRow


StoredMarketRow = MarketCatalogRow


class MarketsReader(Protocol):
    async def read_markets(
        self,
        *,
        limit: int,
        offset: int,
        now: datetime | None = None,
    ) -> tuple[Sequence[StoredMarketRow], int]: ...


class MarketRow(BaseModel):
    market_id: str
    question: str
    venue: str
    volume_24h: float | None
    updated_at: str
    yes_token_id: str | None
    no_token_id: str | None
    subscribed: bool


class MarketsListResponse(BaseModel):
    markets: list[MarketRow]
    limit: int
    offset: int
    total: int


async def list_markets(
    store: MarketsReader,
    *,
    current_asset_ids: frozenset[str],
    limit: int,
    offset: int,
) -> MarketsListResponse:
    rows, total = await store.read_markets(limit=limit, offset=offset)
    return MarketsListResponse(
        markets=[
            MarketRow(
                market_id=row.market_id,
                question=row.question,
                venue=row.venue,
                volume_24h=row.volume_24h,
                updated_at=row.updated_at.isoformat(),
                yes_token_id=row.yes_token_id,
                no_token_id=row.no_token_id,
                subscribed=_is_subscribed(row, current_asset_ids),
            )
            for row in rows
        ],
        limit=limit,
        offset=offset,
        total=total,
    )


def _is_subscribed(row: StoredMarketRow, current_asset_ids: frozenset[str]) -> bool:
    token_ids = {
        token_id
        for token_id in (row.yes_token_id, row.no_token_id)
        if token_id is not None
    }
    return not token_ids.isdisjoint(current_asset_ids)
