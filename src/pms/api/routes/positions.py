from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol

from pydantic import BaseModel

from pms.core.models import Position


StoredPositionRow = Position


class PositionsReader(Protocol):
    async def read_positions(self) -> Sequence[StoredPositionRow]: ...


class PositionRow(BaseModel):
    market_id: str
    token_id: str | None
    venue: str
    side: str
    shares_held: float
    avg_entry_price: float
    unrealized_pnl: float
    locked_usdc: float
    mark_source: str | None = None
    mark_age_seconds: float | None = None
    strategy_id: str
    strategy_version_id: str


class PositionsResponse(BaseModel):
    positions: list[PositionRow]


async def list_positions(store: PositionsReader) -> PositionsResponse:
    positions = await store.read_positions()
    return PositionsResponse(
        positions=[
            PositionRow(
                market_id=position.market_id,
                token_id=position.token_id,
                venue=position.venue,
                side=position.side,
                shares_held=position.shares_held,
                avg_entry_price=position.avg_entry_price,
                unrealized_pnl=position.unrealized_pnl,
                locked_usdc=position.locked_usdc,
                mark_source=position.mark_source,
                mark_age_seconds=position.mark_age_seconds,
                strategy_id=position.strategy_id,
                strategy_version_id=position.strategy_version_id,
            )
            for position in positions
        ]
    )
