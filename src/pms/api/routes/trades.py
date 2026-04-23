from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from typing import Protocol

from pydantic import BaseModel

from pms.storage.fill_store import StoredTradeRow


class TradesReader(Protocol):
    async def read_trades(self, *, limit: int) -> Sequence[StoredTradeRow]: ...


class TradeRow(BaseModel):
    trade_id: str
    fill_id: str
    order_id: str
    decision_id: str
    market_id: str
    question: str
    token_id: str | None
    venue: str
    side: str
    fill_price: float
    fill_notional_usdc: float
    fill_quantity: float
    executed_at: datetime
    filled_at: datetime
    status: str
    strategy_id: str
    strategy_version_id: str


class TradesResponse(BaseModel):
    trades: list[TradeRow]
    limit: int


async def list_trades(
    store: TradesReader,
    *,
    limit: int,
) -> TradesResponse:
    rows = await store.read_trades(limit=limit)
    return TradesResponse(
        trades=[
            TradeRow(
                trade_id=row.trade_id,
                fill_id=row.fill_id,
                order_id=row.order_id,
                decision_id=row.decision_id,
                market_id=row.market_id,
                question=row.question,
                token_id=row.token_id,
                venue=row.venue,
                side=row.side,
                fill_price=row.fill_price,
                fill_notional_usdc=row.fill_notional_usdc,
                fill_quantity=row.fill_quantity,
                executed_at=row.executed_at,
                filled_at=row.filled_at,
                status=row.status,
                strategy_id=row.strategy_id,
                strategy_version_id=row.strategy_version_id,
            )
            for row in rows
        ],
        limit=limit,
    )
