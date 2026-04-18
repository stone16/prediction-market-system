from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta

import asyncpg

from pms.core.enums import MarketStatus
from pms.core.models import BookLevel, BookSnapshot, Market, MarketSignal, Token
from pms.factors.definitions import REGISTERED
from pms.factors.service import FactorService
from pms.storage.market_data_store import PostgresMarketDataStore
from tests.support.strategy_catalog import seed_factor_catalog


MARKET_ID = "factor-panel-e2e"
TOKEN_ID = "factor-panel-e2e-yes"


class EmptySignalStream:
    def __aiter__(self) -> AsyncIterator[MarketSignal]:
        return self._iterate()

    async def _iterate(self) -> AsyncIterator[MarketSignal]:
        return
        yield  # pragma: no cover


def _signal(
    *,
    ts: datetime,
    bids: list[dict[str, float]],
    asks: list[dict[str, float]],
) -> MarketSignal:
    return MarketSignal(
        market_id=MARKET_ID,
        token_id=TOKEN_ID,
        venue="polymarket",
        title="Will the factors page render seeded rows?",
        yes_price=0.47,
        volume_24h=1000.0,
        resolves_at=datetime(2026, 4, 30, tzinfo=UTC),
        orderbook={"bids": bids, "asks": asks},
        external_signal={},
        fetched_at=ts,
        market_status=MarketStatus.OPEN.value,
    )


async def _seed_market_data(store: PostgresMarketDataStore) -> None:
    base_ts = datetime(2026, 4, 18, 12, 0, tzinfo=UTC)
    await store.write_market(
        Market(
            condition_id=MARKET_ID,
            slug=MARKET_ID,
            question="Will the factors page render seeded rows?",
            venue="polymarket",
            resolves_at=datetime(2026, 4, 30, tzinfo=UTC),
            created_at=base_ts,
            last_seen_at=base_ts + timedelta(minutes=1),
        )
    )
    await store.write_token(
        Token(
            token_id=TOKEN_ID,
            condition_id=MARKET_ID,
            outcome="YES",
        )
    )

    snapshots = (
        (
            base_ts,
            [
                BookLevel(snapshot_id=0, market_id=MARKET_ID, side="BUY", price=0.46, size=70.0),
                BookLevel(snapshot_id=0, market_id=MARKET_ID, side="SELL", price=0.49, size=30.0),
            ],
        ),
        (
            base_ts + timedelta(minutes=1),
            [
                BookLevel(snapshot_id=0, market_id=MARKET_ID, side="BUY", price=0.47, size=90.0),
                BookLevel(snapshot_id=0, market_id=MARKET_ID, side="SELL", price=0.50, size=20.0),
            ],
        ),
    )

    for ts, levels in snapshots:
        await store.write_book_snapshot(
            BookSnapshot(
                id=0,
                market_id=MARKET_ID,
                token_id=TOKEN_ID,
                ts=ts,
                hash=f"{MARKET_ID}-{ts.isoformat()}",
                source="subscribe",
            ),
            levels,
        )


async def main() -> None:
    database_url = os.environ.get("PMS_TEST_DATABASE_URL") or os.environ.get("DATABASE_URL")
    if not database_url:
        msg = "PMS_TEST_DATABASE_URL or DATABASE_URL must be set"
        raise RuntimeError(msg)

    pool = await asyncpg.create_pool(dsn=database_url, min_size=1, max_size=2)
    try:
        async with pool.acquire() as connection:
            await seed_factor_catalog(connection, factor_ids=("orderbook_imbalance",))

        store = PostgresMarketDataStore(pool)
        await _seed_market_data(store)

        service = FactorService(
            pool=pool,
            store=store,
            cadence_s=0.1,
            factors=REGISTERED,
            signal_stream=EmptySignalStream(),
        )
        await service.compute_once(
            [
                _signal(
                    ts=datetime(2026, 4, 18, 12, 0, tzinfo=UTC),
                    bids=[{"price": 0.46, "size": 70.0}],
                    asks=[{"price": 0.49, "size": 30.0}],
                ),
                _signal(
                    ts=datetime(2026, 4, 18, 12, 1, tzinfo=UTC),
                    bids=[{"price": 0.47, "size": 90.0}],
                    asks=[{"price": 0.50, "size": 20.0}],
                ),
            ]
        )
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
