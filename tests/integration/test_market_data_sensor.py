from __future__ import annotations

import asyncio
import contextlib
import json
import os
from datetime import UTC, datetime
from typing import Any

import httpx
import pytest
from websockets.asyncio.server import serve

from pms.core.models import Market, Token
from pms.sensor.adapters.market_data import MarketDataSensor
from pms.storage.market_data_store import PostgresMarketDataStore


PMS_TEST_DATABASE_URL = os.environ.get("PMS_TEST_DATABASE_URL")

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.environ.get("PMS_RUN_INTEGRATION") != "1",
        reason="set PMS_RUN_INTEGRATION=1 to run PostgreSQL integration tests",
    ),
    pytest.mark.skipif(
        PMS_TEST_DATABASE_URL is None,
        reason="set PMS_TEST_DATABASE_URL to the compose-backed PostgreSQL URI",
    ),
]


async def _consume_until_cancel(sensor: MarketDataSensor) -> None:
    async for _ in sensor:
        await asyncio.sleep(0)


async def _seed_market_and_token(
    store: PostgresMarketDataStore,
    *,
    market_id: str,
    token_id: str,
) -> None:
    now = datetime.now(tz=UTC)
    await store.write_market(
        Market(
            condition_id=market_id,
            slug=f"market-{market_id}",
            question=f"Will {market_id} update?",
            venue="polymarket",
            resolves_at=now,
            created_at=now,
            last_seen_at=now,
        )
    )
    await store.write_token(
        Token(
            token_id=token_id,
            condition_id=market_id,
            outcome="YES",
        )
    )


@pytest.mark.asyncio(loop_scope="session")
async def test_market_data_sensor_persists_book_price_changes_and_trades_from_local_ws(
    pg_pool: Any,
    db_conn: Any,
) -> None:
    received_subscriptions: list[dict[str, Any]] = []

    async def handler(websocket: Any) -> None:
        received_subscriptions.append(json.loads(await websocket.recv()))
        await websocket.send(
            json.dumps(
                {
                    "event_type": "book",
                    "market": "m-local",
                    "asset_id": "asset-local",
                    "timestamp": "1757908892351",
                    "hash": "book-hash",
                    "bids": [{"price": "0.48", "size": "30"}],
                    "asks": [{"price": "0.52", "size": "25"}],
                    "last_trade_price": "0.50",
                }
            )
        )
        await websocket.send(
            json.dumps(
                {
                    "event_type": "price_change",
                    "market": "m-local",
                    "timestamp": "1757908892352",
                    "price_changes": [
                        {
                            "asset_id": "asset-local",
                            "price": "0.49",
                            "size": "12",
                            "side": "BUY",
                            "hash": "delta-hash",
                            "best_bid": "0.49",
                            "best_ask": "0.52",
                        }
                    ],
                }
            )
        )
        await websocket.send(
            json.dumps(
                {
                    "event_type": "last_trade_price",
                    "market": "m-local",
                    "asset_id": "asset-local",
                    "price": "0.51",
                    "side": "BUY",
                    "size": "4",
                    "fee_rate_bps": "0",
                    "timestamp": "1757908892353",
                }
            )
        )
        await websocket.wait_closed()

    async with serve(handler, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        store = PostgresMarketDataStore(pg_pool)
        await _seed_market_and_token(store, market_id="m-local", token_id="asset-local")
        sensor = MarketDataSensor(
            store=store,
            ws_url=f"ws://127.0.0.1:{port}",
            asset_ids=["asset-local"],
        )
        task = asyncio.create_task(_consume_until_cancel(sensor))
        try:
            async with asyncio.timeout(2.0):
                while True:
                    snapshot_count = await db_conn.fetchval(
                        "SELECT COUNT(*) FROM book_snapshots"
                    )
                    price_change_count = await db_conn.fetchval(
                        "SELECT COUNT(*) FROM price_changes"
                    )
                    trade_count = await db_conn.fetchval("SELECT COUNT(*) FROM trades")
                    if snapshot_count >= 1 and price_change_count >= 1 and trade_count >= 1:
                        break
                    await asyncio.sleep(0.05)
        finally:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
            await sensor.aclose()

    assert received_subscriptions == [
        {
            "assets_ids": ["asset-local"],
            "type": "market",
            "initial_dump": True,
            "level": 2,
        }
    ]


@pytest.mark.asyncio(loop_scope="session")
async def test_market_data_sensor_live_polymarket_writes_first_snapshot_within_five_seconds(
    pg_pool: Any,
    db_conn: Any,
) -> None:
    markets = httpx.get(
        "https://gamma-api.polymarket.com/markets",
        params={"limit": 1},
        timeout=20.0,
    ).json()
    market_row = markets[0]
    token_ids = json.loads(market_row["clobTokenIds"])
    store = PostgresMarketDataStore(pg_pool)
    await store.write_market(
        Market(
            condition_id=market_row["conditionId"],
            slug=market_row["slug"],
            question=market_row["question"],
            venue="polymarket",
            resolves_at=datetime.fromisoformat(
                f"{market_row['endDateIso']}T00:00:00+00:00"
                if len(market_row["endDateIso"]) == 10
                else str(market_row["endDateIso"]).replace("Z", "+00:00")
            ),
            created_at=datetime.fromisoformat(
                str(market_row["createdAt"]).replace("Z", "+00:00")
            ),
            last_seen_at=datetime.now(tz=UTC),
        )
    )
    await store.write_token(
        Token(
            token_id=token_ids[0],
            condition_id=market_row["conditionId"],
            outcome="YES",
        )
    )
    sensor = MarketDataSensor(
        store=store,
        asset_ids=[token_ids[0]],
    )
    task = asyncio.create_task(_consume_until_cancel(sensor))
    try:
        async with asyncio.timeout(5.0):
            while True:
                snapshot = await db_conn.fetchrow(
                    """
                    SELECT source, token_id
                    FROM book_snapshots
                    ORDER BY id DESC
                    LIMIT 1
                    """
                )
                if snapshot is not None:
                    assert snapshot["source"] == "subscribe"
                    assert snapshot["token_id"] == token_ids[0]
                    break
                await asyncio.sleep(0.05)
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        await sensor.aclose()
