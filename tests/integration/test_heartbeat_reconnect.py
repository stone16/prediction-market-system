from __future__ import annotations

import asyncio
import contextlib
import json
import os
from datetime import UTC, datetime
from typing import Any

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
            question=f"Will {market_id} reconnect?",
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
async def test_market_data_sensor_persists_reconnect_snapshot_after_server_close(
    pg_pool: Any,
    db_conn: Any,
) -> None:
    received_subscriptions: list[dict[str, Any]] = []
    connection_count = 0

    async def handler(websocket: Any) -> None:
        nonlocal connection_count
        connection_count += 1
        received_subscriptions.append(json.loads(await websocket.recv()))
        if connection_count == 1:
            await websocket.send(
                json.dumps(
                    {
                        "event_type": "book",
                        "market": "m-heartbeat",
                        "asset_id": "asset-heartbeat",
                        "timestamp": "1757908892351",
                        "hash": "book-subscribe",
                        "bids": [{"price": "0.48", "size": "30"}],
                        "asks": [{"price": "0.52", "size": "25"}],
                        "last_trade_price": "0.50",
                    }
                )
            )
            await websocket.close()
            return

        await websocket.send(
            json.dumps(
                {
                    "event_type": "book",
                    "market": "m-heartbeat",
                    "asset_id": "asset-heartbeat",
                    "timestamp": "1757908892451",
                    "hash": "book-reconnect",
                    "bids": [{"price": "0.41", "size": "18"}],
                    "asks": [{"price": "0.59", "size": "12"}],
                    "last_trade_price": "0.42",
                }
            )
        )
        await websocket.wait_closed()

    async with serve(handler, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        store = PostgresMarketDataStore(pg_pool)
        await _seed_market_and_token(
            store,
            market_id="m-heartbeat",
            token_id="asset-heartbeat",
        )
        sensor = MarketDataSensor(
            store=store,
            ws_url=f"ws://127.0.0.1:{port}",
            asset_ids=["asset-heartbeat"],
        )
        sensor._heartbeat_interval_s = 0.2
        sensor._pong_timeout_s = 0.2
        task = asyncio.create_task(_consume_until_cancel(sensor))
        try:
            async with asyncio.timeout(2.0):
                while True:
                    reconnect_count = await db_conn.fetchval(
                        """
                        SELECT COUNT(*)
                        FROM book_snapshots
                        WHERE source = 'reconnect'
                        """
                    )
                    if reconnect_count >= 1:
                        break
                    await asyncio.sleep(0.05)
        finally:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
            await sensor.aclose()

    rows = await db_conn.fetch(
        """
        SELECT source, hash
        FROM book_snapshots
        ORDER BY id
        """
    )
    assert [(row["source"], row["hash"]) for row in rows] == [
        ("subscribe", "book-subscribe"),
        ("reconnect", "book-reconnect"),
    ]
    assert connection_count >= 2
    assert received_subscriptions[:2] == [
        {
            "assets_ids": ["asset-heartbeat"],
            "type": "market",
            "initial_dump": True,
            "level": 2,
        },
        {
            "assets_ids": ["asset-heartbeat"],
            "type": "market",
            "initial_dump": True,
            "level": 2,
        },
    ]
