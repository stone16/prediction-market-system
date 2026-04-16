from __future__ import annotations

import asyncio
import json
import os
from contextlib import suppress
from datetime import UTC, datetime
from typing import Any

import httpx
import pytest
from websockets.asyncio.server import serve

from pms.core.models import Market, Token
from pms.sensor.adapters.market_data import MarketDataSensor
from pms.sensor.adapters.market_discovery import MarketDiscoverySensor
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


async def _consume(sensor: Any) -> None:
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
            question=f"Will {market_id} keep writing?",
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
async def test_market_data_sensor_continues_while_discovery_poll_is_blocked(
    pg_pool: Any,
    db_conn: Any,
) -> None:
    discovery_block = asyncio.Event()
    received_subscriptions: list[dict[str, Any]] = []

    async def discovery_handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/markets"
        await discovery_block.wait()
        return httpx.Response(200, json=[])

    async def ws_handler(websocket: Any) -> None:
        received_subscriptions.append(json.loads(await websocket.recv()))
        for offset in range(6):
            await websocket.send(
                json.dumps(
                    {
                        "event_type": "book",
                        "market": "m-cadence",
                        "asset_id": "asset-cadence",
                        "timestamp": str(1757908892351 + offset),
                        "hash": f"book-hash-{offset}",
                        "bids": [{"price": "0.48", "size": str(30 + offset)}],
                        "asks": [{"price": "0.52", "size": "25"}],
                        "last_trade_price": "0.50",
                    }
                )
            )
            await asyncio.sleep(1.0)

    async with serve(ws_handler, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        market_data_store = PostgresMarketDataStore(pg_pool)
        await _seed_market_and_token(
            market_data_store,
            market_id="m-cadence",
            token_id="asset-cadence",
        )
        discovery = MarketDiscoverySensor(
            store=PostgresMarketDataStore(pg_pool),
            http_client=httpx.AsyncClient(
                transport=httpx.MockTransport(discovery_handler),
                base_url="https://gamma.example.test",
            ),
            poll_interval_s=60.0,
        )
        market_data = MarketDataSensor(
            store=market_data_store,
            ws_url=f"ws://127.0.0.1:{port}",
            asset_ids=["asset-cadence"],
        )
        discovery_task = asyncio.create_task(_consume(discovery))
        market_data_task = asyncio.create_task(_consume(market_data))
        try:
            await asyncio.sleep(5.2)
            snapshot_count = await db_conn.fetchval("SELECT COUNT(*) FROM book_snapshots")
        finally:
            discovery_task.cancel()
            market_data_task.cancel()
            with suppress(asyncio.CancelledError):
                await discovery_task
            with suppress(asyncio.CancelledError):
                await market_data_task
            await discovery.aclose()
            await market_data.aclose()
            discovery_block.set()

    assert received_subscriptions == [
        {
            "assets_ids": ["asset-cadence"],
            "type": "market",
            "initial_dump": True,
            "level": 2,
        }
    ]
    assert snapshot_count >= 5
