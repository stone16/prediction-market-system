from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from typing import Any

import httpx
import pytest

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


def _gamma_market(condition_id: str, *, venue: str = "polymarket") -> dict[str, Any]:
    return {
        "id": condition_id,
        "conditionId": condition_id,
        "venue": venue,
        "slug": f"market-{condition_id}",
        "question": f"Will {condition_id} settle?",
        "endDateIso": "2026-07-31",
        "createdAt": "2025-05-02T15:03:10.397014Z",
        "clobTokenIds": json.dumps([f"yes-{condition_id}", f"no-{condition_id}"]),
        "outcomes": json.dumps(["Yes", "No"]),
        "active": True,
        "closed": False,
    }


@pytest.mark.asyncio(loop_scope="session")
async def test_market_discovery_poll_once_persists_markets_and_tokens(
    pg_pool: Any,
    db_conn: Any,
) -> None:
    payload = [_gamma_market(f"pm-live-{index}") for index in range(10)]

    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/markets"
        return httpx.Response(200, json=payload)

    sensor = MarketDiscoverySensor(
        store=PostgresMarketDataStore(pg_pool),
        http_client=httpx.AsyncClient(
            transport=httpx.MockTransport(handler),
            base_url="https://gamma.example.test",
        ),
        poll_interval_s=60.0,
    )

    await sensor.poll_once()
    await sensor.aclose()

    market_count = await db_conn.fetchval("SELECT COUNT(*) FROM markets")
    token_count = await db_conn.fetchval("SELECT COUNT(*) FROM tokens")

    assert market_count == 10
    assert token_count == 20


@pytest.mark.asyncio(loop_scope="session")
async def test_market_discovery_live_gamma_poll_persists_valid_rows(
    pg_pool: Any,
    db_conn: Any,
) -> None:
    sensor = MarketDiscoverySensor(
        store=PostgresMarketDataStore(pg_pool),
        http_client=httpx.AsyncClient(base_url="https://gamma-api.polymarket.com"),
        poll_interval_s=60.0,
    )

    await sensor.poll_once()
    await sensor.aclose()

    market_count = await db_conn.fetchval("SELECT COUNT(*) FROM markets")
    token_count = await db_conn.fetchval("SELECT COUNT(*) FROM tokens")
    sample_row = await db_conn.fetchrow(
        """
        SELECT condition_id, slug, question, venue, created_at, last_seen_at
        FROM markets
        ORDER BY last_seen_at DESC, condition_id ASC
        LIMIT 1
        """
    )

    assert market_count >= 1
    assert token_count >= 2
    assert sample_row is not None
    assert sample_row["venue"] == "polymarket"
    assert sample_row["created_at"] <= datetime.now(tz=UTC)
    assert sample_row["last_seen_at"] <= datetime.now(tz=UTC)


@pytest.mark.asyncio(loop_scope="session")
async def test_market_discovery_rejects_kalshi_rows(
    pg_pool: Any,
    db_conn: Any,
) -> None:
    from pms.core.exceptions import KalshiStubError

    payload = [_gamma_market("ka-stub-row", venue="kalshi")]

    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/markets"
        return httpx.Response(200, json=payload)

    sensor = MarketDiscoverySensor(
        store=PostgresMarketDataStore(pg_pool),
        http_client=httpx.AsyncClient(
            transport=httpx.MockTransport(handler),
            base_url="https://gamma.example.test",
        ),
        poll_interval_s=60.0,
    )

    with pytest.raises(
        KalshiStubError,
        match="Kalshi adapter is not implemented in v1",
    ):
        await sensor.poll_once()
    await sensor.aclose()

    market_count = await db_conn.fetchval(
        "SELECT COUNT(*) FROM markets WHERE condition_id = 'ka-stub-row'"
    )
    token_count = await db_conn.fetchval(
        "SELECT COUNT(*) FROM tokens WHERE condition_id = 'ka-stub-row'"
    )

    assert market_count == 0
    assert token_count == 0
