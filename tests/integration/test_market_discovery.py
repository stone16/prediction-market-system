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


def _gamma_market(
    condition_id: str,
    *,
    venue: str = "polymarket",
    outcome_prices: object | None = None,
    last_trade_price: object | None = None,
    best_bid: object | None = None,
    best_ask: object | None = None,
    liquidity: object | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
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
    if outcome_prices is not None:
        payload["outcomePrices"] = outcome_prices
    if last_trade_price is not None:
        payload["lastTradePrice"] = last_trade_price
    if best_bid is not None:
        payload["bestBid"] = best_bid
    if best_ask is not None:
        payload["bestAsk"] = best_ask
    if liquidity is not None:
        payload["liquidity"] = liquidity
    return payload


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
async def test_discovery_poll_persists_price_fields(
    pg_pool: Any,
    db_conn: Any,
) -> None:
    payload = [
        _gamma_market(
            "pm-live-priced",
            outcome_prices=["0.62", "0.38"],
            last_trade_price="0.61",
            best_bid="0.59",
            best_ask="0.62",
            liquidity="2500.25",
        )
    ]

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

    row = await db_conn.fetchrow(
        """
        SELECT
            yes_price,
            no_price,
            best_bid,
            best_ask,
            last_trade_price,
            liquidity,
            spread_bps,
            price_updated_at
        FROM markets
        WHERE condition_id = 'pm-live-priced'
        """
    )

    assert row is not None
    assert float(row["yes_price"]) == pytest.approx(0.62)
    assert float(row["no_price"]) == pytest.approx(0.38)
    assert float(row["best_bid"]) == pytest.approx(0.59)
    assert float(row["best_ask"]) == pytest.approx(0.62)
    assert float(row["last_trade_price"]) == pytest.approx(0.61)
    assert float(row["liquidity"]) == pytest.approx(2500.25)
    assert row["spread_bps"] == 300
    assert row["price_updated_at"] is not None


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
