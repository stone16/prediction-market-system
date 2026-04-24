from __future__ import annotations

import json
import os
from datetime import UTC, datetime, timedelta
from typing import Any, ClassVar

import asyncpg
import httpx
import pytest

from pms.metrics import (
    MARKETS_SNAPSHOT_LAG_SECONDS_MAX_METRIC,
    SENSOR_DISCOVERY_SNAPSHOTS_WRITTEN_TOTAL_METRIC,
    get_metric,
    set_metric,
)
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


def _priced_gamma_markets(prefix: str, count: int) -> list[dict[str, Any]]:
    return [
        _gamma_market(
            f"{prefix}-{index}",
            outcome_prices=["0.62", "0.38"],
            last_trade_price="0.61",
            best_bid="0.59",
            best_ask="0.62",
            liquidity="2500.25",
        )
        for index in range(count)
    ]


def _freeze_discovery_now(monkeypatch: pytest.MonkeyPatch, now: datetime) -> None:
    class FixedDateTime(datetime):
        fixed_now: ClassVar[datetime]

        @classmethod
        def now(cls, tz: Any = None) -> datetime:
            if tz is None:
                return cls.fixed_now.replace(tzinfo=None)
            return cls.fixed_now.astimezone(tz)

    FixedDateTime.fixed_now = now
    monkeypatch.setattr("pms.sensor.adapters.market_discovery.datetime", FixedDateTime)


class SnapshotFailingStore(PostgresMarketDataStore):
    def __init__(self, pool: asyncpg.Pool, *, failing_condition_id: str) -> None:
        super().__init__(pool)
        self._failing_condition_id = failing_condition_id

    async def write_price_snapshot(
        self,
        *,
        condition_id: str,
        snapshot_at: datetime,
        yes_price: float | None,
        no_price: float | None,
        best_bid: float | None,
        best_ask: float | None,
        last_trade_price: float | None,
        liquidity: float | None,
        volume_24h: float | None,
    ) -> None:
        if condition_id == self._failing_condition_id:
            raise asyncpg.PostgresError("simulated snapshot failure")
        await super().write_price_snapshot(
            condition_id=condition_id,
            snapshot_at=snapshot_at,
            yes_price=yes_price,
            no_price=no_price,
            best_bid=best_bid,
            best_ask=best_ask,
            last_trade_price=last_trade_price,
            liquidity=liquidity,
            volume_24h=volume_24h,
        )


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
async def test_discovery_poll_writes_one_snapshot_per_market(
    pg_pool: Any,
    db_conn: Any,
) -> None:
    payload = _priced_gamma_markets("pm-snapshot", 3)
    set_metric(SENSOR_DISCOVERY_SNAPSHOTS_WRITTEN_TOTAL_METRIC, 0.0)

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

    snapshot_count = await db_conn.fetchval(
        """
        SELECT COUNT(*)
        FROM market_price_snapshots
        WHERE condition_id LIKE 'pm-snapshot-%'
        """
    )

    assert snapshot_count == 3
    assert get_metric(SENSOR_DISCOVERY_SNAPSHOTS_WRITTEN_TOTAL_METRIC) == 3.0


@pytest.mark.asyncio(loop_scope="session")
async def test_discovery_poll_idempotent_on_duplicate_timestamp(
    pg_pool: Any,
    db_conn: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    snapshot_at = datetime(2026, 4, 24, 10, 0, tzinfo=UTC)
    _freeze_discovery_now(monkeypatch, snapshot_at)
    payload = _priced_gamma_markets("pm-duplicate-snapshot", 3)

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
    await sensor.poll_once()
    await sensor.aclose()

    snapshot_count = await db_conn.fetchval(
        """
        SELECT COUNT(*)
        FROM market_price_snapshots
        WHERE condition_id LIKE 'pm-duplicate-snapshot-%'
        """
    )

    assert snapshot_count == 3


@pytest.mark.asyncio(loop_scope="session")
async def test_discovery_poll_continues_after_snapshot_write_failure(
    pg_pool: asyncpg.Pool,
    db_conn: asyncpg.Connection,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    snapshot_at = datetime(2026, 4, 24, 11, 0, tzinfo=UTC)
    previous_snapshot_at = snapshot_at - timedelta(seconds=120)
    _freeze_discovery_now(monkeypatch, snapshot_at)
    failing_condition_id = "pm-snapshot-failure-0"
    payload = _priced_gamma_markets("pm-snapshot-failure", 3)
    set_metric(SENSOR_DISCOVERY_SNAPSHOTS_WRITTEN_TOTAL_METRIC, 0.0)

    await db_conn.execute(
        """
        INSERT INTO markets (
            condition_id,
            slug,
            question,
            venue,
            created_at,
            last_seen_at,
            price_updated_at
        ) VALUES (
            $1,
            $1,
            'Will the snapshot failure preserve lag?',
            'polymarket',
            $2,
            $2,
            $2
        )
        """,
        failing_condition_id,
        previous_snapshot_at,
    )
    await db_conn.execute(
        """
        INSERT INTO market_price_snapshots (
            condition_id,
            snapshot_at,
            yes_price,
            no_price,
            best_bid,
            best_ask,
            last_trade_price,
            liquidity,
            volume_24h
        ) VALUES (
            $1, $2, 0.50, 0.50, 0.49, 0.51, 0.50, 100.0, 10.0
        )
        """,
        failing_condition_id,
        previous_snapshot_at,
    )

    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/markets"
        return httpx.Response(200, json=payload)

    sensor = MarketDiscoverySensor(
        store=SnapshotFailingStore(
            pg_pool,
            failing_condition_id=failing_condition_id,
        ),
        http_client=httpx.AsyncClient(
            transport=httpx.MockTransport(handler),
            base_url="https://gamma.example.test",
        ),
        poll_interval_s=60.0,
    )

    with caplog.at_level("WARNING"):
        await sensor.poll_once()
    await sensor.aclose()

    successful_snapshot_count = await db_conn.fetchval(
        """
        SELECT COUNT(*)
        FROM market_price_snapshots
        WHERE condition_id IN ('pm-snapshot-failure-1', 'pm-snapshot-failure-2')
          AND snapshot_at = $1
        """,
        snapshot_at,
    )
    failed_current_snapshot_count = await db_conn.fetchval(
        """
        SELECT COUNT(*)
        FROM market_price_snapshots
        WHERE condition_id = $1
          AND snapshot_at = $2
        """,
        failing_condition_id,
        snapshot_at,
    )

    assert successful_snapshot_count == 2
    assert failed_current_snapshot_count == 0
    assert get_metric(SENSOR_DISCOVERY_SNAPSHOTS_WRITTEN_TOTAL_METRIC) == 2.0
    assert get_metric(MARKETS_SNAPSHOT_LAG_SECONDS_MAX_METRIC) == pytest.approx(120.0)
    assert "write_price_snapshot failed" in caplog.text


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
