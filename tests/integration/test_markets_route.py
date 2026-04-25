from __future__ import annotations

import asyncio
import os
from datetime import UTC, datetime, timedelta
from typing import Any, cast

import asyncpg
import httpx
import pytest

from pms.api.app import create_app
from pms.config import DatabaseSettings, PMSSettings
from pms.core.enums import RunMode
from pms.core.models import Market, Token
from pms.market_selection.subscription_controller import SensorSubscriptionController
from pms.runner import Runner
from pms.storage.market_data_store import PostgresMarketDataStore


PMS_TEST_DATABASE_URL = os.environ.get("PMS_TEST_DATABASE_URL")
ACTIVE_MARKET_NOW = datetime(2035, 4, 23, 12, 0, tzinfo=UTC)
EXPIRED_MARKET_AT = datetime(2020, 1, 1, 12, 0, tzinfo=UTC)

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


class _SubscriptionSink:
    async def update_subscription(self, asset_ids: list[str]) -> None:
        del asset_ids
        return None


def _settings() -> PMSSettings:
    return PMSSettings(
        mode=RunMode.LIVE,
        auto_migrate_default_v2=False,
        database=DatabaseSettings(dsn=cast(str, PMS_TEST_DATABASE_URL)),
    )


async def _seed_market(
    store: PostgresMarketDataStore,
    *,
    market_id: str,
    question: str,
    resolves_at: datetime | None,
    created_at: datetime,
    updated_at: datetime,
    volume_24h: float,
    yes_price: float | None = None,
    no_price: float | None = None,
    best_bid: float | None = None,
    best_ask: float | None = None,
    last_trade_price: float | None = None,
    liquidity: float | None = None,
    spread_bps: int | None = None,
    price_updated_at: datetime | None = None,
) -> tuple[str, str]:
    await store.write_market(
        Market(
            condition_id=market_id,
            slug=f"slug-{market_id}",
            question=question,
            venue="polymarket",
            resolves_at=resolves_at,
            created_at=created_at,
            last_seen_at=updated_at,
            volume_24h=volume_24h,
            yes_price=yes_price,
            no_price=no_price,
            best_bid=best_bid,
            best_ask=best_ask,
            last_trade_price=last_trade_price,
            liquidity=liquidity,
            spread_bps=spread_bps,
            price_updated_at=price_updated_at,
        )
    )
    yes_token_id = f"{market_id}-yes"
    no_token_id = f"{market_id}-no"
    await store.write_token(
        Token(token_id=yes_token_id, condition_id=market_id, outcome="YES")
    )
    await store.write_token(
        Token(token_id=no_token_id, condition_id=market_id, outcome="NO")
    )
    return yes_token_id, no_token_id


def _client(
    pg_pool: asyncpg.Pool,
    *,
    current_asset_ids: frozenset[str] = frozenset(
        {
            "market-00-yes",
            "market-03-no",
            "market-09-yes",
        }
    ),
) -> httpx.AsyncClient:
    runner = Runner(config=_settings())
    runner.bind_pg_pool(pg_pool)
    runner.state.mode = RunMode.LIVE
    runner.state.runner_started_at = datetime(2026, 4, 23, 8, 0, tzinfo=UTC)
    controller = SensorSubscriptionController(_SubscriptionSink())
    setattr(
        controller,
        "_current_asset_ids",
        current_asset_ids,
    )
    setattr(
        controller,
        "_last_updated_at",
        datetime(2026, 4, 23, 8, 30, tzinfo=UTC),
    )
    runner._subscription_controller = controller  # noqa: SLF001
    runner._controller_task = asyncio.get_running_loop().create_future()  # type: ignore[assignment]
    app = create_app(runner)
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    )


@pytest.mark.asyncio(loop_scope="session")
async def test_get_markets_returns_20_active_rows_with_subscription_state(
    pg_pool: asyncpg.Pool,
) -> None:
    store = PostgresMarketDataStore(pg_pool)
    now = ACTIVE_MARKET_NOW

    for index in range(20):
        market_id = f"market-{index:02d}"
        await _seed_market(
            store,
            market_id=market_id,
            question=f"Will checkpoint {index:02d} pass?",
            resolves_at=now + timedelta(days=1 + index),
            created_at=now - timedelta(days=7),
            updated_at=now - timedelta(minutes=index),
            volume_24h=2_000.0 - index,
        )

    await _seed_market(
        store,
        market_id="market-expired",
        question="Should not be returned",
        resolves_at=EXPIRED_MARKET_AT,
        created_at=EXPIRED_MARKET_AT - timedelta(days=14),
        updated_at=EXPIRED_MARKET_AT - timedelta(days=1),
        volume_24h=9_999.0,
    )

    async with _client(pg_pool) as client:
        response = await client.get("/markets?limit=20&offset=0")

    assert response.status_code == 200
    payload = response.json()

    assert payload["limit"] == 20
    assert payload["offset"] == 0
    assert payload["total"] == 20
    assert len(payload["markets"]) == 20
    assert payload["markets"][0] == {
        "market_id": "market-00",
        "question": "Will checkpoint 00 pass?",
        "venue": "polymarket",
        "volume_24h": 2000.0,
        "updated_at": now.isoformat(),
        "resolves_at": (now + timedelta(days=1)).isoformat(),
        "yes_token_id": "market-00-yes",
        "no_token_id": "market-00-no",
        "yes_price": None,
        "no_price": None,
        "best_bid": None,
        "best_ask": None,
        "last_trade_price": None,
        "liquidity": None,
        "spread_bps": None,
        "price_updated_at": None,
        "subscription_source": None,
        "subscribed": True,
    }
    assert payload["markets"][3]["subscribed"] is True
    assert payload["markets"][9]["subscribed"] is True
    assert all(row["market_id"] != "market-expired" for row in payload["markets"])


@pytest.mark.asyncio(loop_scope="session")
async def test_markets_route_returns_price_fields(
    pg_pool: asyncpg.Pool,
) -> None:
    store = PostgresMarketDataStore(pg_pool)
    now = ACTIVE_MARKET_NOW
    price_updated_at = now - timedelta(seconds=15)
    await _seed_market(
        store,
        market_id="market-priced",
        question="Will the route expose prices?",
        resolves_at=now + timedelta(days=1),
        created_at=now - timedelta(days=7),
        updated_at=now,
        volume_24h=3_000.0,
        yes_price=0.62,
        no_price=0.38,
        best_bid=0.61,
        best_ask=0.63,
        last_trade_price=0.62,
        liquidity=2500.25,
        spread_bps=200,
        price_updated_at=price_updated_at,
    )

    async with _client(pg_pool, current_asset_ids=frozenset()) as client:
        response = await client.get("/markets?limit=20&offset=0")

    assert response.status_code == 200
    row = response.json()["markets"][0]
    assert row["market_id"] == "market-priced"
    assert row["resolves_at"] == (now + timedelta(days=1)).isoformat()
    assert row["yes_price"] == 0.62
    assert row["no_price"] == 0.38
    assert row["best_bid"] == 0.61
    assert row["best_ask"] == 0.63
    assert row["last_trade_price"] == 0.62
    assert row["liquidity"] == 2500.25
    assert row["spread_bps"] == 200
    assert row["price_updated_at"] == price_updated_at.isoformat()


@pytest.mark.asyncio(loop_scope="session")
async def test_markets_route_returns_subscription_source_user(
    pg_pool: asyncpg.Pool,
) -> None:
    store = PostgresMarketDataStore(pg_pool)
    now = ACTIVE_MARKET_NOW
    yes_token_id, _ = await _seed_market(
        store,
        market_id="market-user-subscription",
        question="Will user subscription source surface?",
        resolves_at=now + timedelta(days=1),
        created_at=now - timedelta(days=7),
        updated_at=now,
        volume_24h=3_000.0,
    )
    async with pg_pool.acquire() as connection:
        await connection.execute(
            """
            INSERT INTO market_subscriptions (token_id, source)
            VALUES ($1, 'user')
            """,
            yes_token_id,
        )

    async with _client(pg_pool, current_asset_ids=frozenset()) as client:
        response = await client.get("/markets?limit=20&offset=0")

    assert response.status_code == 200
    row = response.json()["markets"][0]
    assert row["market_id"] == "market-user-subscription"
    assert row["subscription_source"] == "user"
    assert row["subscribed"] is False


@pytest.mark.asyncio(loop_scope="session")
async def test_markets_route_subscription_source_null_when_idle(
    pg_pool: asyncpg.Pool,
) -> None:
    store = PostgresMarketDataStore(pg_pool)
    now = ACTIVE_MARKET_NOW
    await _seed_market(
        store,
        market_id="market-idle",
        question="Will idle source stay null?",
        resolves_at=now + timedelta(days=1),
        created_at=now - timedelta(days=7),
        updated_at=now,
        volume_24h=3_000.0,
    )

    async with _client(pg_pool, current_asset_ids=frozenset()) as client:
        response = await client.get("/markets?limit=20&offset=0")

    assert response.status_code == 200
    row = response.json()["markets"][0]
    assert row["market_id"] == "market-idle"
    assert row["subscribed"] is False
    assert row["subscription_source"] is None


@pytest.mark.asyncio(loop_scope="session")
async def test_markets_route_subscribed_true_when_selector_has_token_even_if_no_user_row(
    pg_pool: asyncpg.Pool,
) -> None:
    store = PostgresMarketDataStore(pg_pool)
    now = ACTIVE_MARKET_NOW
    yes_token_id, _ = await _seed_market(
        store,
        market_id="market-selector-subscription",
        question="Will selector subscription stay distinct?",
        resolves_at=now + timedelta(days=1),
        created_at=now - timedelta(days=7),
        updated_at=now,
        volume_24h=3_000.0,
    )

    async with _client(
        pg_pool,
        current_asset_ids=frozenset({yes_token_id}),
    ) as client:
        response = await client.get("/markets?limit=20&offset=0")

    assert response.status_code == 200
    row = response.json()["markets"][0]
    assert row["market_id"] == "market-selector-subscription"
    assert row["subscribed"] is True
    assert row["subscription_source"] is None


@pytest.mark.asyncio(loop_scope="session")
async def test_subscribe_unsubscribe_roundtrip(
    pg_pool: asyncpg.Pool,
) -> None:
    store = PostgresMarketDataStore(pg_pool)
    now = ACTIVE_MARKET_NOW
    yes_token_id, _ = await _seed_market(
        store,
        market_id="market-subscribe-roundtrip",
        question="Will subscribe roundtrip update markets?",
        resolves_at=now + timedelta(days=1),
        created_at=now - timedelta(days=7),
        updated_at=now,
        volume_24h=3_000.0,
    )

    async with _client(pg_pool, current_asset_ids=frozenset()) as client:
        subscribe = await client.post(f"/markets/{yes_token_id}/subscribe")
        subscribed_markets = await client.get("/markets?limit=20&offset=0")
        subscribed_filter = await client.get("/markets?limit=20&offset=0&subscribed=only")
        unsubscribe = await client.delete(f"/markets/{yes_token_id}/subscribe")
        unsubscribed_markets = await client.get("/markets?limit=20&offset=0")

    assert subscribe.status_code == 200
    assert subscribe.json()["token_id"] == yes_token_id
    assert subscribe.json()["source"] == "user"
    assert subscribe.json()["created_at"] is not None

    assert subscribed_markets.status_code == 200
    subscribed_row = subscribed_markets.json()["markets"][0]
    assert subscribed_row["market_id"] == "market-subscribe-roundtrip"
    assert subscribed_row["subscription_source"] == "user"

    assert subscribed_filter.status_code == 200
    subscribed_filter_row = subscribed_filter.json()["markets"][0]
    assert subscribed_filter_row["market_id"] == "market-subscribe-roundtrip"
    assert subscribed_filter_row["subscription_source"] == "user"

    assert unsubscribe.status_code == 200
    assert unsubscribe.json() == {"token_id": yes_token_id, "deleted": True}

    assert unsubscribed_markets.status_code == 200
    unsubscribed_row = unsubscribed_markets.json()["markets"][0]
    assert unsubscribed_row["market_id"] == "market-subscribe-roundtrip"
    assert unsubscribed_row["subscription_source"] is None


@pytest.mark.asyncio(loop_scope="session")
async def test_price_history_endpoint_returns_chronological_order(
    pg_pool: asyncpg.Pool,
) -> None:
    store = PostgresMarketDataStore(pg_pool)
    now = ACTIVE_MARKET_NOW
    await _seed_market(
        store,
        market_id="market-price-history",
        question="Will price history sort chronologically?",
        resolves_at=now + timedelta(days=1),
        created_at=now - timedelta(days=7),
        updated_at=now,
        volume_24h=3_000.0,
    )
    first_snapshot_at = now - timedelta(minutes=3)
    second_snapshot_at = now - timedelta(minutes=2)
    third_snapshot_at = now - timedelta(minutes=1)
    for snapshot_at, yes_price in (
        (second_snapshot_at, 0.53),
        (first_snapshot_at, 0.51),
        (third_snapshot_at, 0.55),
    ):
        await store.write_price_snapshot(
            condition_id="market-price-history",
            snapshot_at=snapshot_at,
            yes_price=yes_price,
            no_price=1.0 - yes_price,
            best_bid=yes_price - 0.01,
            best_ask=yes_price + 0.01,
            last_trade_price=yes_price,
            liquidity=2_500.0,
            volume_24h=3_000.0,
        )

    async with _client(pg_pool, current_asset_ids=frozenset()) as client:
        response = await client.get(
            "/markets/market-price-history/price-history",
            params={
                "since": (now - timedelta(minutes=10)).isoformat(),
                "limit": 10,
            },
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["condition_id"] == "market-price-history"
    assert [row["snapshot_at"] for row in payload["snapshots"]] == [
        first_snapshot_at.isoformat(),
        second_snapshot_at.isoformat(),
        third_snapshot_at.isoformat(),
    ]
    assert [row["yes_price"] for row in payload["snapshots"]] == [0.51, 0.53, 0.55]
