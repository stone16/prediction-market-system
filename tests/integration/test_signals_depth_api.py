from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta
from typing import Any, cast

import asyncpg
import httpx
import pytest

from pms.api.app import create_app
from pms.config import DashboardSettings, DatabaseSettings, PMSSettings
from pms.runner import Runner
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


def _settings() -> PMSSettings:
    return PMSSettings(
        database=DatabaseSettings(dsn=cast(str, PMS_TEST_DATABASE_URL)),
        dashboard=DashboardSettings(stale_snapshot_threshold_s=300.0),
    )


async def _seed_market(
    store: PostgresMarketDataStore,
    *,
    market_id: str,
    token_id: str = "token-yes",
    now: datetime,
) -> None:
    await store.write_market(
        market=cast(
            Any,
            __import__("pms.core.models", fromlist=["Market"]).Market(
                condition_id=market_id,
                slug=market_id,
                question=f"Question for {market_id}",
                venue="polymarket",
                resolves_at=None,
                created_at=now,
                last_seen_at=now,
            ),
        )
    )
    await store.write_token(
        token=cast(
            Any,
            __import__("pms.core.models", fromlist=["Token"]).Token(
                token_id=token_id,
                condition_id=market_id,
                outcome="YES",
            ),
        )
    )


async def _seed_snapshot(
    store: PostgresMarketDataStore,
    *,
    market_id: str,
    token_id: str,
    ts: datetime,
    bid_levels: list[tuple[float, float]],
    ask_levels: list[tuple[float, float]],
) -> int:
    models = __import__("pms.core.models", fromlist=["BookLevel", "BookSnapshot"])
    snapshot = models.BookSnapshot(
        id=0,
        market_id=market_id,
        token_id=token_id,
        ts=ts,
        hash="snapshot-hash",
        source="subscribe",
    )
    levels = [
        models.BookLevel(snapshot_id=0, market_id=market_id, side="BUY", price=price, size=size)
        for price, size in bid_levels
    ] + [
        models.BookLevel(snapshot_id=0, market_id=market_id, side="SELL", price=price, size=size)
        for price, size in ask_levels
    ]
    return await store.write_book_snapshot(snapshot, levels)


async def _seed_delta(
    store: PostgresMarketDataStore,
    *,
    market_id: str,
    token_id: str,
    ts: datetime,
    side: str,
    price: float,
    size: float,
    best_bid: float | None,
    best_ask: float | None,
) -> None:
    model = __import__("pms.core.models", fromlist=["PriceChange"]).PriceChange
    await store.write_price_change(
        model(
            id=0,
            market_id=market_id,
            token_id=token_id,
            ts=ts,
            side=side,
            price=price,
            size=size,
            best_bid=best_bid,
            best_ask=best_ask,
            hash="delta-hash",
        )
    )


def _app_client(pg_pool: asyncpg.Pool) -> httpx.AsyncClient:
    runner = Runner(config=_settings())
    runner.bind_pg_pool(pg_pool)
    app = create_app(runner)
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    )


@pytest.mark.asyncio(loop_scope="session")
async def test_signals_depth_route_returns_reconstructed_depth(
    pg_pool: asyncpg.Pool,
) -> None:
    now = datetime.now(tz=UTC)
    store = PostgresMarketDataStore(pg_pool)
    market_id = "depth-market"
    token_id = "depth-token-yes"
    await _seed_market(store, market_id=market_id, token_id=token_id, now=now)
    snapshot_ts = now - timedelta(seconds=30)
    await _seed_snapshot(
        store,
        market_id=market_id,
        token_id=token_id,
        ts=snapshot_ts,
        bid_levels=[(0.58, 120.0), (0.56, 80.0)],
        ask_levels=[(0.62, 90.0), (0.64, 110.0)],
    )
    await _seed_delta(
        store,
        market_id=market_id,
        token_id=token_id,
        ts=now - timedelta(seconds=10),
        side="BUY",
        price=0.57,
        size=150.0,
        best_bid=0.58,
        best_ask=0.62,
    )
    await _seed_delta(
        store,
        market_id=market_id,
        token_id=token_id,
        ts=now - timedelta(seconds=5),
        side="SELL",
        price=0.62,
        size=0.0,
        best_bid=0.58,
        best_ask=0.64,
    )

    async with _app_client(pg_pool) as client:
        response = await client.get(f"/signals/{market_id}/depth?limit=20")

    assert response.status_code == 200
    payload = response.json()
    assert payload["best_bid"] == 0.58
    assert payload["best_ask"] == 0.64
    assert payload["stale"] is False
    assert payload["bids"] == [
        {"price": 0.58, "size": 120.0},
        {"price": 0.57, "size": 150.0},
        {"price": 0.56, "size": 80.0},
    ]
    assert payload["asks"] == [{"price": 0.64, "size": 110.0}]
    assert payload["last_update_ts"] == (now - timedelta(seconds=5)).isoformat()


@pytest.mark.asyncio(loop_scope="session")
async def test_signals_depth_route_pins_last_update_ts_across_snapshot_and_delta_regimes(
    pg_pool: asyncpg.Pool,
) -> None:
    now = datetime.now(tz=UTC)
    store = PostgresMarketDataStore(pg_pool)

    await _seed_market(store, market_id="snapshot-only", token_id="snapshot-token", now=now)
    snapshot_only_ts = now - timedelta(seconds=50)
    await _seed_snapshot(
        store,
        market_id="snapshot-only",
        token_id="snapshot-token",
        ts=snapshot_only_ts,
        bid_levels=[(0.45, 100.0)],
        ask_levels=[(0.55, 100.0)],
    )

    await _seed_market(store, market_id="delta-only", token_id="delta-token", now=now)
    delta_only_ts = now - timedelta(seconds=20)
    await _seed_delta(
        store,
        market_id="delta-only",
        token_id="delta-token",
        ts=delta_only_ts,
        side="BUY",
        price=0.41,
        size=75.0,
        best_bid=0.41,
        best_ask=0.59,
    )
    await _seed_delta(
        store,
        market_id="delta-only",
        token_id="delta-token",
        ts=delta_only_ts + timedelta(seconds=1),
        side="SELL",
        price=0.59,
        size=60.0,
        best_bid=0.41,
        best_ask=0.59,
    )

    await _seed_market(store, market_id="snapshot-and-delta", token_id="mixed-token", now=now)
    mixed_snapshot_ts = now - timedelta(seconds=120)
    mixed_delta_ts = now - timedelta(seconds=15)
    await _seed_snapshot(
        store,
        market_id="snapshot-and-delta",
        token_id="mixed-token",
        ts=mixed_snapshot_ts,
        bid_levels=[(0.47, 100.0)],
        ask_levels=[(0.53, 100.0)],
    )
    await _seed_delta(
        store,
        market_id="snapshot-and-delta",
        token_id="mixed-token",
        ts=mixed_delta_ts,
        side="BUY",
        price=0.48,
        size=80.0,
        best_bid=0.48,
        best_ask=0.53,
    )

    async with _app_client(pg_pool) as client:
        snapshot_only = await client.get("/signals/snapshot-only/depth")
        delta_only = await client.get("/signals/delta-only/depth")
        snapshot_and_delta = await client.get("/signals/snapshot-and-delta/depth")

    assert snapshot_only.status_code == 200
    assert snapshot_only.json()["last_update_ts"] == snapshot_only_ts.isoformat()

    assert delta_only.status_code == 200
    assert delta_only.json()["last_update_ts"] == (delta_only_ts + timedelta(seconds=1)).isoformat()

    assert snapshot_and_delta.status_code == 200
    assert snapshot_and_delta.json()["last_update_ts"] == mixed_delta_ts.isoformat()


@pytest.mark.asyncio(loop_scope="session")
async def test_signals_depth_route_handles_empty_and_stale_regimes(
    pg_pool: asyncpg.Pool,
) -> None:
    now = datetime.now(tz=UTC)
    store = PostgresMarketDataStore(pg_pool)

    await store.write_market(
        market=cast(
            Any,
            __import__("pms.core.models", fromlist=["Market"]).Market(
                condition_id="known-empty-market",
                slug="known-empty-market",
                question="Known market without a tracked book",
                venue="polymarket",
                resolves_at=None,
                created_at=now,
                last_seen_at=now,
            ),
        )
    )

    await _seed_market(store, market_id="old-snapshot", token_id="old-token", now=now)
    await _seed_snapshot(
        store,
        market_id="old-snapshot",
        token_id="old-token",
        ts=now - timedelta(seconds=301),
        bid_levels=[(0.44, 100.0)],
        ask_levels=[(0.56, 100.0)],
    )

    await _seed_market(store, market_id="fresh-snapshot", token_id="fresh-token", now=now)
    await _seed_snapshot(
        store,
        market_id="fresh-snapshot",
        token_id="fresh-token",
        ts=now - timedelta(seconds=30),
        bid_levels=[(0.46, 100.0)],
        ask_levels=[(0.54, 100.0)],
    )

    await _seed_market(store, market_id="old-snapshot-fresh-delta", token_id="stale-token", now=now)
    await _seed_snapshot(
        store,
        market_id="old-snapshot-fresh-delta",
        token_id="stale-token",
        ts=now - timedelta(seconds=600),
        bid_levels=[(0.40, 100.0)],
        ask_levels=[(0.60, 100.0)],
    )
    await _seed_delta(
        store,
        market_id="old-snapshot-fresh-delta",
        token_id="stale-token",
        ts=now - timedelta(seconds=10),
        side="BUY",
        price=0.41,
        size=120.0,
        best_bid=0.41,
        best_ask=0.60,
    )

    async with _app_client(pg_pool) as client:
        empty = await client.get("/signals/known-empty-market/depth")
        old_snapshot = await client.get("/signals/old-snapshot/depth")
        fresh_snapshot = await client.get("/signals/fresh-snapshot/depth")
        stale = await client.get("/signals/old-snapshot-fresh-delta/depth")

    assert empty.status_code == 200
    assert empty.json() == {
        "best_bid": None,
        "best_ask": None,
        "bids": [],
        "asks": [],
        "last_update_ts": None,
        "stale": False,
    }

    assert old_snapshot.status_code == 200
    assert old_snapshot.json()["stale"] is True

    assert fresh_snapshot.status_code == 200
    assert fresh_snapshot.json()["stale"] is False

    assert stale.status_code == 200
    assert stale.json()["stale"] is True


@pytest.mark.asyncio(loop_scope="session")
@pytest.mark.parametrize("limit", [0, 201])
async def test_signals_depth_route_rejects_out_of_range_limits(
    pg_pool: asyncpg.Pool,
    limit: int,
) -> None:
    now = datetime.now(tz=UTC)
    store = PostgresMarketDataStore(pg_pool)
    market_id = "depth-invalid-limit"
    token_id = "depth-invalid-limit-yes"
    await _seed_market(store, market_id=market_id, token_id=token_id, now=now)
    await _seed_snapshot(
        store,
        market_id=market_id,
        token_id=token_id,
        ts=now - timedelta(seconds=5),
        bid_levels=[(0.51, 50.0)],
        ask_levels=[(0.53, 45.0)],
    )

    async with _app_client(pg_pool) as client:
        response = await client.get(f"/signals/{market_id}/depth?limit={limit}")

    assert response.status_code == 422
