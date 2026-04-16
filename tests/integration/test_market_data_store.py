from __future__ import annotations

import os
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from importlib import import_module
from typing import Any, cast

import asyncpg
import pytest

import pms.core.models as core_models


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


def test_market_data_entities_are_defined() -> None:
    expected_entities = (
        "Market",
        "Token",
        "BookSnapshot",
        "BookLevel",
        "PriceChange",
        "Trade",
    )

    for entity_name in expected_entities:
        assert hasattr(core_models, entity_name)


def _store_type() -> type[Any]:
    module = import_module("pms.storage.market_data_store")
    return cast(type[Any], getattr(module, "PostgresMarketDataStore"))


def _market_type() -> type[Any]:
    return cast(type[Any], getattr(core_models, "Market"))


def _token_type() -> type[Any]:
    return cast(type[Any], getattr(core_models, "Token"))


def _book_snapshot_type() -> type[Any]:
    return cast(type[Any], getattr(core_models, "BookSnapshot"))


def _book_level_type() -> type[Any]:
    return cast(type[Any], getattr(core_models, "BookLevel"))


def _price_change_type() -> type[Any]:
    return cast(type[Any], getattr(core_models, "PriceChange"))


def _trade_type() -> type[Any]:
    return cast(type[Any], getattr(core_models, "Trade"))


def _market(
    *,
    condition_id: str = "market-1",
    slug: str = "market-1",
    question: str = "Will CP05 pass?",
    venue: str = "polymarket",
    resolves_at: datetime | None = None,
    created_at: datetime | None = None,
    last_seen_at: datetime | None = None,
) -> Any:
    market_cls = _market_type()
    created = created_at or datetime(2026, 4, 20, tzinfo=UTC)
    last_seen = last_seen_at or created
    return market_cls(
        condition_id=condition_id,
        slug=slug,
        question=question,
        venue=venue,
        resolves_at=resolves_at,
        created_at=created,
        last_seen_at=last_seen,
    )


def _token(
    *,
    token_id: str = "token-yes",
    condition_id: str = "market-1",
    outcome: str = "YES",
) -> Any:
    token_cls = _token_type()
    return token_cls(
        token_id=token_id,
        condition_id=condition_id,
        outcome=outcome,
    )


def _snapshot(
    *,
    market_id: str = "market-1",
    token_id: str = "token-yes",
    ts: datetime | None = None,
    hash_value: str | None = "book-hash",
    source: str = "subscribe",
) -> Any:
    snapshot_cls = _book_snapshot_type()
    return snapshot_cls(
        id=0,
        market_id=market_id,
        token_id=token_id,
        ts=ts or datetime(2026, 4, 21, tzinfo=UTC),
        hash=hash_value,
        source=source,
    )


def _level(
    *,
    snapshot_id: int = 0,
    market_id: str = "market-1",
    side: str = "BUY",
    price: float = 0.41,
    size: float = 100.0,
) -> Any:
    level_cls = _book_level_type()
    return level_cls(
        snapshot_id=snapshot_id,
        market_id=market_id,
        side=side,
        price=price,
        size=size,
    )


def _price_change(
    *,
    market_id: str = "market-1",
    token_id: str = "token-yes",
    ts: datetime | None = None,
    side: str = "BUY",
    price: float = 0.41,
    size: float = 10.0,
    best_bid: float | None = 0.41,
    best_ask: float | None = 0.43,
    hash_value: str | None = "delta-hash",
) -> Any:
    price_change_cls = _price_change_type()
    return price_change_cls(
        id=0,
        market_id=market_id,
        token_id=token_id,
        ts=ts or datetime(2026, 4, 21, 0, 1, tzinfo=UTC),
        side=side,
        price=price,
        size=size,
        best_bid=best_bid,
        best_ask=best_ask,
        hash=hash_value,
    )


def _trade(
    *,
    market_id: str = "market-1",
    token_id: str = "token-yes",
    ts: datetime | None = None,
    price: float = 0.42,
) -> Any:
    trade_cls = _trade_type()
    return trade_cls(
        id=0,
        market_id=market_id,
        token_id=token_id,
        ts=ts or datetime(2026, 4, 21, 0, 2, tzinfo=UTC),
        price=price,
    )


def _store(pg_pool: asyncpg.Pool) -> Any:
    store_cls = _store_type()
    return store_cls(pg_pool)


async def _fresh_counts(pg_pool: asyncpg.Pool) -> tuple[int, int]:
    async with pg_pool.acquire() as connection:
        snapshot_count = await connection.fetchval("SELECT COUNT(*) FROM book_snapshots")
        level_count = await connection.fetchval("SELECT COUNT(*) FROM book_levels")
    assert isinstance(snapshot_count, int)
    assert isinstance(level_count, int)
    return snapshot_count, level_count


async def _seed_market_and_token(store: Any) -> None:
    await store.write_market(_market())
    await store.write_token(_token())


@pytest.mark.asyncio(loop_scope="session")
async def test_write_market_and_token_insert_and_read_back(
    pg_pool: asyncpg.Pool,
) -> None:
    store = _store(pg_pool)
    market = _market(
        condition_id="market-insert",
        slug="market-insert",
        question="Will market rows persist?",
    )
    token = _token(
        token_id="token-insert",
        condition_id="market-insert",
    )

    await store.write_market(market)
    await store.write_token(token)

    async with pg_pool.acquire() as connection:
        market_row = await connection.fetchrow(
            """
            SELECT condition_id, slug, question, venue, resolves_at, created_at, last_seen_at
            FROM markets
            WHERE condition_id = $1
            """,
            market.condition_id,
        )
        token_row = await connection.fetchrow(
            """
            SELECT token_id, condition_id, outcome
            FROM tokens
            WHERE token_id = $1
            """,
            token.token_id,
        )

    assert market_row is not None
    assert token_row is not None
    assert market_row["slug"] == market.slug
    assert market_row["question"] == market.question
    assert token_row["condition_id"] == market.condition_id
    assert token_row["outcome"] == token.outcome


@pytest.mark.asyncio(loop_scope="session")
async def test_write_market_upsert_updates_existing_row(
    pg_pool: asyncpg.Pool,
) -> None:
    store = _store(pg_pool)
    original = _market(
        condition_id="market-upsert",
        slug="before",
        question="before",
        last_seen_at=datetime(2026, 4, 20, tzinfo=UTC),
    )
    updated = _market(
        condition_id="market-upsert",
        slug="after",
        question="after",
        last_seen_at=datetime(2026, 4, 21, tzinfo=UTC),
    )

    await store.write_market(original)
    await store.write_market(updated)

    async with pg_pool.acquire() as connection:
        row = await connection.fetchrow(
            """
            SELECT slug, question, last_seen_at
            FROM markets
            WHERE condition_id = $1
            """,
            original.condition_id,
        )

    assert row is not None
    assert row["slug"] == "after"
    assert row["question"] == "after"
    assert row["last_seen_at"] == updated.last_seen_at


@pytest.mark.asyncio(loop_scope="session")
async def test_write_book_snapshot_writes_snapshot_and_levels_transactionally(
    pg_pool: asyncpg.Pool,
) -> None:
    store = _store(pg_pool)
    await _seed_market_and_token(store)

    snapshot = _snapshot(
        market_id="market-1",
        token_id="token-yes",
        ts=datetime(2026, 4, 21, 0, 3, tzinfo=UTC),
    )
    levels = [
        _level(side="BUY", price=0.41, size=100.0),
        _level(side="SELL", price=0.43, size=120.0),
    ]

    snapshot_id = await store.write_book_snapshot(snapshot, levels)
    latest_snapshot = await store.read_latest_snapshot("market-1", "token-yes")
    stored_levels = await store.read_levels_for_snapshot(snapshot_id)

    assert snapshot_id > 0
    assert latest_snapshot is not None
    assert latest_snapshot.id == snapshot_id
    assert [level.side for level in stored_levels] == ["BUY", "SELL"]
    assert [level.price for level in stored_levels] == [0.41, 0.43]


@pytest.mark.asyncio(loop_scope="session")
async def test_write_book_snapshot_rolls_back_on_injected_second_level_failure(
    pg_pool: asyncpg.Pool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _store(pg_pool)
    await _seed_market_and_token(store)
    snapshot = _snapshot()
    levels = [_level(side="BUY"), _level(side="SELL")]

    original_execute = asyncpg.Connection.execute
    level_inserts = 0

    async def patched_execute(
        connection: asyncpg.Connection,
        query: str,
        *args: object,
        **kwargs: object,
    ) -> str:
        nonlocal level_inserts
        if "INSERT INTO book_levels" in query:
            level_inserts += 1
            if level_inserts == 2:
                raise RuntimeError("second level insert boom")
        return cast(str, await original_execute(connection, query, *args, **kwargs))

    monkeypatch.setattr(asyncpg.Connection, "execute", patched_execute)

    with pytest.raises(RuntimeError, match="second level insert boom"):
        await store.write_book_snapshot(snapshot, levels)

    assert await _fresh_counts(pg_pool) == (0, 0)


@pytest.mark.asyncio(loop_scope="session")
async def test_write_book_snapshot_rolls_back_on_check_constraint_violation(
    pg_pool: asyncpg.Pool,
) -> None:
    store = _store(pg_pool)
    await _seed_market_and_token(store)

    with pytest.raises(asyncpg.CheckViolationError):
        await store.write_book_snapshot(
            _snapshot(),
            [_level(side="HOLD")],
        )

    assert await _fresh_counts(pg_pool) == (0, 0)


@pytest.mark.asyncio(loop_scope="session")
async def test_write_book_snapshot_rolls_back_on_not_null_violation(
    pg_pool: asyncpg.Pool,
) -> None:
    store = _store(pg_pool)
    await _seed_market_and_token(store)
    invalid_level = _level(price=cast(float, None))

    with pytest.raises(asyncpg.NotNullViolationError):
        await store.write_book_snapshot(_snapshot(), [invalid_level])

    assert await _fresh_counts(pg_pool) == (0, 0)


@pytest.mark.asyncio(loop_scope="session")
async def test_write_price_change_persists_zero_size_delta_and_reads_since(
    pg_pool: asyncpg.Pool,
) -> None:
    store = _store(pg_pool)
    await _seed_market_and_token(store)
    delta = _price_change(
        ts=datetime(2026, 4, 21, 0, 4, tzinfo=UTC),
        size=0.0,
    )

    await store.write_price_change(delta)
    changes = await store.read_price_changes_since(
        delta.market_id,
        delta.ts - timedelta(seconds=1),
    )

    assert len(changes) == 1
    assert changes[0].size == 0.0
    assert changes[0].price == delta.price


@pytest.mark.asyncio(loop_scope="session")
async def test_write_trade_persists_trade_row(
    pg_pool: asyncpg.Pool,
) -> None:
    store = _store(pg_pool)
    await _seed_market_and_token(store)
    trade = _trade(
        market_id="market-trade",
        token_id="token-trade",
        ts=datetime(2026, 4, 21, 0, 5, tzinfo=UTC),
        price=0.52,
    )
    await store.write_market(
        _market(
            condition_id="market-trade",
            slug="market-trade",
            question="Will trade rows persist?",
        )
    )
    await store.write_token(
        _token(
            token_id="token-trade",
            condition_id="market-trade",
        )
    )

    await store.write_trade(trade)

    async with pg_pool.acquire() as connection:
        row = await connection.fetchrow(
            """
            SELECT market_id, token_id, ts, price
            FROM trades
            WHERE market_id = $1 AND token_id = $2
            """,
            trade.market_id,
            trade.token_id,
        )

    assert row is not None
    assert row["price"] == trade.price
    assert row["ts"] == trade.ts
