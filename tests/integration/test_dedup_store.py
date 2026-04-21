from __future__ import annotations

import asyncio
import os
from datetime import UTC, datetime, timedelta

import asyncpg
import pytest

import pms.storage.dedup_store as dedup_store_module
from pms.core.models import TradeDecision
from pms.storage.dedup_store import InMemoryDedupStore, PgDedupStore


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


def _decision(decision_id: str = "d1") -> TradeDecision:
    return TradeDecision(
        decision_id=decision_id,
        market_id="market-dedup",
        token_id="token-yes",
        venue="polymarket",
        side="BUY",
        notional_usdc=10.0,
        order_type="limit",
        max_slippage_bps=100,
        stop_conditions=["integration-test"],
        prob_estimate=0.6,
        expected_edge=0.1,
        time_in_force="GTC",
        opportunity_id=f"op-{decision_id}",
        strategy_id="strategy-a",
        strategy_version_id="strategy-a-v1",
        limit_price=0.42,
    )


def _checked_out_holders(pool: asyncpg.Pool) -> int:
    return sum(1 for holder in pool._holders if holder._in_use is not None)  # pyright: ignore[reportPrivateUsage]


async def _row_for(
    pool: asyncpg.Pool, decision_id: str
) -> asyncpg.Record | None:
    async with pool.acquire() as connection:
        return await connection.fetchrow(
            """
            SELECT
                decision_id,
                strategy_id,
                strategy_version_id,
                acquired_at,
                released_at,
                worker_host,
                worker_pid,
                outcome
            FROM order_intents
            WHERE decision_id = $1
            """,
            decision_id,
        )


@pytest.mark.asyncio(loop_scope="session")
async def test_pg_dedup_store_allows_exactly_one_concurrent_acquire(
    pg_pool: asyncpg.Pool,
) -> None:
    store = PgDedupStore(pg_pool)
    decision = _decision()

    results = await asyncio.gather(
        store.acquire(decision),
        store.acquire(decision),
    )

    assert sorted(results) == [False, True]
    row = await _row_for(pg_pool, decision.decision_id)
    assert row is not None
    assert row["strategy_id"] == decision.strategy_id
    assert row["strategy_version_id"] == decision.strategy_version_id
    assert row["released_at"] is None
    assert row["worker_host"]
    assert row["worker_pid"] is not None


@pytest.mark.asyncio(loop_scope="session")
async def test_pg_dedup_store_persists_duplicate_state_across_pool_recreation(
    pg_pool: asyncpg.Pool,
) -> None:
    del pg_pool
    assert PMS_TEST_DATABASE_URL is not None

    first_pool = await asyncpg.create_pool(
        dsn=PMS_TEST_DATABASE_URL,
        min_size=1,
        max_size=2,
    )
    try:
        first_store = PgDedupStore(first_pool)
        assert await first_store.acquire(_decision()) is True
    finally:
        await first_pool.close()

    second_pool = await asyncpg.create_pool(
        dsn=PMS_TEST_DATABASE_URL,
        min_size=1,
        max_size=2,
    )
    try:
        second_store = PgDedupStore(second_pool)
        assert await second_store.acquire(_decision()) is False
    finally:
        await second_pool.close()


@pytest.mark.asyncio(loop_scope="session")
async def test_pg_dedup_store_release_keeps_row_and_invalid_outcome_hits_check_constraint(
    pg_pool: asyncpg.Pool,
) -> None:
    store = PgDedupStore(pg_pool)
    decision = _decision()
    assert await store.acquire(decision) is True

    async with pg_pool.acquire() as connection:
        row_count_before = await connection.fetchval(
            "SELECT COUNT(*) FROM order_intents WHERE decision_id = $1",
            decision.decision_id,
        )

    await store.release(decision.decision_id, "matched")

    row = await _row_for(pg_pool, decision.decision_id)
    assert row is not None
    assert row["outcome"] == "matched"
    assert isinstance(row["released_at"], datetime)
    assert row["released_at"].tzinfo == UTC

    async with pg_pool.acquire() as connection:
        row_count_after = await connection.fetchval(
            "SELECT COUNT(*) FROM order_intents WHERE decision_id = $1",
            decision.decision_id,
        )

    assert row_count_before == 1
    assert row_count_after == 1

    with pytest.raises(asyncpg.CheckViolationError):
        await store.release(decision.decision_id, "frobnicate")


@pytest.mark.asyncio(loop_scope="session")
async def test_pg_dedup_store_retention_scan_deletes_only_old_released_rows(
    pg_pool: asyncpg.Pool,
) -> None:
    store = PgDedupStore(pg_pool)
    old_decision = _decision("d-old")
    fresh_decision = _decision("d-fresh")
    open_decision = _decision("d-open")

    assert await store.acquire(old_decision) is True
    assert await store.acquire(fresh_decision) is True
    assert await store.acquire(open_decision) is True

    await store.release(old_decision.decision_id, "matched")
    await store.release(fresh_decision.decision_id, "rejected")

    async with pg_pool.acquire() as connection:
        await connection.execute(
            """
            UPDATE order_intents
            SET released_at = $2
            WHERE decision_id = $1
            """,
            old_decision.decision_id,
            datetime.now(tz=UTC) - timedelta(hours=2),
        )

    deleted = await store.retention_scan(timedelta(hours=1))

    assert deleted == 1
    assert await _row_for(pg_pool, old_decision.decision_id) is None
    assert await _row_for(pg_pool, fresh_decision.decision_id) is not None
    assert await _row_for(pg_pool, open_decision.decision_id) is not None


@pytest.mark.asyncio(loop_scope="session")
async def test_pg_dedup_store_releases_connection_on_cancelled_error(
    pg_pool: asyncpg.Pool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = PgDedupStore(pg_pool)
    decision = _decision()
    holders_before = _checked_out_holders(pg_pool)

    async def raise_cancelled(
        connection: asyncpg.Connection,
        decision: TradeDecision,
        *,
        worker_host: str,
        worker_pid: int,
    ) -> str | None:
        del connection, decision, worker_host, worker_pid
        raise asyncio.CancelledError

    monkeypatch.setattr(
        dedup_store_module,
        "_insert_order_intent",
        raise_cancelled,
    )

    with pytest.raises(asyncio.CancelledError):
        await store.acquire(decision)

    assert _checked_out_holders(pg_pool) == holders_before


@pytest.mark.asyncio(loop_scope="session")
async def test_in_memory_dedup_store_keeps_released_rows_until_retention_scan() -> None:
    store = InMemoryDedupStore()
    decision = _decision("d-memory")

    assert await store.acquire(decision) is True
    assert await store.acquire(decision) is False

    await store.release(decision.decision_id, "matched")
    assert await store.acquire(decision) is False

    assert await store.retention_scan(timedelta(0)) == 1
    assert await store.acquire(decision) is True
