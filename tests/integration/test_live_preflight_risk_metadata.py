from __future__ import annotations

import os

import asyncpg
import pytest

from pms.live_preflight import _fresh_usable_book_market_missing_risk_metadata_count


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


async def _seed_fresh_usable_book(
    pool: asyncpg.Pool,
    *,
    condition_id: str,
    risk_group_id: str | None,
) -> None:
    """Seed a fresh, usable book snapshot (BUY+SELL levels at ts=NOW())
    for ``condition_id``. ``risk_group_id`` controls the missing-metadata path.
    """
    async with pool.acquire() as connection:
        await connection.execute(
            """
            INSERT INTO markets (
                condition_id,
                slug,
                question,
                venue,
                created_at,
                last_seen_at,
                risk_group_id
            ) VALUES (
                $1,
                $1,
                'Will the preflight risk-metadata join survive schema reality?',
                'polymarket',
                NOW(),
                NOW(),
                $2
            )
            """,
            condition_id,
            risk_group_id,
        )
        token_id = f"{condition_id}-yes"
        await connection.execute(
            """
            INSERT INTO tokens (token_id, condition_id, outcome)
            VALUES ($1, $2, 'YES')
            """,
            token_id,
            condition_id,
        )
        snapshot_id = await connection.fetchval(
            """
            INSERT INTO book_snapshots (market_id, token_id, ts, source)
            VALUES ($1, $2, NOW(), 'subscribe')
            RETURNING id
            """,
            condition_id,
            token_id,
        )
        await connection.execute(
            """
            INSERT INTO book_levels (snapshot_id, market_id, side, price, size)
            VALUES
                ($1, $2, 'BUY', 0.50, 100.0),
                ($1, $2, 'SELL', 0.52, 100.0)
            """,
            snapshot_id,
            condition_id,
        )


@pytest.mark.asyncio(loop_scope="session")
async def test_missing_risk_metadata_count_zero_when_risk_group_present(
    pg_pool: asyncpg.Pool,
) -> None:
    """PASS path: market has a non-empty ``risk_group_id``, count is 0."""
    await _seed_fresh_usable_book(
        pg_pool,
        condition_id="pm-preflight-risk-present",
        risk_group_id="rg-test",
    )

    count = await _fresh_usable_book_market_missing_risk_metadata_count(
        pg_pool,
        max_age_s=300.0,
    )

    assert count == 0


@pytest.mark.asyncio(loop_scope="session")
async def test_missing_risk_metadata_count_one_when_risk_group_null(
    pg_pool: asyncpg.Pool,
) -> None:
    """FAIL path: market has NULL ``risk_group_id``, count is 1."""
    await _seed_fresh_usable_book(
        pg_pool,
        condition_id="pm-preflight-risk-missing",
        risk_group_id=None,
    )

    count = await _fresh_usable_book_market_missing_risk_metadata_count(
        pg_pool,
        max_age_s=300.0,
    )

    assert count == 1
