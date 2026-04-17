from __future__ import annotations

import os

import asyncpg
import pytest


pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.environ.get("PMS_RUN_INTEGRATION") != "1",
        reason="set PMS_RUN_INTEGRATION=1 to run PostgreSQL integration tests",
    ),
    pytest.mark.skipif(
        os.environ.get("PMS_TEST_DATABASE_URL") is None,
        reason="set PMS_TEST_DATABASE_URL to the compose-backed PostgreSQL URI",
    ),
]


@pytest.mark.asyncio(loop_scope="session")
async def test_strategy_factors_starts_empty(pg_pool: asyncpg.Pool) -> None:
    async with pg_pool.acquire() as connection:
        count = await connection.fetchval("SELECT COUNT(*) FROM strategy_factors")

    assert count == 0
