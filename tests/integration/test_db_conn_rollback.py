from __future__ import annotations

import os
from collections.abc import AsyncIterator
from pathlib import Path

import asyncpg
import pytest
import pytest_asyncio
import yaml


COMPOSE_PATH = Path("compose.yml")
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


def test_compose_postgres_service_contract() -> None:
    compose = yaml.safe_load(COMPOSE_PATH.read_text())
    postgres = compose["services"]["postgres"]

    assert postgres["image"] == "postgres:16"
    assert postgres["ports"] == ["5432:5432"]
    assert postgres["environment"] == {
        "POSTGRES_USER": "postgres",
        "POSTGRES_PASSWORD": "postgres",
        "POSTGRES_DB": "pms_test",
    }
    assert postgres["healthcheck"]["test"] == [
        "CMD-SHELL",
        "pg_isready -U postgres -d pms_test",
    ]


@pytest_asyncio.fixture(loop_scope="session")
async def _assert_feedback_row_rolled_back(
    pg_pool: asyncpg.Pool,
) -> AsyncIterator[None]:
    yield
    async with pg_pool.acquire() as connection:
        row_count = await connection.fetchval("SELECT COUNT(*) FROM feedback")
    assert row_count == 0


@pytest.mark.asyncio(loop_scope="session")
async def test_db_conn_rolls_back_each_test_transaction(
    db_conn: asyncpg.Connection,
    pg_pool: asyncpg.Pool,
    _assert_feedback_row_rolled_back: None,
) -> None:
    await db_conn.execute(
        """
        INSERT INTO feedback (
            feedback_id,
            target,
            source,
            message,
            severity,
            created_at,
            strategy_id,
            strategy_version_id
        ) VALUES ($1, $2, $3, $4, $5, now(), $6, $7)
        """,
        "feedback-cp04",
        "sensor",
        "integration-test",
        "rollback me",
        "warning",
        "default",
        "default-v1",
    )

    inside_row_count = await db_conn.fetchval("SELECT COUNT(*) FROM feedback")

    async with pg_pool.acquire() as connection:
        outside_row_count = await connection.fetchval("SELECT COUNT(*) FROM feedback")

    assert inside_row_count == 1
    assert outside_row_count == 0
