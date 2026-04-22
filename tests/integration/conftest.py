from __future__ import annotations

import os
import subprocess
from collections.abc import AsyncIterator
from pathlib import Path

import asyncpg
import pytest
import pytest_asyncio


SCHEMA_PATH = Path("schema.sql")
PMS_TEST_DATABASE_URL = os.environ.get("PMS_TEST_DATABASE_URL")


def _run_psql(database_url: str, *args: str) -> None:
    subprocess.run(
        ["psql", database_url, "--set", "ON_ERROR_STOP=1", *args],
        text=True,
        capture_output=True,
        check=True,
    )


def _run_alembic(database_url: str, *args: str) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env["DATABASE_URL"] = database_url
    env.pop("PMS_DATABASE_URL", None)
    return subprocess.run(
        ["uv", "run", "alembic", *args],
        text=True,
        capture_output=True,
        check=False,
        env=env,
    )


async def _truncate_public_tables(pool: asyncpg.Pool) -> None:
    async with pool.acquire() as connection:
        records = await connection.fetch(
            """
            SELECT tablename
            FROM pg_tables
            WHERE schemaname = 'public'
              AND tablename <> 'alembic_version'
            ORDER BY tablename
            """
        )
        table_names = [f'"{record["tablename"]}"' for record in records]
        if not table_names:
            return
        truncate_sql = (
            f"TRUNCATE TABLE {', '.join(table_names)} RESTART IDENTITY CASCADE"
        )
        await connection.execute(truncate_sql)


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def pg_pool() -> AsyncIterator[asyncpg.Pool]:
    assert PMS_TEST_DATABASE_URL is not None, "PMS_TEST_DATABASE_URL must be set"
    upgrade = _run_alembic(PMS_TEST_DATABASE_URL, "upgrade", "head")
    assert upgrade.returncode == 0, upgrade.stderr
    pool = await asyncpg.create_pool(
        dsn=PMS_TEST_DATABASE_URL,
        min_size=1,
        max_size=5,
    )
    try:
        yield pool
    finally:
        await pool.close()


@pytest_asyncio.fixture(loop_scope="session")
async def db_conn(pg_pool: asyncpg.Pool) -> AsyncIterator[asyncpg.Connection]:
    async with pg_pool.acquire() as connection:
        transaction = connection.transaction()
        await transaction.start()
        try:
            yield connection
        finally:
            await transaction.rollback()


@pytest_asyncio.fixture(autouse=True, loop_scope="session")
async def _truncate_cross_connection_state(
    request: pytest.FixtureRequest,
) -> AsyncIterator[None]:
    if "pg_pool" not in request.fixturenames and "db_conn" not in request.fixturenames:
        yield
        return

    pool = request.getfixturevalue("pg_pool")
    assert isinstance(pool, asyncpg.Pool)
    await _truncate_public_tables(pool)
    try:
        yield
    finally:
        await _truncate_public_tables(pool)
