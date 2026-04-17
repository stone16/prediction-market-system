from __future__ import annotations

import os
from datetime import UTC, datetime
from typing import cast

import asyncpg
import httpx
import pytest

from pms.api.app import create_app
from pms.config import DatabaseSettings, PMSSettings
from pms.runner import Runner


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
    )


async def _seed_default_strategy(connection: asyncpg.Connection) -> datetime:
    async with connection.transaction():
        await connection.execute("SET CONSTRAINTS ALL DEFERRED")
        await connection.execute(
            """
            INSERT INTO strategies (strategy_id, active_version_id)
            VALUES ('default', 'default-v1')
            ON CONFLICT (strategy_id) DO NOTHING
            """
        )
        await connection.execute(
            """
            INSERT INTO strategy_versions (
                strategy_version_id,
                strategy_id,
                config_json
            ) VALUES (
                'default-v1',
                'default',
                '{"config":{},"risk":{},"eval":{},"forecaster":{},"market_selection":{}}'::jsonb
            )
            ON CONFLICT (strategy_version_id) DO NOTHING
            """
        )
    created_at = await connection.fetchval(
        "SELECT created_at FROM strategies WHERE strategy_id = 'default'"
    )
    assert isinstance(created_at, datetime)
    return created_at.astimezone(UTC)


def _app_client(pg_pool: asyncpg.Pool) -> httpx.AsyncClient:
    runner = Runner(config=_settings())
    runner.bind_pg_pool(pg_pool)
    app = create_app(runner)
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    )


@pytest.mark.asyncio(loop_scope="session")
async def test_strategies_route_returns_seeded_registry_rows(
    pg_pool: asyncpg.Pool,
) -> None:
    async with pg_pool.acquire() as connection:
        expected_created_at = await _seed_default_strategy(connection)

    async with _app_client(pg_pool) as client:
        response = await client.get("/strategies")

    assert response.status_code == 200
    assert response.json() == {
        "strategies": [
            {
                "strategy_id": "default",
                "active_version_id": "default-v1",
                "created_at": expected_created_at.isoformat(),
            }
        ]
    }
