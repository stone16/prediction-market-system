from __future__ import annotations

import json
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
        auto_migrate_default_v2=False,
        database=DatabaseSettings(dsn=cast(str, PMS_TEST_DATABASE_URL)),
    )


def _client(pg_pool: asyncpg.Pool) -> httpx.AsyncClient:
    runner = Runner(config=_settings())
    runner.bind_pg_pool(pg_pool)
    app = create_app(runner, auto_start=False)
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    )


@pytest.mark.asyncio(loop_scope="session")
async def test_metrics_route_exposes_first_trade_time_seconds(
    pg_pool: asyncpg.Pool,
) -> None:
    created_at = datetime(2026, 4, 23, 10, 0, tzinfo=UTC)
    filled_at = datetime(2026, 4, 23, 10, 2, tzinfo=UTC)

    async with pg_pool.acquire() as connection:
        await connection.execute(
            """
            INSERT INTO decisions (
                decision_id,
                opportunity_id,
                strategy_id,
                strategy_version_id,
                status,
                factor_snapshot_hash,
                created_at,
                updated_at,
                expires_at
            ) VALUES (
                'decision-cp12',
                'opportunity-cp12',
                'default',
                'default-v1',
                'accepted',
                'snapshot-cp12',
                $1,
                $1,
                $2
            )
            """,
            created_at,
            filled_at,
        )
        await connection.execute(
            """
            INSERT INTO fills (
                fill_id,
                order_id,
                market_id,
                ts,
                fill_notional_usdc,
                fill_quantity,
                strategy_id,
                strategy_version_id
            ) VALUES (
                'fill-cp12',
                'order-cp12',
                'market-cp12',
                $1,
                25.0,
                60.0,
                'default',
                'default-v1'
            )
            """,
            filled_at,
        )
        await connection.execute(
            """
            CREATE TABLE IF NOT EXISTS fill_payloads (
                fill_id TEXT PRIMARY KEY REFERENCES fills(fill_id) ON DELETE CASCADE,
                payload JSONB NOT NULL
            )
            """
        )
        await connection.execute(
            """
            INSERT INTO fill_payloads (fill_id, payload)
            VALUES (
                'fill-cp12',
                $1::jsonb
            )
            """,
            json.dumps(
                {
                    "trade_id": "trade-cp12",
                    "decision_id": "decision-cp12",
                    "token_id": "token-cp12-yes",
                    "venue": "polymarket",
                    "side": "BUY",
                    "fill_price": 0.42,
                    "executed_at": filled_at.isoformat(),
                    "status": "matched",
                    "anomaly_flags": [],
                    "fee_bps": None,
                    "fees": None,
                    "liquidity_side": None,
                    "transaction_ref": None,
                    "resolved_outcome": None,
                },
                sort_keys=True,
                separators=(",", ":"),
            ),
        )

    async with _client(pg_pool) as client:
        response = await client.get("/metrics")

    assert response.status_code == 200
    assert response.json()["pms.ui.first_trade_time_seconds"] == 120.0
