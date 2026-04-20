from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from typing import cast
from uuid import uuid4

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


_SWEEP_YAML = """
base_spec:
  strategy_versions:
    - ["alpha", "alpha-v1"]
  dataset:
    source: "fixture"
    version: "v1"
    coverage_start: "2026-04-01T00:00:00+00:00"
    coverage_end: "2026-04-30T00:00:00+00:00"
    market_universe_filter:
      market_ids: ["market-a"]
    data_quality_gaps: []
  execution_model:
    fee_rate: 0.0
    slippage_bps: 5.0
    latency_ms: 0.0
    staleness_ms: 60000.0
    fill_policy: "immediate_or_cancel"
  risk_policy:
    max_position_notional_usdc: 100.0
    max_daily_drawdown_pct: 2.5
    min_order_size_usdc: 1.0
  date_range_start: "2026-04-01T00:00:00+00:00"
  date_range_end: "2026-04-30T00:00:00+00:00"
exec_config:
  chunk_days: 7
  time_budget: 1800
parameter_grid:
  strategy_versions:
    - [["alpha", "alpha-v1"]]
    - [["beta", "beta-v1"]]
""".strip()


def _settings() -> PMSSettings:
    return PMSSettings(
        database=DatabaseSettings(dsn=cast(str, PMS_TEST_DATABASE_URL)),
    )


def _app_client(pg_pool: asyncpg.Pool) -> httpx.AsyncClient:
    runner = Runner(config=_settings())
    runner.bind_pg_pool(pg_pool)
    app = create_app(runner)
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    )


async def _insert_strategy_run(pool: asyncpg.Pool, *, run_id: str) -> None:
    async with pool.acquire() as connection:
        await connection.execute(
            """
            INSERT INTO strategy_runs (
                strategy_run_id,
                run_id,
                strategy_id,
                strategy_version_id,
                brier,
                pnl_cum,
                drawdown_max,
                fill_rate,
                slippage_bps,
                opportunity_count,
                decision_count,
                fill_count,
                portfolio_target_json,
                started_at,
                finished_at
            ) VALUES (
                $1::uuid,
                $2::uuid,
                $3,
                $4,
                $5,
                $6,
                $7,
                $8,
                $9,
                $10,
                $11,
                $12,
                $13::jsonb,
                $14,
                $15
            )
            """,
            str(uuid4()),
            run_id,
            "alpha",
            "alpha-v1",
            0.14,
            12.5,
            1.0,
            0.75,
            8.0,
            4,
            3,
            2,
            json.dumps([]),
            datetime(2026, 4, 10, 9, 0, tzinfo=UTC),
            datetime(2026, 4, 10, 10, 0, tzinfo=UTC),
        )


@pytest.mark.asyncio(loop_scope="session")
async def test_research_backtest_post_and_get_routes_roundtrip(
    pg_pool: asyncpg.Pool,
) -> None:
    async with _app_client(pg_pool) as client:
        create_response = await client.post(
            "/research/backtest",
            content=_SWEEP_YAML,
            headers={"content-type": "application/x-yaml"},
        )

    assert create_response.status_code == 200
    payload = create_response.json()
    assert len(payload["run_ids"]) == 2
    run_id = payload["run_ids"][0]

    async with pg_pool.acquire() as connection:
        queued_count = await connection.fetchval(
            "SELECT COUNT(*) FROM backtest_runs WHERE run_id = ANY($1::uuid[])",
            payload["run_ids"],
        )
    assert queued_count == 2

    async with _app_client(pg_pool) as client:
        run_response = await client.get(f"/research/backtest/{run_id}")

    assert run_response.status_code == 200
    assert run_response.json()["run_id"] == run_id
    assert run_response.json()["status"] == "queued"
    assert run_response.json()["started_at"] is None
    assert run_response.json()["finished_at"] is None
    assert run_response.json()["failure_reason"] is None

    await _insert_strategy_run(pg_pool, run_id=run_id)

    async with _app_client(pg_pool) as client:
        strategies_response = await client.get(f"/research/backtest/{run_id}/strategies")

    assert strategies_response.status_code == 200
    assert strategies_response.json() == [
        {
            "strategy_run_id": strategies_response.json()[0]["strategy_run_id"],
            "run_id": run_id,
            "strategy_id": "alpha",
            "strategy_version_id": "alpha-v1",
            "brier": 0.14,
            "pnl_cum": 12.5,
            "drawdown_max": 1.0,
            "fill_rate": 0.75,
            "slippage_bps": 8.0,
            "opportunity_count": 4,
            "decision_count": 3,
            "fill_count": 2,
            "portfolio_target_json": [],
            "started_at": "2026-04-10T09:00:00+00:00",
            "finished_at": "2026-04-10T10:00:00+00:00",
        }
    ]


@pytest.mark.asyncio(loop_scope="session")
async def test_research_backtest_orphan_scan_marks_dead_workers_failed(
    pg_pool: asyncpg.Pool,
) -> None:
    run_id = str(uuid4())
    async with pg_pool.acquire() as connection:
        await connection.execute(
            """
            INSERT INTO backtest_runs (
                run_id,
                spec_hash,
                status,
                strategy_ids,
                date_range_start,
                date_range_end,
                exec_config_json,
                spec_json,
                started_at,
                worker_pid
            ) VALUES (
                $1::uuid,
                $2,
                'running',
                $3::text[],
                $4,
                $5,
                $6::jsonb,
                $7::jsonb,
                $8,
                $9
            )
            """,
            run_id,
            f"spec-{run_id}",
            ["alpha"],
            datetime(2026, 4, 1, tzinfo=UTC),
            datetime(2026, 4, 30, tzinfo=UTC),
            json.dumps({"chunk_days": 7, "time_budget": 1800}),
            json.dumps({"strategy_versions": [["alpha", "alpha-v1"]]}),
            datetime(2026, 4, 5, 12, 0, tzinfo=UTC),
            999999,
        )

    runner = Runner(config=_settings())
    runner.bind_pg_pool(pg_pool)
    app = create_app(runner, auto_start=False)

    async with app.router.lifespan_context(app):
        async with pg_pool.acquire() as connection:
            row = await connection.fetchrow(
                """
                SELECT status, failure_reason
                FROM backtest_runs
                WHERE run_id = $1::uuid
                """,
                run_id,
            )

    assert row is not None
    assert row["status"] == "failed"
    assert row["failure_reason"] == "orphaned (worker process gone)"
