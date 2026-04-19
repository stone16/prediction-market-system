from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

import asyncpg
import httpx
import pytest

from pms.api.app import create_app
from pms.config import DatabaseSettings, PMSSettings
from pms.runner import Runner
from pms.strategies.projections import (
    EvalSpec,
    FactorCompositionStep,
    ForecasterSpec,
    MarketSelectionSpec,
    RiskParams,
    StrategyConfig,
)
from pms.strategies.versioning import serialize_strategy_config_json


PMS_TEST_DATABASE_URL = os.environ.get("PMS_TEST_DATABASE_URL")
CP06_EVIDENCE_DIR = Path(".harness/pms-controller-per-strategy-v1/checkpoints/06/iter-1/evidence")

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


def _default_strategy_config_json() -> str:
    return serialize_strategy_config_json(
        StrategyConfig(
            strategy_id="default",
            factor_composition=(
                FactorCompositionStep(
                    factor_id="factor-a",
                    role="weighted",
                    param="",
                    weight=0.6,
                    threshold=None,
                ),
                FactorCompositionStep(
                    factor_id="factor-b",
                    role="weighted",
                    param="",
                    weight=0.4,
                    threshold=None,
                ),
            ),
            metadata=(("owner", "system"), ("tier", "default")),
        ),
        RiskParams(
            max_position_notional_usdc=100.0,
            max_daily_drawdown_pct=2.5,
            min_order_size_usdc=1.0,
        ),
        EvalSpec(metrics=("brier", "pnl", "fill_rate")),
        ForecasterSpec(
            forecasters=(
                ("rules", (("threshold", "0.55"),)),
                ("stats", (("window", "15m"),)),
            )
        ),
        MarketSelectionSpec(
            venue="polymarket",
            resolution_time_max_horizon_days=7,
            volume_min_usdc=500.0,
        ),
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
                $1::jsonb
            )
            ON CONFLICT (strategy_version_id) DO NOTHING
            """,
            _default_strategy_config_json(),
        )
    created_at = await connection.fetchval(
        "SELECT created_at FROM strategies WHERE strategy_id = 'default'"
    )
    assert isinstance(created_at, datetime)
    return created_at.astimezone(UTC)


async def _seed_strategy(
    connection: asyncpg.Connection,
    *,
    strategy_id: str,
    strategy_version_id: str,
) -> datetime:
    async with connection.transaction():
        await connection.execute("SET CONSTRAINTS ALL DEFERRED")
        await connection.execute(
            """
            INSERT INTO strategies (strategy_id, active_version_id)
            VALUES ($1, $2)
            """,
            strategy_id,
            strategy_version_id,
        )
        await connection.execute(
            """
            INSERT INTO strategy_versions (
                strategy_version_id,
                strategy_id,
                config_json
            ) VALUES ($1, $2, $3::jsonb)
            """,
            strategy_version_id,
            strategy_id,
            serialize_strategy_config_json(
                StrategyConfig(
                    strategy_id=strategy_id,
                    factor_composition=(),
                    metadata=(("owner", "system"),),
                ),
                RiskParams(
                    max_position_notional_usdc=100.0,
                    max_daily_drawdown_pct=2.5,
                    min_order_size_usdc=1.0,
                ),
                EvalSpec(metrics=("brier", "pnl", "fill_rate")),
                ForecasterSpec(forecasters=()),
                MarketSelectionSpec(
                    venue="polymarket",
                    resolution_time_max_horizon_days=7,
                    volume_min_usdc=500.0,
                ),
            ),
        )
    created_at = await connection.fetchval(
        "SELECT created_at FROM strategies WHERE strategy_id = $1",
        strategy_id,
    )
    assert isinstance(created_at, datetime)
    return created_at.astimezone(UTC)


async def _seed_eval_record(
    connection: asyncpg.Connection,
    *,
    decision_id: str,
    strategy_id: str,
    strategy_version_id: str,
    brier_score: float,
    pnl: float,
    slippage_bps: float,
    recorded_at: datetime,
) -> None:
    await connection.execute(
        """
        INSERT INTO eval_records (
            decision_id,
            market_id,
            prob_estimate,
            resolved_outcome,
            brier_score,
            fill_status,
            recorded_at,
            citations,
            category,
            model_id,
            pnl,
            slippage_bps,
            filled,
            strategy_id,
            strategy_version_id
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9, $10, $11, $12, $13, $14, $15
        )
        """,
        decision_id,
        "market-1",
        0.6,
        1.0,
        brier_score,
        "matched",
        recorded_at,
        '["seed"]',
        "model-a",
        "model-a",
        pnl,
        slippage_bps,
        True,
        strategy_id,
        strategy_version_id,
    )


def _plan_uses_index(plan: object, *, index_name: str) -> bool:
    if isinstance(plan, str):
        stripped = plan.strip()
        if stripped.startswith("{") or stripped.startswith("["):
            return _plan_uses_index(json.loads(plan), index_name=index_name)
        return False
    if isinstance(plan, dict):
        if plan.get("Index Name") == index_name:
            return True
        return any(_plan_uses_index(value, index_name=index_name) for value in plan.values())
    if isinstance(plan, list):
        return any(_plan_uses_index(item, index_name=index_name) for item in plan)
    return False


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


@pytest.mark.asyncio(loop_scope="session")
async def test_strategy_metrics_route_returns_grouped_comparative_rows(
    pg_pool: asyncpg.Pool,
) -> None:
    async with pg_pool.acquire() as connection:
        default_created_at = await _seed_default_strategy(connection)
        alpha_created_at = await _seed_strategy(
            connection,
            strategy_id="alpha",
            strategy_version_id="alpha-v1",
        )
        beta_created_at = await _seed_strategy(
            connection,
            strategy_id="beta",
            strategy_version_id="beta-v1",
        )
        await _seed_eval_record(
            connection,
            decision_id="alpha-1",
            strategy_id="alpha",
            strategy_version_id="alpha-v1",
            brier_score=0.09,
            pnl=5.0,
            slippage_bps=10.0,
            recorded_at=datetime(2026, 4, 19, 0, 0, tzinfo=UTC),
        )
        await _seed_eval_record(
            connection,
            decision_id="alpha-2",
            strategy_id="alpha",
            strategy_version_id="alpha-v1",
            brier_score=0.16,
            pnl=-3.0,
            slippage_bps=20.0,
            recorded_at=datetime(2026, 4, 19, 0, 5, tzinfo=UTC),
        )
        await _seed_eval_record(
            connection,
            decision_id="beta-1",
            strategy_id="beta",
            strategy_version_id="beta-v1",
            brier_score=0.25,
            pnl=-2.0,
            slippage_bps=8.0,
            recorded_at=datetime(2026, 4, 19, 0, 10, tzinfo=UTC),
        )
        await _seed_eval_record(
            connection,
            decision_id="beta-2",
            strategy_id="beta",
            strategy_version_id="beta-v1",
            brier_score=0.36,
            pnl=6.0,
            slippage_bps=12.0,
            recorded_at=datetime(2026, 4, 19, 0, 15, tzinfo=UTC),
        )

    async with _app_client(pg_pool) as client:
        response = await client.get("/strategies/metrics")

    assert response.status_code == 200
    rows = response.json()["strategies"]
    assert [row["strategy_id"] for row in rows] == ["alpha", "beta", "default"]

    alpha_row = rows[0]
    beta_row = rows[1]
    default_row = rows[2]

    assert alpha_row == {
        "strategy_id": "alpha",
        "strategy_version_id": "alpha-v1",
        "created_at": alpha_created_at.isoformat(),
        "record_count": 2,
        "insufficient_samples": False,
        "brier_overall": pytest.approx(0.125),
        "pnl": pytest.approx(2.0),
        "fill_rate": pytest.approx(1.0),
        "slippage_bps": pytest.approx(15.0),
        "drawdown": pytest.approx(3.0),
    }
    assert beta_row == {
        "strategy_id": "beta",
        "strategy_version_id": "beta-v1",
        "created_at": beta_created_at.isoformat(),
        "record_count": 2,
        "insufficient_samples": False,
        "brier_overall": pytest.approx(0.305),
        "pnl": pytest.approx(4.0),
        "fill_rate": pytest.approx(1.0),
        "slippage_bps": pytest.approx(10.0),
        "drawdown": pytest.approx(2.0),
    }
    assert default_row == {
        "strategy_id": "default",
        "strategy_version_id": "default-v1",
        "created_at": default_created_at.isoformat(),
        "record_count": 0,
        "insufficient_samples": True,
        "brier_overall": None,
        "pnl": pytest.approx(0.0),
        "fill_rate": pytest.approx(0.0),
        "slippage_bps": pytest.approx(0.0),
        "drawdown": pytest.approx(0.0),
    }

    async with pg_pool.acquire() as connection:
        await connection.execute(
            """
            INSERT INTO eval_records (
                decision_id,
                market_id,
                prob_estimate,
                resolved_outcome,
                brier_score,
                fill_status,
                recorded_at,
                citations,
                category,
                model_id,
                pnl,
                slippage_bps,
                filled,
                strategy_id,
                strategy_version_id
            )
            SELECT
                'bulk-' || series::text,
                'market-bulk',
                0.5,
                0.0,
                0.25,
                'matched',
                TIMESTAMPTZ '2026-04-19T01:00:00+00:00' + (series || ' seconds')::interval,
                '["bulk"]'::jsonb,
                'model-bulk',
                'model-bulk',
                0.0,
                1.0,
                TRUE,
                'default',
                'default-v1'
            FROM generate_series(1, 1200) AS series
            """
        )
        explain_plan = await connection.fetchval(
            """
            EXPLAIN (FORMAT JSON)
            SELECT
                market_id,
                decision_id,
                prob_estimate,
                resolved_outcome,
                brier_score,
                fill_status,
                recorded_at,
                citations,
                strategy_id,
                strategy_version_id,
                category,
                model_id,
                pnl,
                slippage_bps,
                filled
            FROM eval_records
            WHERE strategy_id = $1 AND strategy_version_id = $2
            ORDER BY recorded_at ASC, decision_id ASC
            """,
            "alpha",
            "alpha-v1",
        )

    CP06_EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)
    plan_path = CP06_EVIDENCE_DIR / "strategies-metrics-explain.json"
    plan_path.write_text(
        json.dumps(explain_plan, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    assert _plan_uses_index(
        explain_plan,
        index_name="idx_eval_records_strategy_identity",
    )
