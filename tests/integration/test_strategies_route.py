from __future__ import annotations

import json
import os
from datetime import UTC, datetime, timedelta
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


async def _seed_decision_row(
    connection: asyncpg.Connection,
    *,
    decision_id: str,
    opportunity_id: str,
    strategy_id: str,
    strategy_version_id: str,
    created_at: datetime,
) -> None:
    await connection.execute(
        """
        INSERT INTO decisions (
            decision_id,
            opportunity_id,
            strategy_id,
            strategy_version_id,
            status,
            created_at,
            updated_at,
            expires_at
        ) VALUES ($1, $2, $3, $4, 'filled', $5, $5, $6)
        """,
        decision_id,
        opportunity_id,
        strategy_id,
        strategy_version_id,
        created_at,
        created_at.replace(year=created_at.year + 1),
    )


async def _seed_fill_row(
    connection: asyncpg.Connection,
    *,
    fill_id: str,
    order_id: str,
    market_id: str,
    strategy_id: str,
    strategy_version_id: str,
    filled_at: datetime,
    fill_notional_usdc: float,
) -> None:
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
        ) VALUES ($1, $2, $3, $4, $5, 2.0, $6, $7)
        """,
        fill_id,
        order_id,
        market_id,
        filled_at,
        fill_notional_usdc,
        strategy_id,
        strategy_version_id,
    )


async def _seed_quote_eval_row(
    connection: asyncpg.Connection,
    *,
    fill_id: str,
    decision_id: str,
    strategy_id: str,
    strategy_version_id: str,
    mtm_pnl: float,
    quote_score: float,
    recorded_at: datetime,
    quote_lag_seconds: int = 0,
) -> None:
    await connection.execute(
        """
        INSERT INTO quote_eval_records (
            fill_id,
            decision_id,
            market_id,
            token_id,
            strategy_id,
            strategy_version_id,
            prob_estimate,
            quote_price,
            quote_source,
            quote_lag_seconds,
            quote_score,
            mtm_pnl,
            book_ts,
            recorded_at,
            citations,
            category,
            model_id
        ) VALUES (
            $1, $2, 'market-quote', 'token-quote', $3, $4, 0.55, 0.50,
            'postgres_snapshot', $8, $5, $6, $7, $7, '[]'::jsonb,
            'paper', 'paper-model'
        )
        """,
        fill_id,
        decision_id,
        strategy_id,
        strategy_version_id,
        quote_score,
        mtm_pnl,
        recorded_at,
        quote_lag_seconds,
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
        "baseline_brier_overall": None,
        "brier_improvement_overall": None,
        "pnl": pytest.approx(2.0),
        "pnl_source": "final_eval",
        "fill_rate": pytest.approx(1.0),
        "slippage_bps": pytest.approx(15.0),
        "drawdown": pytest.approx(3.0),
        "decision_count": 0,
        "fill_count": 0,
        "execution_fill_rate": pytest.approx(0.0),
        "executed_notional_usdc": pytest.approx(0.0),
        "quote_record_count": 0,
        "quote_score_overall": None,
        "quote_mtm_pnl": pytest.approx(0.0),
    }
    assert beta_row == {
        "strategy_id": "beta",
        "strategy_version_id": "beta-v1",
        "created_at": beta_created_at.isoformat(),
        "record_count": 2,
        "insufficient_samples": False,
        "brier_overall": pytest.approx(0.305),
        "baseline_brier_overall": None,
        "brier_improvement_overall": None,
        "pnl": pytest.approx(4.0),
        "pnl_source": "final_eval",
        "fill_rate": pytest.approx(1.0),
        "slippage_bps": pytest.approx(10.0),
        "drawdown": pytest.approx(2.0),
        "decision_count": 0,
        "fill_count": 0,
        "execution_fill_rate": pytest.approx(0.0),
        "executed_notional_usdc": pytest.approx(0.0),
        "quote_record_count": 0,
        "quote_score_overall": None,
        "quote_mtm_pnl": pytest.approx(0.0),
    }
    assert default_row == {
        "strategy_id": "default",
        "strategy_version_id": "default-v1",
        "created_at": default_created_at.isoformat(),
        "record_count": 0,
        "insufficient_samples": True,
        "brier_overall": None,
        "baseline_brier_overall": None,
        "brier_improvement_overall": None,
        "pnl": pytest.approx(0.0),
        "pnl_source": "none",
        "fill_rate": pytest.approx(0.0),
        "slippage_bps": pytest.approx(0.0),
        "drawdown": pytest.approx(0.0),
        "decision_count": 0,
        "fill_count": 0,
        "execution_fill_rate": pytest.approx(0.0),
        "executed_notional_usdc": pytest.approx(0.0),
        "quote_record_count": 0,
        "quote_score_overall": None,
        "quote_mtm_pnl": pytest.approx(0.0),
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
        index_exists = await connection.fetchval(
            """
            SELECT EXISTS (
                SELECT 1
                FROM pg_indexes
                WHERE indexname = $1
                  AND tablename = 'eval_records'
            )
            """,
            "idx_eval_records_strategy_identity",
        )
    assert index_exists is True


@pytest.mark.asyncio(loop_scope="session")
async def test_strategy_metrics_route_windows_eval_execution_and_quote_rows(
    pg_pool: asyncpg.Pool,
) -> None:
    old_at = datetime(2026, 4, 1, 0, 0, tzinfo=UTC)
    in_window_at = datetime(2026, 5, 30, 0, 0, tzinfo=UTC)
    async with pg_pool.acquire() as connection:
        await _seed_strategy(
            connection,
            strategy_id="windowed",
            strategy_version_id="windowed-v1",
        )
        await _seed_eval_record(
            connection,
            decision_id="windowed-old-eval",
            strategy_id="windowed",
            strategy_version_id="windowed-v1",
            brier_score=0.36,
            pnl=-100.0,
            slippage_bps=100.0,
            recorded_at=old_at,
        )
        await _seed_eval_record(
            connection,
            decision_id="windowed-new-eval",
            strategy_id="windowed",
            strategy_version_id="windowed-v1",
            brier_score=0.09,
            pnl=3.0,
            slippage_bps=10.0,
            recorded_at=in_window_at,
        )
        await _seed_decision_row(
            connection,
            decision_id="windowed-old-decision",
            opportunity_id="windowed-old-opportunity",
            strategy_id="windowed",
            strategy_version_id="windowed-v1",
            created_at=old_at,
        )
        await _seed_decision_row(
            connection,
            decision_id="windowed-new-decision",
            opportunity_id="windowed-new-opportunity",
            strategy_id="windowed",
            strategy_version_id="windowed-v1",
            created_at=in_window_at,
        )
        await _seed_fill_row(
            connection,
            fill_id="windowed-old-fill",
            order_id="windowed-old-order",
            market_id="windowed-market",
            strategy_id="windowed",
            strategy_version_id="windowed-v1",
            filled_at=old_at,
            fill_notional_usdc=100.0,
        )
        await _seed_fill_row(
            connection,
            fill_id="windowed-new-fill",
            order_id="windowed-new-order",
            market_id="windowed-market",
            strategy_id="windowed",
            strategy_version_id="windowed-v1",
            filled_at=in_window_at,
            fill_notional_usdc=2.0,
        )
        await _seed_quote_eval_row(
            connection,
            fill_id="windowed-old-fill",
            decision_id="windowed-old-decision",
            strategy_id="windowed",
            strategy_version_id="windowed-v1",
            mtm_pnl=-100.0,
            quote_score=0.50,
            recorded_at=old_at,
        )
        await _seed_quote_eval_row(
            connection,
            fill_id="windowed-new-fill",
            decision_id="windowed-new-decision",
            strategy_id="windowed",
            strategy_version_id="windowed-v1",
            mtm_pnl=0.5,
            quote_score=0.05,
            recorded_at=in_window_at,
        )

    async with _app_client(pg_pool) as client:
        response = await client.get(
            "/strategies/metrics",
            params={
                "since": "2026-05-01T00:00:00+00:00",
                "until": "2026-05-31T00:00:00+00:00",
            },
        )

    assert response.status_code == 200
    row = next(
        item for item in response.json()["strategies"] if item["strategy_id"] == "windowed"
    )
    assert row["record_count"] == 1
    assert row["pnl"] == pytest.approx(3.0)
    assert row["slippage_bps"] == pytest.approx(10.0)
    assert row["decision_count"] == 1
    assert row["fill_count"] == 1
    assert row["executed_notional_usdc"] == pytest.approx(2.0)
    assert row["quote_record_count"] == 1
    assert row["quote_mtm_pnl"] == pytest.approx(0.5)


@pytest.mark.asyncio(loop_scope="session")
async def test_strategy_metrics_route_excludes_archived_active_strategies(
    pg_pool: asyncpg.Pool,
) -> None:
    async with pg_pool.acquire() as connection:
        await _seed_strategy(
            connection,
            strategy_id="archived-metrics",
            strategy_version_id="archived-metrics-v1",
        )
        await connection.execute(
            """
            UPDATE strategies
            SET archived = TRUE
            WHERE strategy_id = 'archived-metrics'
            """
        )
        await _seed_eval_record(
            connection,
            decision_id="archived-metrics-eval",
            strategy_id="archived-metrics",
            strategy_version_id="archived-metrics-v1",
            brier_score=0.09,
            pnl=3.0,
            slippage_bps=10.0,
            recorded_at=datetime(2026, 5, 30, 0, 0, tzinfo=UTC),
        )
        await _seed_decision_row(
            connection,
            decision_id="archived-metrics-decision",
            opportunity_id="archived-metrics-opportunity",
            strategy_id="archived-metrics",
            strategy_version_id="archived-metrics-v1",
            created_at=datetime(2026, 5, 30, 0, 0, tzinfo=UTC),
        )

    async with _app_client(pg_pool) as client:
        response = await client.get("/strategies/metrics")

    assert response.status_code == 200
    strategy_ids = {
        row["strategy_id"] for row in response.json()["strategies"]
    }
    assert "archived-metrics" not in strategy_ids


@pytest.mark.asyncio(loop_scope="session")
async def test_strategy_metrics_route_uses_execution_and_quote_metrics_before_resolution(
    pg_pool: asyncpg.Pool,
) -> None:
    async with pg_pool.acquire() as connection:
        created_at = await _seed_strategy(
            connection,
            strategy_id="paper",
            strategy_version_id="paper-v1",
        )
        decision_at = datetime(2026, 4, 19, 0, 0, tzinfo=UTC)
        await _seed_decision_row(
            connection,
            decision_id="paper-decision-1",
            opportunity_id="paper-opportunity-1",
            strategy_id="paper",
            strategy_version_id="paper-v1",
            created_at=decision_at,
        )
        await _seed_decision_row(
            connection,
            decision_id="paper-decision-2",
            opportunity_id="paper-opportunity-2",
            strategy_id="paper",
            strategy_version_id="paper-v1",
            created_at=decision_at,
        )
        await _seed_fill_row(
            connection,
            fill_id="paper-fill-1",
            order_id="paper-order-1",
            market_id="market-paper",
            strategy_id="paper",
            strategy_version_id="paper-v1",
            filled_at=decision_at,
            fill_notional_usdc=1.25,
        )
        await _seed_quote_eval_row(
            connection,
            fill_id="paper-fill-1",
            decision_id="paper-decision-1",
            strategy_id="paper",
            strategy_version_id="paper-v1",
            mtm_pnl=-0.12,
            quote_score=0.03,
            recorded_at=decision_at,
        )
        await _seed_quote_eval_row(
            connection,
            fill_id="paper-fill-1",
            decision_id="paper-decision-1",
            strategy_id="paper",
            strategy_version_id="paper-v1",
            mtm_pnl=0.08,
            quote_score=0.05,
            recorded_at=decision_at + timedelta(hours=1),
            quote_lag_seconds=7_200,
        )

    async with _app_client(pg_pool) as client:
        response = await client.get("/strategies/metrics")

    assert response.status_code == 200
    paper_row = next(
        row for row in response.json()["strategies"] if row["strategy_id"] == "paper"
    )
    assert paper_row["created_at"] == created_at.isoformat()
    assert paper_row["record_count"] == 0
    assert paper_row["insufficient_samples"] is True
    assert paper_row["decision_count"] == 2
    assert paper_row["fill_count"] == 1
    assert paper_row["execution_fill_rate"] == pytest.approx(0.5)
    assert paper_row["fill_rate"] == pytest.approx(0.5)
    assert paper_row["executed_notional_usdc"] == pytest.approx(1.25)
    assert paper_row["quote_record_count"] == 2
    assert paper_row["quote_score_overall"] == pytest.approx(0.04)
    assert paper_row["pnl"] == pytest.approx(0.08)
    assert paper_row["quote_mtm_pnl"] == pytest.approx(0.08)
    assert paper_row["pnl_source"] == "quote_mtm"
