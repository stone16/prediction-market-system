from __future__ import annotations

import os
from datetime import UTC, datetime
import json
from uuid import uuid4

import asyncpg
import pytest

from pms.research.report import EvaluationReportGenerator


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


async def _insert_completed_run(pool: asyncpg.Pool, run_id: str) -> None:
    async with pool.acquire() as connection:
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
                finished_at
            ) VALUES (
                $1::uuid,
                'report-spec',
                'completed',
                ARRAY['alpha', 'beta', 'gamma']::text[],
                $2,
                $3,
                $4::jsonb,
                $5::jsonb,
                $2,
                $3
            )
            """,
            run_id,
            datetime(2026, 4, 1, tzinfo=UTC),
            datetime(2026, 4, 30, tzinfo=UTC),
            json.dumps({"chunk_days": 7, "time_budget": 1800}),
            json.dumps(
                {
                    "strategy_versions": [
                        ["alpha", "alpha-v1"],
                        ["beta", "beta-v1"],
                        ["gamma", "gamma-v1"],
                    ],
                    "dataset": {
                        "source": "fixture",
                        "version": "v1",
                        "coverage_start": "2026-04-01T00:00:00+00:00",
                        "coverage_end": "2026-04-30T00:00:00+00:00",
                        "market_universe_filter": {"market_ids": ["mkt-1"]},
                        "data_quality_gaps": [],
                    },
                    "execution_model": {
                        "fee_rate": 0.0,
                        "slippage_bps": 5.0,
                        "latency_ms": 0.0,
                        "staleness_ms": 60000.0,
                        "fill_policy": "immediate_or_cancel",
                    },
                    "risk_policy": {
                        "max_position_notional_usdc": 100.0,
                        "max_daily_drawdown_pct": 2.5,
                        "min_order_size_usdc": 1.0,
                    },
                    "date_range_start": "2026-04-01T00:00:00+00:00",
                    "date_range_end": "2026-04-30T00:00:00+00:00",
                }
            ),
        )


async def _insert_strategy_run(
    pool: asyncpg.Pool,
    *,
    run_id: str,
    strategy_id: str,
    strategy_version_id: str,
    brier: float,
    pnl_cum: float,
    drawdown_max: float,
) -> None:
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
                1.0,
                5.0,
                10,
                10,
                10,
                '[]'::jsonb,
                $8,
                $9
            )
            """,
            str(uuid4()),
            run_id,
            strategy_id,
            strategy_version_id,
            brier,
            pnl_cum,
            drawdown_max,
            datetime(2026, 4, 1, tzinfo=UTC),
            datetime(2026, 4, 30, tzinfo=UTC),
        )


async def _report_count(pool: asyncpg.Pool, run_id: str) -> int:
    async with pool.acquire() as connection:
        count = await connection.fetchval(
            """
            SELECT COUNT(*)
            FROM evaluation_reports
            WHERE run_id = $1::uuid
            """,
            run_id,
        )
    assert isinstance(count, int)
    return count


@pytest.mark.asyncio(loop_scope="session")
async def test_generate_ranks_three_strategies_and_persists_report(
    pg_pool: asyncpg.Pool,
) -> None:
    run_id = str(uuid4())
    await _insert_completed_run(pg_pool, run_id)
    await _insert_strategy_run(
        pg_pool,
        run_id=run_id,
        strategy_id="alpha",
        strategy_version_id="alpha-v1",
        brier=0.10,
        pnl_cum=80.0,
        drawdown_max=40.0,
    )
    await _insert_strategy_run(
        pg_pool,
        run_id=run_id,
        strategy_id="beta",
        strategy_version_id="beta-v1",
        brier=0.05,
        pnl_cum=30.0,
        drawdown_max=60.0,
    )
    await _insert_strategy_run(
        pg_pool,
        run_id=run_id,
        strategy_id="gamma",
        strategy_version_id="gamma-v1",
        brier=0.20,
        pnl_cum=50.0,
        drawdown_max=10.0,
    )

    report = await EvaluationReportGenerator(pg_pool).generate(run_id)

    assert report.ranking_metric == "brier"
    assert len(report.ranked_strategies) == 3
    assert tuple(entry.rank for entry in report.ranked_strategies) == (1, 2, 3)
    assert [entry.strategy_id for entry in report.ranked_strategies] == [
        "beta",
        "alpha",
        "gamma",
    ]
    assert all(entry.metric_value is not None for entry in report.ranked_strategies)
    assert isinstance(report.attribution_commentary, str)
    assert report.next_action
    assert await _report_count(pg_pool, run_id) == 1


@pytest.mark.asyncio(loop_scope="session")
async def test_generate_supports_metric_switches_and_idempotent_upserts(
    pg_pool: asyncpg.Pool,
) -> None:
    run_id = str(uuid4())
    await _insert_completed_run(pg_pool, run_id)
    await _insert_strategy_run(
        pg_pool,
        run_id=run_id,
        strategy_id="alpha",
        strategy_version_id="alpha-v1",
        brier=0.10,
        pnl_cum=80.0,
        drawdown_max=40.0,
    )
    await _insert_strategy_run(
        pg_pool,
        run_id=run_id,
        strategy_id="beta",
        strategy_version_id="beta-v1",
        brier=0.05,
        pnl_cum=30.0,
        drawdown_max=60.0,
    )
    await _insert_strategy_run(
        pg_pool,
        run_id=run_id,
        strategy_id="gamma",
        strategy_version_id="gamma-v1",
        brier=0.20,
        pnl_cum=50.0,
        drawdown_max=10.0,
    )

    generator = EvaluationReportGenerator(pg_pool)
    first_brier = await generator.generate(run_id, ranking_metric="brier")
    sharpe = await generator.generate(run_id, ranking_metric="sharpe")

    assert [entry.strategy_id for entry in first_brier.ranked_strategies] == [
        "beta",
        "alpha",
        "gamma",
    ]
    assert [entry.strategy_id for entry in sharpe.ranked_strategies] == [
        "gamma",
        "alpha",
        "beta",
    ]
    assert await _report_count(pg_pool, run_id) == 2

    async with pg_pool.acquire() as connection:
        await connection.execute(
            """
            UPDATE strategy_runs
            SET brier = 0.01
            WHERE run_id = $1::uuid
              AND strategy_id = 'alpha'
              AND strategy_version_id = 'alpha-v1'
            """,
            run_id,
        )

    updated_brier = await generator.generate(run_id, ranking_metric="brier")
    assert await _report_count(pg_pool, run_id) == 2
    assert [entry.strategy_id for entry in updated_brier.ranked_strategies] == [
        "alpha",
        "beta",
        "gamma",
    ]
