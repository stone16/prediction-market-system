from __future__ import annotations

from datetime import UTC, datetime
import json
import os
from typing import cast
from uuid import uuid4

import asyncpg
import httpx
import pytest

from pms.api.app import create_app
from pms.config import DatabaseSettings, PMSSettings
from pms.research.comparison import BacktestLiveComparisonTool
from pms.research.entities import PortfolioTarget, serialize_portfolio_target_json
from pms.research.policies import SymbolNormalizationPolicy, TimeAlignmentPolicy
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


def _app_client(pg_pool: asyncpg.Pool) -> httpx.AsyncClient:
    runner = Runner(config=_settings())
    runner.bind_pg_pool(pg_pool)
    app = create_app(runner)
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    )


async def _insert_completed_run(pool: asyncpg.Pool, *, run_id: str) -> None:
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
                'comparison-spec',
                'completed',
                ARRAY['alpha']::text[],
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
            json.dumps({"strategy_versions": [["alpha", "alpha-v1"]]}),
        )


async def _insert_strategy_run(pool: asyncpg.Pool, *, run_id: str) -> None:
    portfolio_target = PortfolioTarget(
        strategy_id="alpha",
        strategy_version_id="alpha-v1",
        targets={
            ("market-a", "token-a", "buy_yes", datetime(2026, 4, 9, 10, 0, tzinfo=UTC)): 20.0,
            ("market-b", "token-b", "buy_yes", datetime(2026, 4, 9, 11, 0, tzinfo=UTC)): 25.0,
        },
    )
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
                'alpha',
                'alpha-v1',
                0.11,
                10.0,
                1.5,
                0.8,
                6.0,
                4,
                3,
                2,
                $3::jsonb,
                $4,
                $5
            )
            """,
            str(uuid4()),
            run_id,
            serialize_portfolio_target_json(portfolio_target),
            datetime(2026, 4, 9, 9, 0, tzinfo=UTC),
            datetime(2026, 4, 12, 18, 0, tzinfo=UTC),
        )


async def _insert_live_fill(
    pool: asyncpg.Pool,
    *,
    market_id: str,
    ts: datetime,
) -> None:
    async with pool.acquire() as connection:
        await connection.execute(
            """
            INSERT INTO fills (
                fill_id,
                order_id,
                market_id,
                ts,
                strategy_id,
                strategy_version_id
            ) VALUES (
                $1,
                $2,
                $3,
                $4,
                'alpha',
                'alpha-v1'
            )
            """,
            f"fill-{uuid4()}",
            f"order-{uuid4()}",
            market_id,
            ts,
        )


async def _insert_live_opportunity(
    pool: asyncpg.Pool,
    *,
    market_id: str,
    token_id: str,
    created_at: datetime,
) -> None:
    async with pool.acquire() as connection:
        await connection.execute(
            """
            INSERT INTO opportunities (
                opportunity_id,
                market_id,
                token_id,
                side,
                selected_factor_values,
                expected_edge,
                rationale,
                target_size_usdc,
                expiry,
                staleness_policy,
                strategy_id,
                strategy_version_id,
                created_at
            ) VALUES (
                $1,
                $2,
                $3,
                'yes',
                '{}'::jsonb,
                0.05,
                'fixture',
                25.0,
                NULL,
                'fresh',
                'alpha',
                'alpha-v1',
                $4
            )
            """,
            f"opp-{uuid4()}",
            market_id,
            token_id,
            created_at,
        )


async def _insert_pnl_report(pool: asyncpg.Pool, *, run_id: str, generated_at: datetime) -> None:
    await _insert_report(
        pool,
        run_id=run_id,
        generated_at=generated_at,
        ranking_metric="pnl_cum",
        metric_value=2.5,
    )


async def _insert_report(
    pool: asyncpg.Pool,
    *,
    run_id: str,
    generated_at: datetime,
    ranking_metric: str,
    metric_value: float,
) -> None:
    async with pool.acquire() as connection:
        await connection.execute(
            """
            INSERT INTO evaluation_reports (
                report_id,
                run_id,
                ranking_metric,
                ranked_strategies,
                benchmark_rows,
                attribution_commentary,
                warnings,
                next_action,
                generated_at
            ) VALUES (
                $1::uuid,
                $2::uuid,
                $3,
                $4::jsonb,
                '[]'::jsonb,
                'fixture',
                '[]'::jsonb,
                'fixture',
                $5
            )
            """,
            str(uuid4()),
            run_id,
            ranking_metric,
            json.dumps(
                [
                    {
                        "strategy_id": "alpha",
                        "strategy_version_id": "alpha-v1",
                        "metric_value": metric_value,
                        "rank": 1,
                    }
                ]
            ),
            generated_at,
        )


async def _insert_live_eval_record(
    pool: asyncpg.Pool,
    *,
    run_id: str,
    market_id: str,
    recorded_at: datetime,
    pnl: float,
) -> None:
    del run_id
    async with pool.acquire() as connection:
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
                $1,
                $2,
                0.6,
                1.0,
                0.16,
                'matched',
                $3,
                '[]'::jsonb,
                'fixture',
                'fixture-model',
                $4,
                5.0,
                TRUE,
                'alpha',
                'alpha-v1'
            )
            """,
            f"decision-{uuid4()}",
            market_id,
            recorded_at,
            pnl,
        )


async def _seed_comparison_fixture(pool: asyncpg.Pool, *, run_id: str) -> None:
    await _insert_completed_run(pool, run_id=run_id)
    await _insert_strategy_run(pool, run_id=run_id)
    await _insert_live_fill(
        pool,
        market_id="market-b",
        ts=datetime(2026, 4, 11, 0, 30, tzinfo=UTC),
    )
    await _insert_live_eval_record(
        pool,
        run_id=run_id,
        market_id="market-b",
        recorded_at=datetime(2026, 4, 11, 0, 30, tzinfo=UTC),
        pnl=1.5,
    )
    await _insert_live_eval_record(
        pool,
        run_id=run_id,
        market_id="market-b",
        recorded_at=datetime(2026, 4, 12, 12, 0, tzinfo=UTC),
        pnl=1.0,
    )
    await _insert_live_opportunity(
        pool,
        market_id="market-b",
        token_id="token-b",
        created_at=datetime(2026, 4, 10, 9, 0, tzinfo=UTC),
    )
    await _insert_live_opportunity(
        pool,
        market_id="market-c",
        token_id="token-c",
        created_at=datetime(2026, 4, 11, 9, 0, tzinfo=UTC),
    )
    await _insert_live_opportunity(
        pool,
        market_id="market-d",
        token_id="token-d",
        created_at=datetime(2026, 4, 12, 9, 0, tzinfo=UTC),
    )
    await _insert_pnl_report(
        pool,
        run_id=run_id,
        generated_at=datetime(2026, 4, 12, 12, 0, tzinfo=UTC),
    )


@pytest.mark.asyncio(loop_scope="session")
async def test_backtest_live_comparison_compute_supports_all_denominators(
    pg_pool: asyncpg.Pool,
) -> None:
    run_id = str(uuid4())
    await _seed_comparison_fixture(pg_pool, run_id=run_id)

    tool = BacktestLiveComparisonTool(
        pool=pg_pool,
        time_alignment_policy=TimeAlignmentPolicy(),
        symbol_normalization_policy=SymbolNormalizationPolicy(),
    )
    live_window_start = datetime(2026, 4, 10, 0, 0, tzinfo=UTC)
    live_window_end = datetime(2026, 4, 12, 23, 59, 59, tzinfo=UTC)

    backtest_view = await tool.compute(
        run_id=run_id,
        strategy_id="alpha",
        strategy_version_id="alpha-v1",
        live_window_start=live_window_start,
        live_window_end=live_window_end,
        denominator="backtest_set",
    )
    live_view = await tool.compute(
        run_id=run_id,
        strategy_id="alpha",
        strategy_version_id="alpha-v1",
        live_window_start=live_window_start,
        live_window_end=live_window_end,
        denominator="live_set",
    )
    union_view = await tool.compute(
        run_id=run_id,
        strategy_id="alpha",
        strategy_version_id="alpha-v1",
        live_window_start=live_window_start,
        live_window_end=live_window_end,
        denominator="union",
    )

    assert backtest_view.run_id == run_id
    assert backtest_view.strategy_id == "alpha"
    assert backtest_view.strategy_version_id == "alpha-v1"
    assert len(union_view.equity_delta_json) == 3
    assert [entry["day"] for entry in union_view.equity_delta_json] == [
        "2026-04-10",
        "2026-04-11",
        "2026-04-12",
    ]
    assert union_view.equity_delta_json[0]["live_equity"] == pytest.approx(0.0)
    assert union_view.equity_delta_json[-1]["live_equity"] == pytest.approx(2.5)
    assert backtest_view.overlap_ratio == pytest.approx(0.5)
    assert live_view.overlap_ratio == pytest.approx(1.0 / 3.0)
    assert union_view.overlap_ratio == pytest.approx(0.25)
    assert len(
        {
            backtest_view.overlap_ratio,
            live_view.overlap_ratio,
            union_view.overlap_ratio,
        }
    ) == 3
    assert union_view.backtest_only_symbols == ("market-a::token-a",)
    assert union_view.live_only_symbols == (
        "market-c::token-c",
        "market-d::token-d",
    )

    async with pg_pool.acquire() as connection:
        stored_count = await connection.fetchval(
            """
            SELECT COUNT(*)
            FROM backtest_live_comparisons
            WHERE run_id = $1::uuid
            """,
            run_id,
        )

    assert stored_count == 3


@pytest.mark.asyncio(loop_scope="session")
async def test_backtest_live_comparison_non_identity_time_policy_updates_curve_and_warnings(
    pg_pool: asyncpg.Pool,
) -> None:
    run_id = str(uuid4())
    await _seed_comparison_fixture(pg_pool, run_id=run_id)
    live_window_start = datetime(2026, 4, 10, 0, 0, tzinfo=UTC)
    live_window_end = datetime(2026, 4, 12, 23, 59, 59, tzinfo=UTC)

    identity_tool = BacktestLiveComparisonTool(
        pool=pg_pool,
        time_alignment_policy=TimeAlignmentPolicy(),
        symbol_normalization_policy=SymbolNormalizationPolicy(),
    )
    shifted_tool = BacktestLiveComparisonTool(
        pool=pg_pool,
        time_alignment_policy=TimeAlignmentPolicy(evaluation_offset_s=-3600.0),
        symbol_normalization_policy=SymbolNormalizationPolicy(),
    )

    identity = await identity_tool.compute(
        run_id=run_id,
        strategy_id="alpha",
        strategy_version_id="alpha-v1",
        live_window_start=live_window_start,
        live_window_end=live_window_end,
        denominator="union",
    )
    shifted = await shifted_tool.compute(
        run_id=run_id,
        strategy_id="alpha",
        strategy_version_id="alpha-v1",
        live_window_start=live_window_start,
        live_window_end=live_window_end,
        denominator="union",
    )

    assert identity.equity_delta_json != shifted.equity_delta_json

    async with pg_pool.acquire() as connection:
        warnings = await connection.fetchval(
            """
            SELECT warnings
            FROM evaluation_reports
            WHERE run_id = $1::uuid
              AND ranking_metric = 'pnl_cum'
            """,
            run_id,
        )

    assert warnings is not None
    decoded_warnings = json.loads(warnings) if isinstance(warnings, str) else warnings
    assert any(
        "non-identity comparison policy applied" in warning
        for warning in cast(list[str], decoded_warnings)
    )


@pytest.mark.asyncio(loop_scope="session")
async def test_backtest_live_comparison_warning_append_is_scoped_and_idempotent(
    pg_pool: asyncpg.Pool,
) -> None:
    run_id = str(uuid4())
    await _seed_comparison_fixture(pg_pool, run_id=run_id)
    await _insert_report(
        pg_pool,
        run_id=run_id,
        generated_at=datetime(2026, 4, 12, 12, 0, tzinfo=UTC),
        ranking_metric="brier",
        metric_value=0.11,
    )

    tool = BacktestLiveComparisonTool(
        pool=pg_pool,
        time_alignment_policy=TimeAlignmentPolicy(evaluation_offset_s=-3600.0),
        symbol_normalization_policy=SymbolNormalizationPolicy(),
    )
    live_window_start = datetime(2026, 4, 10, 0, 0, tzinfo=UTC)
    live_window_end = datetime(2026, 4, 12, 23, 59, 59, tzinfo=UTC)

    await tool.compute(
        run_id=run_id,
        strategy_id="alpha",
        strategy_version_id="alpha-v1",
        live_window_start=live_window_start,
        live_window_end=live_window_end,
        denominator="union",
    )
    await tool.compute(
        run_id=run_id,
        strategy_id="alpha",
        strategy_version_id="alpha-v1",
        live_window_start=live_window_start,
        live_window_end=live_window_end,
        denominator="union",
    )

    async with pg_pool.acquire() as connection:
        pnl_warnings = await connection.fetchval(
            """
            SELECT warnings
            FROM evaluation_reports
            WHERE run_id = $1::uuid
              AND ranking_metric = 'pnl_cum'
            """,
            run_id,
        )
        brier_warnings = await connection.fetchval(
            """
            SELECT warnings
            FROM evaluation_reports
            WHERE run_id = $1::uuid
              AND ranking_metric = 'brier'
            """,
            run_id,
        )

    assert pnl_warnings is not None
    decoded_pnl_warnings = (
        json.loads(pnl_warnings) if isinstance(pnl_warnings, str) else pnl_warnings
    )
    matching_warnings = [
        warning
        for warning in cast(list[str], decoded_pnl_warnings)
        if "non-identity comparison policy applied" in warning
    ]
    assert len(matching_warnings) == 1
    decoded_brier_warnings = (
        json.loads(brier_warnings) if isinstance(brier_warnings, str) else brier_warnings
    )
    assert decoded_brier_warnings == []


@pytest.mark.asyncio(loop_scope="session")
async def test_backtest_live_comparison_compare_route_returns_four_artefacts(
    pg_pool: asyncpg.Pool,
) -> None:
    run_id = str(uuid4())
    await _seed_comparison_fixture(pg_pool, run_id=run_id)

    async with _app_client(pg_pool) as client:
        response = await client.post(
            f"/research/backtest/{run_id}/compare",
            json={
                "live_window_start": "2026-04-10T00:00:00+00:00",
                "live_window_end": "2026-04-12T23:59:59+00:00",
                "denominator": "union",
            },
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["run_id"] == run_id
    assert payload["overlap_ratio"] == pytest.approx(0.25)
    assert len(payload["equity_delta_json"]) == 3
    assert payload["backtest_only_symbols"] == ["market-a::token-a"]
    assert payload["live_only_symbols"] == [
        "market-c::token-c",
        "market-d::token-d",
    ]
