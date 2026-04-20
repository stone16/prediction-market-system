from __future__ import annotations

import asyncio
import json
import os
import signal
import subprocess
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast
from uuid import uuid4

import asyncpg
import pytest

from pms.core.models import BookLevel, BookSnapshot, Market, Token
from pms.storage.market_data_store import PostgresMarketDataStore
from pms.strategies.aggregate import Strategy
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
SWEEP_FIXTURE = Path("tests/fixtures/sweep_10variant.yaml")

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


def _cli_env(extra: dict[str, str] | None = None) -> dict[str, str]:
    env = os.environ.copy()
    assert PMS_TEST_DATABASE_URL is not None
    env["DATABASE_URL"] = PMS_TEST_DATABASE_URL
    if extra is not None:
        env.update(extra)
    return env


def _run_cli(*args: str, extra_env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["uv", "run", "pms-research", *args],
        text=True,
        capture_output=True,
        check=True,
        env=_cli_env(extra_env),
    )


def _strategy(strategy_id: str) -> Strategy:
    return Strategy(
        config=StrategyConfig(
            strategy_id=strategy_id,
            factor_composition=(
                FactorCompositionStep(
                    factor_id="orderbook_imbalance",
                    role="weighted",
                    param="",
                    weight=1.0,
                    threshold=None,
                ),
            ),
            metadata=(("owner", "cp06a"),),
        ),
        risk=RiskParams(
            max_position_notional_usdc=100.0,
            max_daily_drawdown_pct=2.5,
            min_order_size_usdc=1.0,
        ),
        eval_spec=EvalSpec(metrics=("brier", "pnl")),
        forecaster=ForecasterSpec(forecasters=(("rules", (("threshold", "0.55"),)),)),
        market_selection=MarketSelectionSpec(
            venue="polymarket",
            resolution_time_max_horizon_days=7,
            volume_min_usdc=500.0,
        ),
    )


async def _seed_strategy_versions(
    pool: asyncpg.Pool,
    strategy_ids: list[str],
) -> None:
    async with pool.acquire() as connection:
        for strategy_id in strategy_ids:
            strategy = _strategy(strategy_id)
            await connection.execute(
                """
                INSERT INTO strategies (strategy_id, metadata_json)
                VALUES ($1, '{}'::jsonb)
                ON CONFLICT (strategy_id) DO NOTHING
                """,
                strategy_id,
            )
            await connection.execute(
                """
                INSERT INTO strategy_versions (
                    strategy_version_id,
                    strategy_id,
                    config_json,
                    created_at
                ) VALUES ($1, $2, $3::jsonb, clock_timestamp())
                ON CONFLICT (strategy_version_id) DO NOTHING
                """,
                f"{strategy_id}-v1",
                strategy_id,
                serialize_strategy_config_json(*strategy.snapshot()),
            )


async def _seed_market_data(pool: asyncpg.Pool) -> None:
    store = PostgresMarketDataStore(pool)
    ts = datetime(2026, 4, 18, 12, 0, tzinfo=UTC)
    await store.write_market(
        Market(
            condition_id="sweep-market",
            slug="sweep-market",
            question="Will the 06a worker finish?",
            venue="polymarket",
            resolves_at=datetime(2026, 4, 30, tzinfo=UTC),
            created_at=ts,
            last_seen_at=ts,
            volume_24h=1000.0,
        )
    )
    await store.write_token(
        Token(
            token_id="sweep-market-yes",
            condition_id="sweep-market",
            outcome="YES",
        )
    )
    await store.write_book_snapshot(
        BookSnapshot(
            id=0,
            market_id="sweep-market",
            token_id="sweep-market-yes",
            ts=ts,
            hash="cp06a-snapshot",
            source="subscribe",
        ),
        [
            BookLevel(
                snapshot_id=0,
                market_id="sweep-market",
                side="BUY",
                price=0.41,
                size=120.0,
            ),
            BookLevel(
                snapshot_id=0,
                market_id="sweep-market",
                side="SELL",
                price=0.59,
                size=95.0,
            ),
        ],
    )


async def _insert_run(
    pool: asyncpg.Pool,
    *,
    run_id: str,
    strategy_id: str,
) -> None:
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
                spec_json
            ) VALUES (
                $1::uuid,
                $2,
                'queued',
                $3::text[],
                $4,
                $5,
                $6::jsonb,
                $7::jsonb
            )
            """,
            run_id,
            f"spec-{run_id}",
            [strategy_id],
            datetime(2026, 4, 1, tzinfo=UTC),
            datetime(2026, 4, 30, tzinfo=UTC),
            json.dumps({"chunk_days": 7, "time_budget": 1800}),
            json.dumps(
                {
                    "strategy_versions": [[strategy_id, f"{strategy_id}-v1"]],
                    "dataset": {
                        "source": "fixture",
                        "version": "v1",
                        "coverage_start": "2026-04-01T00:00:00+00:00",
                        "coverage_end": "2026-04-30T00:00:00+00:00",
                        "market_universe_filter": {"market_ids": ["sweep-market"]},
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


async def _run_status(pool: asyncpg.Pool, run_id: str) -> tuple[str, str | None]:
    async with pool.acquire() as connection:
        row = await connection.fetchrow(
            """
            SELECT status, failure_reason
            FROM backtest_runs
            WHERE run_id = $1::uuid
            """,
            run_id,
        )
    assert row is not None
    return cast(str, row["status"]), cast(str | None, row["failure_reason"])


async def _evaluation_report_count(pool: asyncpg.Pool, run_id: str) -> int:
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


async def _wait_for_status(
    pool: asyncpg.Pool,
    run_id: str,
    expected: str,
    *,
    timeout_s: float = 10.0,
) -> tuple[str, str | None]:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        status = await _run_status(pool, run_id)
        if status[0] == expected:
            return status
        await asyncio.sleep(0.05)
    return await _run_status(pool, run_id)


@pytest.mark.asyncio(loop_scope="session")
async def test_sweep_cli_enqueues_deduped_runs_and_reports_cache_gate(
    pg_pool: asyncpg.Pool,
) -> None:
    await _seed_strategy_versions(
        pg_pool,
        [f"sweep-alpha-{index}" for index in range(1, 10)],
    )

    result = _run_cli(
        "sweep",
        str(SWEEP_FIXTURE),
        "--database-url",
        cast(str, PMS_TEST_DATABASE_URL),
    )
    payload = json.loads(result.stdout)

    assert payload["unique_run_count"] == 9
    assert payload["cache_hit_rate"] > 0.95

    async with pg_pool.acquire() as connection:
        total_runs = await connection.fetchval("SELECT COUNT(*) FROM backtest_runs")
        duplicate_hash_count = await connection.fetchval(
            """
            SELECT COUNT(*)
            FROM backtest_runs
            WHERE spec_hash = $1
            """,
            payload["runs"][0]["spec_hash"],
        )
    assert total_runs == 9
    assert duplicate_hash_count == 1

    no_cache = _run_cli(
        "sweep",
        str(SWEEP_FIXTURE),
        "--database-url",
        cast(str, PMS_TEST_DATABASE_URL),
        "--no-cache",
    )
    no_cache_payload = json.loads(no_cache.stdout)
    assert no_cache_payload["cache_hit_rate"] == 0.0


@pytest.mark.asyncio(loop_scope="session")
async def test_worker_cli_completes_queued_run_and_generates_report(
    pg_pool: asyncpg.Pool,
) -> None:
    strategy_id = "worker-alpha"
    await _seed_strategy_versions(pg_pool, [strategy_id])
    await _seed_market_data(pg_pool)
    run_id = str(uuid4())
    await _insert_run(pg_pool, run_id=run_id, strategy_id=strategy_id)

    result = _run_cli(
        "worker",
        "--database-url",
        cast(str, PMS_TEST_DATABASE_URL),
        "--poll-interval",
        "0.05",
        "--max-runs",
        "1",
    )

    assert result.returncode == 0
    assert await _run_status(pg_pool, run_id) == ("completed", None)
    assert await _evaluation_report_count(pg_pool, run_id) == 1


@pytest.mark.asyncio(loop_scope="session")
async def test_worker_cli_sigterm_finishes_current_run_before_exit(
    pg_pool: asyncpg.Pool,
) -> None:
    strategy_id = "worker-sigterm"
    await _seed_strategy_versions(pg_pool, [strategy_id])
    await _seed_market_data(pg_pool)
    run_id = str(uuid4())
    await _insert_run(pg_pool, run_id=run_id, strategy_id=strategy_id)

    process = subprocess.Popen(
        [
            "uv",
            "run",
            "pms-research",
            "worker",
            "--database-url",
            cast(str, PMS_TEST_DATABASE_URL),
            "--poll-interval",
            "0.05",
        ],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=_cli_env({"PMS_RESEARCH_WORKER_CANCEL_PROBE_DELAY_S": "1.0"}),
    )
    try:
        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline:
            status, _ = await _run_status(pg_pool, run_id)
            if status == "running":
                break
            await asyncio.sleep(0.05)
        else:
            raise AssertionError("worker never claimed the queued run")

        process.send_signal(signal.SIGTERM)
        stdout, stderr = process.communicate(timeout=10.0)
        assert process.returncode == 0, (stdout, stderr)
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5.0)

    assert await _run_status(pg_pool, run_id) == ("completed", None)
