from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
import json
import os
from pathlib import Path
import socket
from typing import Any, cast
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4
import warnings

import asyncpg
from alembic import command
from alembic.config import Config
import httpx
import pytest
import uvicorn

from pms.api.app import create_app
from pms.config import DatabaseSettings, PMSSettings
from pms.core.models import BookLevel, BookSnapshot, Market, Token
from pms.research.runner import BacktestRunner
from pms.runner import Runner
from pms.storage.market_data_store import PostgresMarketDataStore
from pms.strategies.aggregate import Strategy
from pms.strategies.projections import (
    EvalSpec,
    ForecasterSpec,
    MarketSelectionSpec,
    RiskParams,
    StrategyConfig,
)
from pms.strategies.versioning import serialize_strategy_config_json


ROOT = Path(__file__).resolve().parents[2]
ALEMBIC_INI = ROOT / "alembic.ini"
EVIDENCE_DIR = ROOT / ".harness" / "pms-correctness-bundle-v1" / "checkpoints" / "20" / "evidence"
MARKET_ID = "cp20-smoke-market"
TOKEN_ID = "cp20-smoke-market-yes"

warnings.filterwarnings(
    "ignore",
    message="No path_separator found in configuration.*prepend_sys_path.*",
    category=DeprecationWarning,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.slow,
    pytest.mark.skipif(
        os.environ.get("PMS_RUN_INTEGRATION") != "1",
        reason="set PMS_RUN_INTEGRATION=1 to run PostgreSQL integration tests",
    ),
]


def _replace_database(database_url: str, database_name: str) -> str:
    parts = urlsplit(database_url)
    return urlunsplit(
        (
            parts.scheme,
            parts.netloc,
            f"/{database_name}",
            parts.query,
            parts.fragment,
        )
    )


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _neutral_strategy(strategy_id: str) -> Strategy:
    return Strategy(
        config=StrategyConfig(
            strategy_id=strategy_id,
            factor_composition=(),
            metadata=(("owner", "cp20"), ("profile", "neutral-smoke")),
        ),
        risk=RiskParams(
            max_position_notional_usdc=100.0,
            max_daily_drawdown_pct=2.5,
            min_order_size_usdc=1.0,
        ),
        eval_spec=EvalSpec(metrics=("brier", "pnl", "fill_rate")),
        forecaster=ForecasterSpec(
            forecasters=(
                ("rules", (("threshold", "0.55"),)),
                ("stats", (("prior_strength", "2.0"),)),
            )
        ),
        market_selection=MarketSelectionSpec(
            venue="polymarket",
            resolution_time_max_horizon_days=30,
            volume_min_usdc=100.0,
        ),
    )


async def _create_database(admin_database_url: str, database_name: str) -> None:
    connection = await asyncpg.connect(admin_database_url)
    try:
        await connection.execute(f'CREATE DATABASE "{database_name}"')
    finally:
        await connection.close()


async def _drop_database(admin_database_url: str, database_name: str) -> None:
    connection = await asyncpg.connect(admin_database_url)
    try:
        await connection.execute(f'DROP DATABASE IF EXISTS "{database_name}" WITH (FORCE)')
    finally:
        await connection.close()


def _run_alembic_upgrade(database_url: str) -> None:
    previous_database_url = os.environ.get("DATABASE_URL")
    previous_pms_database_url = os.environ.get("PMS_DATABASE_URL")
    try:
        os.environ["DATABASE_URL"] = database_url
        os.environ.pop("PMS_DATABASE_URL", None)
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore",
                message="No path_separator found in configuration.*prepend_sys_path.*",
                category=DeprecationWarning,
            )
            command.upgrade(Config(str(ALEMBIC_INI)), "head")
    finally:
        if previous_database_url is None:
            os.environ.pop("DATABASE_URL", None)
        else:
            os.environ["DATABASE_URL"] = previous_database_url
        if previous_pms_database_url is None:
            os.environ.pop("PMS_DATABASE_URL", None)
        else:
            os.environ["PMS_DATABASE_URL"] = previous_pms_database_url


async def _seed_strategy(pool: asyncpg.Pool, *, strategy_id: str, strategy_version_id: str) -> None:
    strategy = _neutral_strategy(strategy_id)
    async with pool.acquire() as connection:
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
            strategy_version_id,
            strategy_id,
            serialize_strategy_config_json(*strategy.snapshot()),
        )


async def _seed_market_data(pool: asyncpg.Pool) -> None:
    store = PostgresMarketDataStore(pool)
    start = datetime(2026, 4, 1, 12, 0, tzinfo=UTC)
    await store.write_market(
        Market(
            condition_id=MARKET_ID,
            slug=MARKET_ID,
            question="Will the CP20 smoke backtest stay neutral?",
            venue="polymarket",
            resolves_at=datetime(2026, 5, 15, tzinfo=UTC),
            created_at=start,
            last_seen_at=start + timedelta(days=29),
            volume_24h=2_500.0,
        )
    )
    await store.write_token(
        Token(
            token_id=TOKEN_ID,
            condition_id=MARKET_ID,
            outcome="YES",
        )
    )
    for day in range(30):
        ts = start + timedelta(days=day)
        snapshot_id = await store.write_book_snapshot(
            BookSnapshot(
                id=0,
                market_id=MARKET_ID,
                token_id=TOKEN_ID,
                ts=ts,
                hash=f"cp20-smoke-{day}",
                source="subscribe",
            ),
            [
                BookLevel(
                    snapshot_id=0,
                    market_id=MARKET_ID,
                    side="BUY",
                    price=0.49,
                    size=300.0,
                ),
                BookLevel(
                    snapshot_id=0,
                    market_id=MARKET_ID,
                    side="SELL",
                    price=0.51,
                    size=300.0,
                ),
            ],
        )
        assert snapshot_id > 0


async def _insert_backtest_run(
    pool: asyncpg.Pool,
    *,
    run_id: str,
    strategy_id: str,
    strategy_version_id: str,
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
            f"cp20-{run_id}",
            [strategy_id],
            datetime(2026, 4, 1, tzinfo=UTC),
            datetime(2026, 5, 1, tzinfo=UTC),
            json.dumps({"chunk_days": 7, "time_budget": 1800}),
            json.dumps(
                {
                    "strategy_versions": [[strategy_id, strategy_version_id]],
                    "dataset": {
                        "source": "cp20-smoke-fixture",
                        "version": "v1",
                        "coverage_start": "2026-04-01T00:00:00+00:00",
                        "coverage_end": "2026-05-01T00:00:00+00:00",
                        "market_universe_filter": {"market_ids": [MARKET_ID]},
                        "data_quality_gaps": [],
                    },
                    "execution_model": {
                        "fee_rate": 0.0,
                        "slippage_bps": 0.0,
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
                    "date_range_end": "2026-05-01T00:00:00+00:00",
                }
            ),
        )


async def _wait_for_status(
    client: httpx.AsyncClient,
    *,
    port: int,
    timeout_s: float = 20.0,
) -> dict[str, Any]:
    deadline = asyncio.get_running_loop().time() + timeout_s
    last_error: Exception | None = None
    while asyncio.get_running_loop().time() < deadline:
        try:
            response = await client.get(f"http://127.0.0.1:{port}/status", timeout=0.5)
            if response.status_code == 200:
                payload = response.json()
                assert isinstance(payload, dict)
                return cast(dict[str, Any], payload)
        except Exception as exc:  # noqa: BLE001
            last_error = exc
        await asyncio.sleep(0.1)
    raise AssertionError(f"/status never became ready: {last_error}")


async def _poll_run_completion(
    pool: asyncpg.Pool,
    run_id: str,
    *,
    timeout_s: float = 20.0,
) -> tuple[str, str | None]:
    deadline = asyncio.get_running_loop().time() + timeout_s
    while asyncio.get_running_loop().time() < deadline:
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
        status = cast(str, row["status"])
        failure_reason = cast(str | None, row["failure_reason"])
        if status in {"completed", "failed"}:
            return status, failure_reason
        await asyncio.sleep(0.05)
    raise AssertionError(f"run {run_id} did not finish within {timeout_s} seconds")


async def _degenerate_fill_count(pool: asyncpg.Pool) -> int:
    async with pool.acquire() as connection:
        count = await connection.fetchval(
            """
            SELECT COUNT(*)
            FROM orders
            WHERE filled_notional_usdc > 0
              AND (filled_notional_usdc < 0.001 OR filled_quantity < 0.001)
            """
        )
    assert isinstance(count, int)
    return count


def _write_evidence(*, lines: list[str], metrics_payload: dict[str, Any]) -> None:
    EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)
    (EVIDENCE_DIR / "cp20-smoke-stdout.txt").write_text(
        "\n".join(lines) + "\n",
        encoding="utf-8",
    )
    (EVIDENCE_DIR / "cp20-smoke-metrics.json").write_text(
        json.dumps(metrics_payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


@pytest.mark.asyncio(loop_scope="session")
async def test_e2e_smoke_compose_alembic_api_backtest_metrics(
    compose_postgres_dsn: str,
) -> None:
    admin_database_url = _replace_database(compose_postgres_dsn, "postgres")
    database_name = f"pms_cp20_{uuid4().hex[:8]}"
    database_url = _replace_database(compose_postgres_dsn, database_name)
    api_pool: asyncpg.Pool | None = None
    server: uvicorn.Server | None = None
    server_task: asyncio.Task[None] | None = None
    evidence_lines = [f"compose_dsn={compose_postgres_dsn}", f"database_name={database_name}"]
    port = _free_port()

    try:
        await _create_database(admin_database_url, database_name)
        evidence_lines.append("created temp database")
        _run_alembic_upgrade(database_url)
        evidence_lines.append("alembic upgrade head completed")

        api_pool = await asyncpg.create_pool(dsn=database_url, min_size=1, max_size=4)
        await _seed_strategy(
            api_pool,
            strategy_id="neutral-smoke",
            strategy_version_id="neutral-smoke-v1",
        )
        await _seed_market_data(api_pool)
        run_id = str(uuid4())
        await _insert_backtest_run(
            api_pool,
            run_id=run_id,
            strategy_id="neutral-smoke",
            strategy_version_id="neutral-smoke-v1",
        )
        evidence_lines.append(f"seeded run_id={run_id}")

        app = create_app(
            runner=Runner(
                config=PMSSettings(
                    database=DatabaseSettings(dsn=database_url),
                    auto_migrate_default_v2=False,
                )
            ),
            auto_start=False,
        )
        config = uvicorn.Config(
            app,
            host="127.0.0.1",
            port=port,
            log_level="warning",
            ws="none",
        )
        server = uvicorn.Server(config)
        server.install_signal_handlers = lambda: None
        server_task = asyncio.create_task(server.serve())

        async with httpx.AsyncClient() as client:
            status_payload = await _wait_for_status(client, port=port)
            evidence_lines.append(f"/status keys={sorted(status_payload)}")

            runner = BacktestRunner(
                writable_pool=api_pool,
                readonly_pool=api_pool,
            )
            execute_task = asyncio.create_task(runner.execute(run_id))
            run_status, failure_reason = await _poll_run_completion(api_pool, run_id)
            execute_result = await execute_task
            assert execute_result is True
            assert run_status == "completed"
            assert failure_reason is None
            evidence_lines.append(f"backtest status={run_status}")

            assert await _degenerate_fill_count(api_pool) == 0
            evidence_lines.append("degenerate_fill_count=0")

            metrics = await client.get(f"http://127.0.0.1:{port}/metrics")
            assert metrics.status_code == 200
            metrics_payload = metrics.json()
            assert isinstance(metrics_payload, dict)
            evidence_lines.append(f"/metrics keys={sorted(metrics_payload)}")

        _write_evidence(lines=evidence_lines, metrics_payload=cast(dict[str, Any], metrics_payload))
    finally:
        if server is not None:
            server.should_exit = True
        if server_task is not None:
            await asyncio.wait_for(server_task, timeout=10.0)
        if api_pool is not None:
            await api_pool.close()
        await _drop_database(admin_database_url, database_name)
