from __future__ import annotations

import os
import re
import signal
import socket
import subprocess
import time
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Iterator

import asyncpg
import httpx
import pytest

from pms.strategies.projections import (
    EvalSpec,
    ForecasterSpec,
    MarketSelectionSpec,
    RiskParams,
    StrategyConfig,
)
from pms.strategies.versioning import serialize_strategy_config_json


ROOT = Path(__file__).resolve().parents[2]
DASHBOARD_DIR = ROOT / "dashboard"
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


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _wait_for_http_ok(process: subprocess.Popen[str], url: str) -> None:
    deadline = time.monotonic() + 60.0
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise AssertionError(
                f"process for {url} exited early with code {process.returncode}"
            )
        try:
            response = httpx.get(url, timeout=0.5)
            if response.status_code < 500:
                return
        except Exception as exc:  # noqa: BLE001
            last_error = exc
        time.sleep(0.1)
    _stop_process_tree(process)
    raise AssertionError(f"{url} never became ready: {last_error}")


def _stop_process_tree(process: subprocess.Popen[str], *, timeout: float = 20.0) -> None:
    if process.poll() is not None:
        process.wait(timeout=5)
        return
    try:
        # Callers launch with start_new_session=True, so the child owns a process group.
        pgid = os.getpgid(process.pid)
    except ProcessLookupError:
        process.wait(timeout=5)
        return

    try:
        os.killpg(pgid, signal.SIGTERM)
    except ProcessLookupError:
        process.wait(timeout=5)
        return

    try:
        process.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(pgid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired as exc:
            raise AssertionError(f"process group {pgid} did not exit after SIGKILL") from exc


@contextmanager
def _run_api_server(database_url: str, port: int) -> Iterator[subprocess.Popen[str]]:
    env = os.environ.copy()
    env["DATABASE_URL"] = database_url
    env["PMS_API_HOST"] = "127.0.0.1"
    env["PMS_AUTO_START"] = "0"
    env.pop("PMS_DATABASE_URL", None)
    process = subprocess.Popen(
        [
            "uv",
            "run",
            "pms-api",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "--log-level",
            "warning",
        ],
        cwd=ROOT,
        env=env,
        text=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    try:
        _wait_for_http_ok(process, f"http://127.0.0.1:{port}/status")
        yield process
    finally:
        _stop_process_tree(process)


@contextmanager
def _run_dashboard_server(
    *,
    api_port: int,
    dashboard_port: int,
    revalidate_seconds: int,
) -> Iterator[subprocess.Popen[str]]:
    build_env = os.environ.copy()
    build_env["PMS_API_BASE_URL"] = f"http://127.0.0.1:{api_port}"
    build_env["PMS_SHARE_DEBUG_RENDER"] = "1"
    build_env["PMS_SHARE_REVALIDATE_SECONDS"] = str(revalidate_seconds)
    subprocess.run(
        ["npm", "ci"],
        cwd=DASHBOARD_DIR,
        env=build_env,
        text=True,
        capture_output=True,
        check=True,
    )
    subprocess.run(
        ["npm", "run", "build"],
        cwd=DASHBOARD_DIR,
        env=build_env,
        text=True,
        capture_output=True,
        check=True,
    )

    start_env = build_env.copy()
    next_binary = DASHBOARD_DIR / "node_modules" / ".bin" / "next"
    process = subprocess.Popen(
        [
            str(next_binary),
            "start",
            "--hostname",
            "127.0.0.1",
            "--port",
            str(dashboard_port),
        ],
        cwd=DASHBOARD_DIR,
        env=start_env,
        text=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    try:
        _wait_for_http_ok(process, f"http://127.0.0.1:{dashboard_port}/share/alpha")
        yield process
    finally:
        _stop_process_tree(process)


def _config_json(strategy_id: str) -> str:
    return serialize_strategy_config_json(
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
    )


async def _seed_share_strategy(pool: asyncpg.Pool, *, title: str) -> None:
    now = datetime(2026, 4, 23, 12, 0, tzinfo=UTC)
    async with pool.acquire() as connection:
        async with connection.transaction():
            await connection.execute("SET CONSTRAINTS ALL DEFERRED")
            await connection.execute(
                """
                INSERT INTO strategies (
                    strategy_id,
                    active_version_id,
                    title,
                    description,
                    archived,
                    share_enabled
                ) VALUES (
                    'alpha', 'alpha-v1234567', $1, 'Buy dislocations when liquidity is deep.', FALSE, TRUE
                )
                """,
                title,
            )
            await connection.execute(
                """
                INSERT INTO strategy_versions (
                    strategy_version_id,
                    strategy_id,
                    config_json
                ) VALUES (
                    'alpha-v1234567',
                    'alpha',
                    $1::jsonb
                )
                """,
                _config_json("alpha"),
            )
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
                'alpha-decision-1',
                'market-cp11',
                0.6,
                1.0,
                0.125,
                'matched',
                $1,
                '["seed"]',
                'cp11',
                'model-cp11',
                5.0,
                10.0,
                TRUE,
                'alpha',
                'alpha-v1234567'
            )
            """,
            now,
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
                'alpha-fill-1',
                'alpha-order-1',
                'market-cp11',
                $1,
                25.0,
                50.0,
                'alpha',
                'alpha-v1234567'
            )
            """,
            now,
        )


def _extract_debug_reads(html: str) -> str:
    match = re.search(r'data-testid="share-debug-read-count">([^<]+)<', html)
    assert match is not None, html
    return match.group(1)


@pytest.mark.asyncio(loop_scope="session")
async def test_share_page_revalidates_cached_projection_in_next_start_mode(
    pg_pool: asyncpg.Pool,
) -> None:
    assert PMS_TEST_DATABASE_URL is not None

    await _seed_share_strategy(pg_pool, title="Alpha Theory")
    api_port = _free_port()
    dashboard_port = _free_port()
    revalidate_seconds = 2

    with _run_api_server(PMS_TEST_DATABASE_URL, api_port):
        with _run_dashboard_server(
            api_port=api_port,
            dashboard_port=dashboard_port,
            revalidate_seconds=revalidate_seconds,
        ):
            first = httpx.get(
                f"http://127.0.0.1:{dashboard_port}/share/alpha",
                timeout=5.0,
            )
            assert first.status_code == 200
            assert "Alpha Theory" in first.text
            first_reads = int(_extract_debug_reads(first.text))

            async with pg_pool.acquire() as connection:
                await connection.execute(
                    """
                    UPDATE strategies
                    SET title = 'Alpha Theory Reloaded'
                    WHERE strategy_id = 'alpha'
                    """
                )

            second = httpx.get(
                f"http://127.0.0.1:{dashboard_port}/share/alpha",
                timeout=5.0,
            )
            assert second.status_code == 200
            assert "Alpha Theory" in second.text
            assert "Alpha Theory Reloaded" not in second.text
            assert int(_extract_debug_reads(second.text)) == first_reads

            time.sleep(revalidate_seconds + 1)

            third = httpx.get(
                f"http://127.0.0.1:{dashboard_port}/share/alpha",
                timeout=5.0,
            )
            assert third.status_code == 200
            assert "Alpha Theory Reloaded" in third.text
            assert int(_extract_debug_reads(third.text)) > first_reads
