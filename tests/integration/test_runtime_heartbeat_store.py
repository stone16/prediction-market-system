from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta

import asyncpg
import pytest

from pms.storage.runtime_heartbeat_store import RuntimeHeartbeatStore


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


@pytest.mark.asyncio(loop_scope="session")
async def test_runtime_continuity_includes_initial_missing_heartbeat_gap(
    pg_pool: asyncpg.Pool,
) -> None:
    run_id = "runtime-heartbeat-late-start"
    started_at = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
    first_observed_at = started_at + timedelta(days=30)
    store = RuntimeHeartbeatStore(pg_pool)
    async with pg_pool.acquire() as connection:
        await connection.execute(
            "DELETE FROM runtime_heartbeats WHERE run_id = $1",
            run_id,
        )

    await store.append(
        run_id=run_id,
        mode="paper",
        started_at=started_at,
        observed_at=first_observed_at,
        strategy_fingerprint="strategy-fingerprint",
        component_status={"running": True},
    )

    continuity = await store.continuity(
        run_id=run_id,
        observed_until=first_observed_at,
    )

    assert continuity is not None
    assert continuity.healthy_days == 0
    assert continuity.heartbeat_count == 1
    assert continuity.max_gap_seconds == pytest.approx(2_592_000.0)


@pytest.mark.asyncio(loop_scope="session")
async def test_runtime_continuity_includes_trailing_missing_heartbeat_gap(
    pg_pool: asyncpg.Pool,
) -> None:
    run_id = "runtime-heartbeat-stale-tail"
    started_at = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
    first_observed_at = started_at
    last_observed_at = started_at + timedelta(minutes=1)
    observed_until = started_at + timedelta(minutes=10)
    store = RuntimeHeartbeatStore(pg_pool)
    async with pg_pool.acquire() as connection:
        await connection.execute(
            "DELETE FROM runtime_heartbeats WHERE run_id = $1",
            run_id,
        )

    for observed_at in (first_observed_at, last_observed_at):
        await store.append(
            run_id=run_id,
            mode="paper",
            started_at=started_at,
            observed_at=observed_at,
            strategy_fingerprint="strategy-fingerprint",
            component_status={"running": True, "sensor_tasks": 1, "controller_runtimes": 1},
        )

    continuity = await store.continuity(
        run_id=run_id,
        observed_until=observed_until,
    )

    assert continuity is not None
    assert continuity.heartbeat_count == 2
    assert continuity.max_gap_seconds == pytest.approx(540.0)


@pytest.mark.asyncio(loop_scope="session")
async def test_runtime_continuity_excludes_heartbeats_after_report_cutoff(
    pg_pool: asyncpg.Pool,
) -> None:
    run_id = "runtime-heartbeat-retrospective-cutoff"
    started_at = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
    observed_until = started_at + timedelta(days=1)
    store = RuntimeHeartbeatStore(pg_pool)
    async with pg_pool.acquire() as connection:
        await connection.execute(
            "DELETE FROM runtime_heartbeats WHERE run_id = $1",
            run_id,
        )

    observations: tuple[tuple[datetime, dict[str, object]], ...] = (
        (started_at, {"running": True, "sensor_tasks": 1, "controller_runtimes": 1}),
        (
            observed_until,
            {"running": True, "sensor_tasks": 1, "controller_runtimes": 1},
        ),
        (
            observed_until + timedelta(days=1),
            {"running": False, "sensor_tasks": 0, "controller_runtimes": 0},
        ),
    )
    for observed_at, component_status in observations:
        await store.append(
            run_id=run_id,
            mode="paper",
            started_at=started_at,
            observed_at=observed_at,
            strategy_fingerprint="strategy-fingerprint",
            component_status=component_status,
        )

    continuity = await store.continuity(
        run_id=run_id,
        observed_until=observed_until,
    )

    assert continuity is not None
    assert continuity.heartbeat_count == 2
    assert continuity.last_observed_at == observed_until
    assert continuity.unhealthy_heartbeat_count == 0
    assert continuity.min_controller_runtimes == 1
    assert continuity.healthy_days == 1


@pytest.mark.asyncio(loop_scope="session")
async def test_runtime_continuity_measures_healthy_days_to_report_clock(
    pg_pool: asyncpg.Pool,
) -> None:
    run_id = "runtime-heartbeat-report-clock-days"
    started_at = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
    last_observed_at = started_at + timedelta(days=30) - timedelta(seconds=60)
    observed_until = started_at + timedelta(days=30)
    store = RuntimeHeartbeatStore(pg_pool)
    async with pg_pool.acquire() as connection:
        await connection.execute(
            "DELETE FROM runtime_heartbeats WHERE run_id = $1",
            run_id,
        )

    for observed_at in (started_at, last_observed_at):
        await store.append(
            run_id=run_id,
            mode="paper",
            started_at=started_at,
            observed_at=observed_at,
            strategy_fingerprint="strategy-fingerprint",
            component_status={"running": True, "sensor_tasks": 1, "controller_runtimes": 1},
        )

    continuity = await store.continuity(
        run_id=run_id,
        observed_until=observed_until,
    )

    assert continuity is not None
    assert continuity.last_observed_at == last_observed_at
    assert continuity.healthy_days == 30
    assert continuity.max_gap_seconds == pytest.approx(2_592_000.0 - 60.0)
