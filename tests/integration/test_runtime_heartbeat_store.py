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

    continuity = await store.continuity(run_id=run_id)

    assert continuity is not None
    assert continuity.healthy_days == 30
    assert continuity.heartbeat_count == 1
    assert continuity.max_gap_seconds == pytest.approx(2_592_000.0)
