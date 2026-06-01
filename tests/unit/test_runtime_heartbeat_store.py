from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any, cast

import pytest

from pms.storage.runtime_heartbeat_store import RuntimeHeartbeatStore


@pytest.mark.asyncio
async def test_runtime_continuity_counts_only_elapsed_healthy_days() -> None:
    store = RuntimeHeartbeatStore(
        pool=cast(
            Any,
            _Pool(
                {
                    "first_started_at": datetime(2026, 1, 1, 23, 29, tzinfo=UTC),
                    "first_observed_at": datetime(2026, 1, 1, 23, 30, tzinfo=UTC),
                    "last_observed_at": datetime(2026, 1, 31, 23, 29, tzinfo=UTC),
                    "heartbeat_count": 2,
                    "max_gap_seconds": 60.0,
                    "unhealthy_heartbeat_count": 0,
                    "min_controller_runtimes": 1,
                }
            ),
        )
    )

    continuity = await store.continuity(run_id="run-near-midnight")

    assert continuity is not None
    assert continuity.healthy_days == 30


@pytest.mark.asyncio
async def test_runtime_continuity_query_counts_initial_heartbeat_gap() -> None:
    pool = _Pool(
        {
            "first_started_at": datetime(2026, 1, 1, 0, 0, tzinfo=UTC),
            "first_observed_at": datetime(2026, 1, 31, 0, 0, tzinfo=UTC),
            "last_observed_at": datetime(2026, 1, 31, 0, 0, tzinfo=UTC),
            "heartbeat_count": 1,
            "max_gap_seconds": 2_592_000.0,
            "unhealthy_heartbeat_count": 0,
            "min_controller_runtimes": 1,
        }
    )
    store = RuntimeHeartbeatStore(pool=cast(Any, pool))

    continuity = await store.continuity(run_id="run-late-heartbeat")

    assert continuity is not None
    assert continuity.healthy_days == 30
    assert continuity.max_gap_seconds == 2_592_000.0
    assert "MIN(observed_at) - MIN(started_at)" in pool.last_query


@pytest.mark.asyncio
async def test_runtime_continuity_reports_unhealthy_controller_heartbeats() -> None:
    pool = _Pool(
        {
            "first_started_at": datetime(2026, 1, 1, 0, 0, tzinfo=UTC),
            "first_observed_at": datetime(2026, 1, 1, 0, 1, tzinfo=UTC),
            "last_observed_at": datetime(2026, 1, 31, 0, 0, tzinfo=UTC),
            "heartbeat_count": 30,
            "max_gap_seconds": 60.0,
            "unhealthy_heartbeat_count": 1,
            "min_controller_runtimes": 0,
        }
    )
    store = RuntimeHeartbeatStore(pool=cast(Any, pool))

    continuity = await store.continuity(run_id="run-controller-gap")

    assert continuity is not None
    assert continuity.unhealthy_heartbeat_count == 1
    assert continuity.min_controller_runtimes == 0
    assert "controller_runtimes" in pool.last_query


class _Pool:
    def __init__(self, row: Mapping[str, object]) -> None:
        self._row = row
        self.last_query = ""

    def acquire(self) -> _AcquireContext:
        return _AcquireContext(_Connection(self))


class _AcquireContext:
    def __init__(self, connection: _Connection) -> None:
        self._connection = connection

    async def __aenter__(self) -> _Connection:
        return self._connection

    async def __aexit__(
        self,
        exc_type: object,
        exc: object,
        traceback: object,
    ) -> None:
        return None


class _Connection:
    def __init__(self, pool: _Pool) -> None:
        self._pool = pool

    async def fetchrow(self, query: str, run_id: str) -> Mapping[str, object]:
        del run_id
        self._pool.last_query = query
        return self._pool._row
