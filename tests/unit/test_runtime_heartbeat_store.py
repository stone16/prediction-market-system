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
                    "first_observed_at": datetime(2026, 1, 1, 23, 30, tzinfo=UTC),
                    "last_observed_at": datetime(2026, 1, 31, 23, 29, tzinfo=UTC),
                    "heartbeat_count": 2,
                    "max_gap_seconds": 60.0,
                }
            ),
        )
    )

    continuity = await store.continuity(run_id="run-near-midnight")

    assert continuity is not None
    assert continuity.healthy_days == 29


class _Pool:
    def __init__(self, row: Mapping[str, object]) -> None:
        self._row = row

    def acquire(self) -> _AcquireContext:
        return _AcquireContext(_Connection(self._row))


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
    def __init__(self, row: Mapping[str, object]) -> None:
        self._row = row

    async def fetchrow(self, query: str, run_id: str) -> Mapping[str, object]:
        del query, run_id
        return self._row
