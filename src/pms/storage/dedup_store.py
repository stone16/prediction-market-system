from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
import inspect
from typing import Any, Protocol, cast

import asyncpg

from pms.core.models import TradeDecision


@dataclass(frozen=True, slots=True)
class PgDedupStore:
    pool: asyncpg.Pool

    async def acquire(self, decision: TradeDecision) -> bool:
        async with _acquire_connection(self.pool) as acquired_connection:
            connection = cast(asyncpg.Connection, acquired_connection)
            return await _insert_order_intent(
                connection,
                decision,
                worker_host=_worker_host(),
                worker_pid=_worker_pid(),
            )

    async def release(self, decision_id: str, outcome: str) -> None:
        async with _acquire_connection(self.pool) as acquired_connection:
            connection = cast(asyncpg.Connection, acquired_connection)
            await connection.execute(
                """
                UPDATE order_intents
                SET released_at = now(), outcome = $2
                WHERE decision_id = $1
                """,
                decision_id,
                outcome,
            )

    async def retention_scan(self, older_than: timedelta) -> int:
        async with _acquire_connection(self.pool) as acquired_connection:
            connection = cast(asyncpg.Connection, acquired_connection)
            cutoff = datetime.now(tz=UTC) - older_than
            rows = await connection.fetch(
                """
                DELETE FROM order_intents
                WHERE released_at IS NOT NULL
                  AND released_at < $1
                RETURNING decision_id
                """,
                cutoff,
            )
            return len(rows)


@dataclass(slots=True)
class InMemoryDedupStore:
    _entries: dict[str, _DedupEntry] = field(default_factory=dict)

    async def acquire(self, decision: TradeDecision) -> bool:
        if decision.decision_id in self._entries:
            return False

        now = datetime.now(tz=UTC)
        self._entries[decision.decision_id] = _DedupEntry(
            decision_id=decision.decision_id,
            strategy_id=decision.strategy_id,
            strategy_version_id=decision.strategy_version_id,
            acquired_at=now,
            worker_host=_worker_host(),
            worker_pid=_worker_pid(),
        )
        return True

    async def release(self, decision_id: str, outcome: str) -> None:
        entry = self._entries.get(decision_id)
        if entry is None:
            return
        self._entries[decision_id] = replace(
            entry,
            released_at=datetime.now(tz=UTC),
            outcome=outcome,
        )

    async def retention_scan(self, older_than: timedelta) -> int:
        cutoff = datetime.now(tz=UTC) - older_than
        stale_ids = [
            decision_id
            for decision_id, entry in self._entries.items()
            if entry.released_at is not None and entry.released_at < cutoff
        ]
        for decision_id in stale_ids:
            self._entries.pop(decision_id, None)
        return len(stale_ids)

    def contains(self, decision_id: str) -> bool:
        return decision_id in self._entries


@dataclass(frozen=True, slots=True)
class _DedupEntry:
    decision_id: str
    strategy_id: str
    strategy_version_id: str
    acquired_at: datetime
    released_at: datetime | None = None
    worker_host: str | None = None
    worker_pid: int | None = None
    outcome: str | None = None


class _AsyncAcquireContext(Protocol):
    async def __aenter__(self) -> object: ...

    async def __aexit__(
        self,
        exc_type: Any,
        exc: Any,
        tb: Any,
    ) -> None: ...


@asynccontextmanager
async def _acquire_connection(pool: asyncpg.Pool) -> AsyncIterator[object]:
    acquired = pool.acquire()
    if hasattr(acquired, "__aenter__") and hasattr(acquired, "__aexit__"):
        async with cast(_AsyncAcquireContext, acquired) as managed_connection:
            yield managed_connection
        return

    if not inspect.isawaitable(acquired):
        msg = "pool.acquire() must return an awaitable or async context manager"
        raise TypeError(msg)

    raw_connection: object = await acquired
    try:
        yield raw_connection
    finally:
        release = getattr(pool, "release", None)
        if callable(release):
            released = release(raw_connection)
            if inspect.isawaitable(released):
                await cast(Any, released)


async def _insert_order_intent(
    connection: asyncpg.Connection,
    decision: TradeDecision,
    *,
    worker_host: str,
    worker_pid: int,
) -> bool:
    row = await connection.fetchrow(
        """
        INSERT INTO order_intents (
            decision_id,
            strategy_id,
            strategy_version_id,
            worker_host,
            worker_pid
        ) VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (decision_id) DO NOTHING
        RETURNING decision_id
        """,
        decision.decision_id,
        decision.strategy_id,
        decision.strategy_version_id,
        worker_host,
        worker_pid,
    )
    return row is not None


def _worker_host() -> str:
    import socket

    return socket.gethostname()


def _worker_pid() -> int:
    import os

    return os.getpid()
