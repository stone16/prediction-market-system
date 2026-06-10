from __future__ import annotations

from collections.abc import Sequence

import asyncpg
import pytest

from tests.integration import conftest as integration_conftest


class _ConnectionContext:
    def __init__(self, connection: "_RetryingConnection") -> None:
        self._connection = connection

    async def __aenter__(self) -> "_RetryingConnection":
        return self._connection

    async def __aexit__(
        self,
        exc_type: object,
        exc: object,
        traceback: object,
    ) -> None:
        return None


class _RetryingPool:
    def __init__(self, connection: "_RetryingConnection") -> None:
        self._connection = connection

    def acquire(self) -> _ConnectionContext:
        return _ConnectionContext(self._connection)


class _RetryingConnection:
    def __init__(self, failures: Sequence[BaseException]) -> None:
        self._failures = list(failures)
        self.execute_calls = 0

    async def fetch(self, query: str) -> list[dict[str, str]]:
        assert "FROM pg_tables" in query
        return [{"tablename": "markets"}, {"tablename": "tokens"}]

    async def execute(self, query: str) -> str:
        assert query == 'TRUNCATE TABLE "markets", "tokens" RESTART IDENTITY CASCADE'
        self.execute_calls += 1
        if self._failures:
            raise self._failures.pop(0)
        return "TRUNCATE TABLE"


@pytest.mark.asyncio
async def test_truncate_public_tables_retries_transient_deadlock() -> None:
    connection = _RetryingConnection([asyncpg.exceptions.DeadlockDetectedError("boom")])
    pool = _RetryingPool(connection)

    await integration_conftest._truncate_public_tables(pool)

    assert connection.execute_calls == 2


@pytest.mark.asyncio
async def test_truncate_public_tables_stops_after_retry_budget() -> None:
    connection = _RetryingConnection(
        [
            asyncpg.exceptions.LockNotAvailableError("locked"),
            asyncpg.exceptions.LockNotAvailableError("locked"),
            asyncpg.exceptions.LockNotAvailableError("locked"),
            asyncpg.exceptions.LockNotAvailableError("locked"),
        ]
    )
    pool = _RetryingPool(connection)

    with pytest.raises(asyncpg.exceptions.LockNotAvailableError):
        await integration_conftest._truncate_public_tables(pool)

    assert connection.execute_calls == 4
