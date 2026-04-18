from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

import pytest


class _TestAsyncpgConnection:
    async def execute(self, query: str, *args: object) -> str:
        del query, args
        return "INSERT 0 1"

    async def fetchrow(self, query: str, *args: object) -> None:
        del query, args
        return None

    async def fetchval(self, query: str, *args: object) -> None:
        del query, args
        return None

    def transaction(self) -> "_TestAsyncpgTransactionContext":
        return _TestAsyncpgTransactionContext()


class _TestAsyncpgTransactionContext:
    async def __aenter__(self) -> "_TestAsyncpgTransactionContext":
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        return None


class _TestAsyncpgConnectionContext:
    def __init__(self, pool: "_TestAsyncpgPool") -> None:
        self._pool = pool

    async def __aenter__(self) -> _TestAsyncpgConnection:
        del self._pool
        return _TestAsyncpgConnection()

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        return None


@dataclass
class _TestAsyncpgPool:
    close_calls: int = 0
    closed: bool = False

    async def close(self) -> None:
        self.close_calls += 1
        self.closed = True

    def acquire(self) -> _TestAsyncpgConnectionContext:
        return _TestAsyncpgConnectionContext(self)


@pytest.fixture(autouse=True)
def _stub_runner_asyncpg_pool(
    monkeypatch: pytest.MonkeyPatch,
    request: pytest.FixtureRequest,
) -> None:
    if (
        request.node.get_closest_marker("integration") is not None
        and os.environ.get("PMS_RUN_INTEGRATION") == "1"
    ):
        return

    async def fake_create_pool(*, dsn: str, min_size: int, max_size: int) -> _TestAsyncpgPool:
        return _TestAsyncpgPool()

    monkeypatch.setattr("pms.runner.asyncpg.create_pool", fake_create_pool)
