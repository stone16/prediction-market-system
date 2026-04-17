from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass, field
from typing import Any

import pytest


class _TestAsyncpgConnectionContext:
    def __init__(self, pool: "_TestAsyncpgPool") -> None:
        self._pool = pool

    async def __aenter__(self) -> object:
        await self._pool._release_acquires.wait()
        return object()

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        return None


@dataclass
class _TestAsyncpgPool:
    close_calls: int = 0
    closed: bool = False
    _release_acquires: asyncio.Event = field(default_factory=asyncio.Event)

    async def close(self) -> None:
        self.close_calls += 1
        self.closed = True
        self._release_acquires.set()

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
