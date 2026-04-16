from __future__ import annotations

import asyncio
from dataclasses import dataclass, field

import pytest


@dataclass
class _TestAsyncpgPool:
    close_calls: int = 0
    closed: bool = False
    _release_acquires: asyncio.Event = field(default_factory=asyncio.Event)

    async def close(self) -> None:
        self.close_calls += 1
        self.closed = True
        self._release_acquires.set()

    async def acquire(self) -> object:
        await self._release_acquires.wait()
        return object()


@pytest.fixture(autouse=True)
def _stub_runner_asyncpg_pool(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_create_pool(*, dsn: str, min_size: int, max_size: int) -> _TestAsyncpgPool:
        return _TestAsyncpgPool()

    monkeypatch.setattr("pms.runner.asyncpg.create_pool", fake_create_pool)
