from __future__ import annotations

import importlib
from typing import Any

import asyncpg
import pytest
from alembic.script import ScriptDirectory


class _FakeConnection:
    def __init__(self, *, result: object = None, error: BaseException | None = None) -> None:
        self._result = result
        self._error = error
        self.queries: list[str] = []

    async def fetchval(self, query: str) -> object:
        self.queries.append(query)
        if self._error is not None:
            raise self._error
        return self._result


class _FakePool:
    def __init__(self, connection: _FakeConnection) -> None:
        self._connection = connection
        self.acquired = 0
        self.released = 0

    async def acquire(self) -> _FakeConnection:
        self.acquired += 1
        return self._connection

    async def release(self, _connection: _FakeConnection) -> None:
        self.released += 1


@pytest.mark.asyncio
async def test_ensure_schema_current_accepts_matching_head(monkeypatch: pytest.MonkeyPatch) -> None:
    module = importlib.import_module("pms.storage.schema_check")
    monkeypatch.setattr(module, "EXPECTED_SCHEMA_HEAD", "0001_baseline")
    monkeypatch.setattr(module, "EXPECTED_SCHEMA_HEAD_ERROR", None)
    connection = _FakeConnection(result="0001_baseline")
    pool = _FakePool(connection)

    await module.ensure_schema_current(pool)

    assert connection.queries == ["SELECT version_num FROM alembic_version LIMIT 1"]
    assert pool.acquired == 1
    assert pool.released == 1


@pytest.mark.asyncio
async def test_ensure_schema_current_reports_missing_version_table(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = importlib.import_module("pms.storage.schema_check")
    monkeypatch.setattr(module, "EXPECTED_SCHEMA_HEAD", "0001_baseline")
    monkeypatch.setattr(module, "EXPECTED_SCHEMA_HEAD_ERROR", None)
    connection = _FakeConnection(error=asyncpg.UndefinedTableError("missing alembic_version"))
    pool = _FakePool(connection)

    with pytest.raises(module.SchemaVersionMismatchError) as exc_info:
        await module.ensure_schema_current(pool)

    assert "schema not initialized" in str(exc_info.value)
    assert "alembic upgrade head" in str(exc_info.value)
    assert pool.released == 1


@pytest.mark.asyncio
async def test_ensure_schema_current_reports_mismatched_head(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = importlib.import_module("pms.storage.schema_check")
    monkeypatch.setattr(module, "EXPECTED_SCHEMA_HEAD", "0001_baseline")
    monkeypatch.setattr(module, "EXPECTED_SCHEMA_HEAD_ERROR", None)
    pool = _FakePool(_FakeConnection(result="0000_previous"))

    with pytest.raises(module.SchemaVersionMismatchError) as exc_info:
        await module.ensure_schema_current(pool)

    assert "schema out of date" in str(exc_info.value)
    assert "observed 0000_previous" in str(exc_info.value)
    assert "expected 0001_baseline" in str(exc_info.value)
    assert pool.released == 1


def test_schema_check_reports_inconsistent_migration_graph(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _raise(_config: object) -> object:
        raise RuntimeError("broken graph")

    monkeypatch.setattr(ScriptDirectory, "from_config", _raise)
    module = importlib.reload(importlib.import_module("pms.storage.schema_check"))

    assert module.EXPECTED_SCHEMA_HEAD is None
    assert module.EXPECTED_SCHEMA_HEAD_ERROR is not None
    assert "broken graph" in str(module.EXPECTED_SCHEMA_HEAD_ERROR)
