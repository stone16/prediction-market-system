from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import pytest


MIGRATION_PATH = (
    Path(__file__).resolve().parents[2]
    / "alembic"
    / "versions"
    / "0001_baseline.py"
)


def _load_migration_module() -> ModuleType:
    module_name = "test_alembic_0001_baseline"
    sys.modules.pop(module_name, None)
    spec = importlib.util.spec_from_file_location(module_name, MIGRATION_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


class _FakeConnection:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def exec_driver_sql(self, statement: str) -> None:
        self.statements.append(statement)


def test_baseline_migration_declares_expected_revision_metadata() -> None:
    module = _load_migration_module()

    assert module.revision == "0001_baseline"
    assert module.down_revision is None
    assert callable(module.upgrade)
    assert callable(module.downgrade)


def test_baseline_migration_strips_outer_transaction_wrapper() -> None:
    module = _load_migration_module()

    schema_sql = module._migration_schema_sql()

    assert not schema_sql.lstrip().startswith("BEGIN;")
    assert not schema_sql.rstrip().endswith("COMMIT;")
    assert "CREATE TABLE IF NOT EXISTS markets" in schema_sql
    assert "CREATE TABLE IF NOT EXISTS backtest_live_comparisons" in schema_sql


def test_baseline_migration_upgrade_executes_schema_sql(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_migration_module()
    fake_connection = _FakeConnection()
    monkeypatch.setattr(module.op, "get_bind", lambda: fake_connection)

    module.upgrade()

    assert len(fake_connection.statements) == 1
    assert "CREATE TABLE IF NOT EXISTS markets" in fake_connection.statements[0]
    assert not fake_connection.statements[0].lstrip().startswith("BEGIN;")


def test_baseline_migration_downgrade_drops_all_tables_in_reverse_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_migration_module()
    fake_connection = _FakeConnection()
    monkeypatch.setattr(module.op, "get_bind", lambda: fake_connection)

    module.downgrade()

    assert fake_connection.statements[0] == (
        'DROP TABLE IF EXISTS "backtest_live_comparisons" CASCADE'
    )
    assert fake_connection.statements[-1] == 'DROP TABLE IF EXISTS "markets" CASCADE'
    assert len(fake_connection.statements) == len(module._CREATED_TABLES)
