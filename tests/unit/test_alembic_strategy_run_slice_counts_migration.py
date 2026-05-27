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
    / "0019_strategy_run_slice_counts.py"
)


class _FakeConnection:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def exec_driver_sql(self, statement: str) -> None:
        self.statements.append(statement)


def _load_migration_module() -> ModuleType:
    assert MIGRATION_PATH.exists(), f"migration file missing: {MIGRATION_PATH}"
    module_name = "test_alembic_0019_strategy_run_slice_counts"
    sys.modules.pop(module_name, None)
    spec = importlib.util.spec_from_file_location(module_name, MIGRATION_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def test_strategy_run_slice_counts_migration_declares_revision_metadata() -> None:
    module = _load_migration_module()

    assert module.revision == "0019_strategy_run_slice_counts"
    assert module.down_revision == "0018_strategy_run_slices"
    assert callable(module.upgrade)
    assert callable(module.downgrade)


def test_strategy_run_slice_counts_migration_adds_count_columns_and_check(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_migration_module()
    fake_connection = _FakeConnection()
    monkeypatch.setattr(module.op, "get_bind", lambda: fake_connection)

    module.upgrade()

    statements = "\n".join(fake_connection.statements)
    assert "ALTER TABLE strategy_run_slices" in statements
    assert "ADD COLUMN IF NOT EXISTS opportunity_count INTEGER NOT NULL DEFAULT 0" in statements
    assert "ADD COLUMN IF NOT EXISTS decision_count INTEGER NOT NULL DEFAULT 0" in statements
    assert "ADD COLUMN IF NOT EXISTS fill_count INTEGER NOT NULL DEFAULT 0" in statements
    assert "CONSTRAINT strategy_run_slices_counts_check" in statements
    assert "opportunity_count >= 0" in statements
    assert "decision_count >= 0" in statements
    assert "fill_count >= 0" in statements


def test_strategy_run_slice_counts_migration_downgrade_drops_count_columns(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_migration_module()
    fake_connection = _FakeConnection()
    monkeypatch.setattr(module.op, "get_bind", lambda: fake_connection)

    module.downgrade()

    statements = "\n".join(fake_connection.statements)
    assert "DROP CONSTRAINT IF EXISTS strategy_run_slices_counts_check" in statements
    assert "DROP COLUMN IF EXISTS fill_count" in statements
    assert "DROP COLUMN IF EXISTS decision_count" in statements
    assert "DROP COLUMN IF EXISTS opportunity_count" in statements
