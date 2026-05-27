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
    / "0018_strategy_run_slices.py"
)


class _FakeConnection:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def exec_driver_sql(self, statement: str) -> None:
        self.statements.append(statement)


def _load_migration_module() -> ModuleType:
    assert MIGRATION_PATH.exists(), f"migration file missing: {MIGRATION_PATH}"
    module_name = "test_alembic_0018_strategy_run_slices"
    sys.modules.pop(module_name, None)
    spec = importlib.util.spec_from_file_location(module_name, MIGRATION_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def test_strategy_run_slices_migration_declares_revision_metadata() -> None:
    module = _load_migration_module()

    assert module.revision == "0018_strategy_run_slices"
    assert module.down_revision == "0017_eval_brier_baseline"
    assert callable(module.upgrade)
    assert callable(module.downgrade)


def test_strategy_run_slices_migration_creates_inner_ring_table_and_indexes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_migration_module()
    fake_connection = _FakeConnection()
    monkeypatch.setattr(module.op, "get_bind", lambda: fake_connection)

    module.upgrade()

    statements = "\n".join(fake_connection.statements)
    assert "CREATE TABLE IF NOT EXISTS strategy_run_slices" in statements
    assert "strategy_id TEXT NOT NULL" in statements
    assert "strategy_version_id TEXT NOT NULL" in statements
    assert "slice_label TEXT NOT NULL" in statements
    assert "slice_start TIMESTAMPTZ NOT NULL" in statements
    assert "slice_end TIMESTAMPTZ NOT NULL" in statements
    assert "slice_kind TEXT NOT NULL" in statements
    assert "CONSTRAINT strategy_run_slices_strategy_identity_check" in statements
    assert "CONSTRAINT strategy_run_slices_window_check" in statements
    assert "idx_strategy_run_slices_run_strategy_identity" in statements


def test_strategy_run_slices_migration_downgrade_drops_table(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_migration_module()
    fake_connection = _FakeConnection()
    monkeypatch.setattr(module.op, "get_bind", lambda: fake_connection)

    module.downgrade()

    assert fake_connection.statements == ["DROP TABLE IF EXISTS strategy_run_slices"]
