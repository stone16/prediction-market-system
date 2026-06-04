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
    / "0023_backtest_execution_rows.py"
)


class _FakeConnection:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def exec_driver_sql(self, statement: str) -> None:
        self.statements.append(statement)


def _load_migration_module() -> ModuleType:
    assert MIGRATION_PATH.exists(), f"migration file missing: {MIGRATION_PATH}"
    module_name = "test_alembic_0023_backtest_execution_rows"
    sys.modules.pop(module_name, None)
    spec = importlib.util.spec_from_file_location(module_name, MIGRATION_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def test_backtest_execution_rows_migration_declares_revision_metadata() -> None:
    module = _load_migration_module()

    assert module.revision == "0023_backtest_execution_rows"
    assert module.down_revision == "0022_runtime_heartbeats"
    assert callable(module.upgrade)
    assert callable(module.downgrade)


def test_backtest_execution_rows_migration_creates_inner_ring_table(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_migration_module()
    fake_connection = _FakeConnection()
    monkeypatch.setattr(module.op, "get_bind", lambda: fake_connection)

    module.upgrade()

    statements = "\n".join(fake_connection.statements)
    assert "CREATE TABLE IF NOT EXISTS backtest_execution_rows" in statements
    assert "run_id UUID NOT NULL REFERENCES backtest_runs(run_id) ON DELETE CASCADE" in statements
    assert "decision_id TEXT NOT NULL" in statements
    assert "strategy_id TEXT NOT NULL" in statements
    assert "strategy_version_id TEXT NOT NULL" in statements
    assert "status TEXT NOT NULL" in statements
    assert "slippage_bps DOUBLE PRECISION" in statements
    assert "pnl DOUBLE PRECISION" in statements
    assert "rejection_reason TEXT" in statements
    assert "CONSTRAINT backtest_execution_rows_status_check" in statements
    assert "CONSTRAINT backtest_execution_rows_unique_decision" in statements
    assert "idx_backtest_execution_rows_run_strategy" in statements


def test_backtest_execution_rows_migration_downgrade_drops_table(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_migration_module()
    fake_connection = _FakeConnection()
    monkeypatch.setattr(module.op, "get_bind", lambda: fake_connection)

    module.downgrade()

    assert fake_connection.statements == ["DROP TABLE IF EXISTS backtest_execution_rows"]
