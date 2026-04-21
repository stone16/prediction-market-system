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
    / "0003_order_intents.py"
)


def _load_migration_module() -> ModuleType:
    assert MIGRATION_PATH.exists(), f"migration file missing: {MIGRATION_PATH}"
    module_name = "test_alembic_0003_order_intents"
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


def test_order_intents_migration_declares_expected_revision_metadata() -> None:
    module = _load_migration_module()

    assert module.revision == "0003_order_intents"
    assert module.down_revision == "0002_unit_split"
    assert callable(module.upgrade)
    assert callable(module.downgrade)
    assert module.ORDER_INTENT_OUTCOMES == (
        "matched",
        "invalid",
        "rejected",
        "venue_rejection",
        "cancelled_ttl",
        "cancelled_limit_invalidated",
        "cancelled_session_end",
    )


def test_order_intents_migration_upgrade_creates_expected_table_and_indexes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_migration_module()
    fake_connection = _FakeConnection()
    monkeypatch.setattr(module.op, "get_bind", lambda: fake_connection)

    module.upgrade()

    assert len(fake_connection.statements) == 3
    assert "CREATE TABLE IF NOT EXISTS order_intents" in fake_connection.statements[0]
    assert "decision_id TEXT PRIMARY KEY" in fake_connection.statements[0]
    assert "CHECK (strategy_id != '')" in fake_connection.statements[0]
    assert "CHECK (strategy_version_id != '')" in fake_connection.statements[0]
    assert "CHECK (outcome IS NULL OR outcome IN ('matched', 'invalid', 'rejected', 'venue_rejection', 'cancelled_ttl', 'cancelled_limit_invalidated', 'cancelled_session_end'))" in fake_connection.statements[0]
    assert "idx_order_intents_strategy_acquired_at_desc" in fake_connection.statements[1]
    assert "strategy_id, acquired_at DESC" in fake_connection.statements[1]
    assert "idx_order_intents_released_at_nulls_first" in fake_connection.statements[2]
    assert "released_at NULLS FIRST" in fake_connection.statements[2]


def test_order_intents_migration_downgrade_drops_table(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_migration_module()
    fake_connection = _FakeConnection()
    monkeypatch.setattr(module.op, "get_bind", lambda: fake_connection)

    module.downgrade()

    assert fake_connection.statements == ["DROP TABLE IF EXISTS order_intents"]
