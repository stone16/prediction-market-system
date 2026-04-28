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
    / "0014_strategy_artifacts.py"
)


def _load_migration_module() -> ModuleType:
    assert MIGRATION_PATH.exists(), f"migration file missing: {MIGRATION_PATH}"
    module_name = "test_alembic_0014_strategy_artifacts"
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


def test_strategy_artifacts_migration_declares_revision_metadata() -> None:
    module = _load_migration_module()

    assert module.revision == "0014_strategy_artifacts"
    assert module.down_revision == "0013_market_status_fields"
    assert callable(module.upgrade)
    assert callable(module.downgrade)


def test_strategy_artifacts_migration_creates_inner_ring_tables_and_indexes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_migration_module()
    fake_connection = _FakeConnection()
    monkeypatch.setattr(module.op, "get_bind", lambda: fake_connection)

    module.upgrade()

    assert len(fake_connection.statements) == 6
    judgement_table = fake_connection.statements[0]
    execution_table = fake_connection.statements[1]
    assert "CREATE TABLE IF NOT EXISTS strategy_judgement_artifacts" in judgement_table
    assert "artifact_id TEXT PRIMARY KEY" in judgement_table
    assert "strategy_id TEXT NOT NULL" in judgement_table
    assert "strategy_version_id TEXT NOT NULL" in judgement_table
    assert "CHECK (strategy_id <> '' AND strategy_version_id <> '')" in judgement_table
    assert "CHECK (char_length(judgement_summary) <= 4000)" in judgement_table
    assert "artifact_type IN ('approved_intent', 'rejected_candidate')" in judgement_table

    assert "CREATE TABLE IF NOT EXISTS strategy_execution_artifacts" in execution_table
    assert "artifact_id TEXT PRIMARY KEY" in execution_table
    assert "strategy_id TEXT NOT NULL" in execution_table
    assert "strategy_version_id TEXT NOT NULL" in execution_table
    assert "CHECK (strategy_id <> '' AND strategy_version_id <> '')" in execution_table
    assert (
        "artifact_type IN ('accepted_execution_plan', 'rejected_execution_plan')"
        in execution_table
    )

    assert "idx_strategy_judgement_artifacts_strategy_created_at" in (
        fake_connection.statements[2]
    )
    assert "strategy_id, strategy_version_id, created_at DESC" in (
        fake_connection.statements[2]
    )
    assert "idx_strategy_judgement_artifacts_candidate" in fake_connection.statements[3]
    assert "idx_strategy_execution_artifacts_strategy_created_at" in (
        fake_connection.statements[4]
    )
    assert "idx_strategy_execution_artifacts_intent" in fake_connection.statements[5]


def test_strategy_artifacts_migration_downgrade_drops_tables(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_migration_module()
    fake_connection = _FakeConnection()
    monkeypatch.setattr(module.op, "get_bind", lambda: fake_connection)

    module.downgrade()

    assert fake_connection.statements == [
        "DROP TABLE IF EXISTS strategy_execution_artifacts",
        "DROP TABLE IF EXISTS strategy_judgement_artifacts",
    ]
