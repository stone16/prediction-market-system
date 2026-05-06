from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from types import ModuleType

import pytest


ROOT = Path(__file__).resolve().parents[2]
MIGRATION_PATH = ROOT / "alembic" / "versions" / "0015_strategy_meta_evidence.py"


def _load_migration_module() -> ModuleType:
    assert MIGRATION_PATH.exists(), f"migration file missing: {MIGRATION_PATH}"
    module_name = "test_alembic_0015_strategy_meta_evidence"
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


def test_schema_sql_declares_strategy_meta_evidence_surfaces() -> None:
    schema_sql = (ROOT / "schema.sql").read_text(encoding="utf-8")

    assert "metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb" in schema_sql
    assert "edge_at_decision DOUBLE PRECISION NOT NULL DEFAULT 0.0" in schema_sql
    assert "spread_bps_at_decision INTEGER" in schema_sql
    assert "CREATE TABLE IF NOT EXISTS strategy_performance_peaks" in schema_sql
    assert "CREATE TABLE IF NOT EXISTS alpha_competition_snapshots" in schema_sql
    assert "UNIQUE (strategy_id, strategy_version_id, snapshot_date)" in schema_sql


def test_family_f_evalrecord_schema_fixture_declares_persisted_columns() -> None:
    fixture = ROOT / "tests" / "fixtures" / "family_f_evalrecord_schema.json"
    payload = json.loads(fixture.read_text(encoding="utf-8"))
    tables = {table["table"]: table for table in payload["tables"]}

    eval_columns = {
        column["column_name"]: column
        for column in tables["eval_records"]["columns"]
    }
    assert eval_columns["edge_at_decision"]["data_type"] == "double precision"
    assert eval_columns["edge_at_decision"]["is_nullable"] == "NO"
    assert eval_columns["spread_bps_at_decision"]["data_type"] == "integer"
    assert eval_columns["spread_bps_at_decision"]["is_nullable"] == "YES"

    strategy_version_columns = {
        column["column_name"]: column
        for column in tables["strategy_versions"]["columns"]
    }
    assert strategy_version_columns["metadata_json"]["data_type"] == "jsonb"
    assert strategy_version_columns["metadata_json"]["is_nullable"] == "NO"
    assert (
        tables["strategy_versions"]["namespace_contracts"]["promotion"]
        == "Family A writes scorecard artifacts under metadata_json.promotion"
    )


def test_strategy_meta_evidence_migration_creates_columns_and_tables(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_migration_module()
    fake_connection = _FakeConnection()
    monkeypatch.setattr(module.op, "get_bind", lambda: fake_connection)

    module.upgrade()

    statements = "\n".join(fake_connection.statements)
    assert module.revision == "0015_strategy_meta_evidence"
    assert module.down_revision == "0014_strategy_artifacts"
    assert "ALTER TABLE strategy_versions" in statements
    assert "ADD COLUMN IF NOT EXISTS metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb" in statements
    assert "ALTER TABLE eval_records" in statements
    assert "ADD COLUMN IF NOT EXISTS edge_at_decision DOUBLE PRECISION NOT NULL DEFAULT 0.0" in statements
    assert "ADD COLUMN IF NOT EXISTS spread_bps_at_decision INTEGER" in statements
    assert "CREATE TABLE IF NOT EXISTS strategy_performance_peaks" in statements
    assert "CREATE TABLE IF NOT EXISTS alpha_competition_snapshots" in statements


def test_strategy_meta_evidence_migration_downgrade_drops_surfaces(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_migration_module()
    fake_connection = _FakeConnection()
    monkeypatch.setattr(module.op, "get_bind", lambda: fake_connection)

    module.downgrade()

    statements = "\n".join(fake_connection.statements)
    assert "DROP TABLE IF EXISTS alpha_competition_snapshots" in statements
    assert "DROP TABLE IF EXISTS strategy_performance_peaks" in statements
    assert "DROP COLUMN IF EXISTS spread_bps_at_decision" in statements
    assert "DROP COLUMN IF EXISTS edge_at_decision" in statements
    assert "DROP COLUMN IF EXISTS metadata_json" in statements
