from __future__ import annotations

import importlib.util
from pathlib import Path
from types import ModuleType
from typing import Any


MIGRATION_PATH = (
    Path(__file__).resolve().parents[2]
    / "alembic"
    / "versions"
    / "0016_market_relations.py"
)


class _FakeConnection:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def exec_driver_sql(self, statement: str) -> None:
        self.statements.append(statement)


def _load_migration() -> ModuleType:
    spec = importlib.util.spec_from_file_location(
        "test_alembic_0016_market_relations",
        MIGRATION_PATH,
    )
    if spec is None or spec.loader is None:
        raise AssertionError("could not load market relations migration")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_market_relations_migration_declares_revision_metadata() -> None:
    module = _load_migration()

    assert module.revision == "0016_market_relations"
    assert module.down_revision == "0015_strategy_meta_evidence"
    assert callable(module.upgrade)
    assert callable(module.downgrade)


def test_market_relations_migration_creates_middle_ring_table_and_index(
    monkeypatch: Any,
) -> None:
    module = _load_migration()
    connection = _FakeConnection()
    monkeypatch.setattr(module.op, "get_bind", lambda: connection)

    module.upgrade()

    combined_sql = "\n".join(connection.statements)
    assert "CREATE TABLE IF NOT EXISTS market_relations" in combined_sql
    assert "id SERIAL PRIMARY KEY" in combined_sql
    assert "market_id_a TEXT NOT NULL" in combined_sql
    assert "market_id_b TEXT NOT NULL" in combined_sql
    assert "relation_type TEXT NOT NULL" in combined_sql
    assert "confidence FLOAT NOT NULL" in combined_sql
    assert "detected_at TIMESTAMPTZ NOT NULL" in combined_sql
    assert "metadata JSONB" in combined_sql
    assert "strategy_id" not in combined_sql
    assert "idx_market_relations_pair_type" in combined_sql
    assert "(market_id_a, market_id_b, relation_type)" in combined_sql


def test_market_relations_migration_downgrade_drops_table(monkeypatch: Any) -> None:
    module = _load_migration()
    connection = _FakeConnection()
    monkeypatch.setattr(module.op, "get_bind", lambda: connection)

    module.downgrade()

    assert connection.statements == ["DROP TABLE IF EXISTS market_relations"]
