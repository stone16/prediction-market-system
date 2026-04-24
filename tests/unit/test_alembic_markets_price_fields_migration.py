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
    / "0006_markets_price_fields.py"
)


def _load_migration_module() -> ModuleType:
    assert MIGRATION_PATH.exists(), f"migration file missing: {MIGRATION_PATH}"
    module_name = "test_alembic_0006_markets_price_fields"
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


def test_markets_price_fields_migration_declares_expected_revision_metadata() -> None:
    module = _load_migration_module()

    assert module.revision == "0006_markets_price_fields"
    assert module.down_revision == "0005_strategies_share_metadata"
    assert callable(module.upgrade)
    assert callable(module.downgrade)


def test_markets_price_fields_migration_upgrade_adds_nullable_columns_and_index(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_migration_module()
    fake_connection = _FakeConnection()
    monkeypatch.setattr(module.op, "get_bind", lambda: fake_connection)

    module.upgrade()

    assert len(fake_connection.statements) == 2
    alter_statement = fake_connection.statements[0]
    assert "ALTER TABLE markets" in alter_statement
    assert "ADD COLUMN IF NOT EXISTS yes_price NUMERIC(6,4)" in alter_statement
    assert "ADD COLUMN IF NOT EXISTS no_price NUMERIC(6,4)" in alter_statement
    assert "ADD COLUMN IF NOT EXISTS best_bid NUMERIC(6,4)" in alter_statement
    assert "ADD COLUMN IF NOT EXISTS best_ask NUMERIC(6,4)" in alter_statement
    assert (
        "ADD COLUMN IF NOT EXISTS last_trade_price NUMERIC(6,4)" in alter_statement
    )
    assert "ADD COLUMN IF NOT EXISTS liquidity NUMERIC" in alter_statement
    assert "ADD COLUMN IF NOT EXISTS spread_bps INTEGER" in alter_statement
    assert "ADD COLUMN IF NOT EXISTS price_updated_at TIMESTAMPTZ" in alter_statement
    assert "NOT NULL" not in alter_statement

    index_statement = fake_connection.statements[1]
    assert "CREATE INDEX IF NOT EXISTS idx_markets_price_updated_at" in index_statement
    assert "ON markets (price_updated_at DESC)" in index_statement
    assert "WHERE price_updated_at IS NOT NULL" in index_statement


def test_markets_price_fields_migration_downgrade_reverses_changes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_migration_module()
    fake_connection = _FakeConnection()
    monkeypatch.setattr(module.op, "get_bind", lambda: fake_connection)

    module.downgrade()

    assert fake_connection.statements == [
        "DROP INDEX IF EXISTS idx_markets_price_updated_at",
        """
        ALTER TABLE markets
          DROP COLUMN IF EXISTS price_updated_at,
          DROP COLUMN IF EXISTS spread_bps,
          DROP COLUMN IF EXISTS liquidity,
          DROP COLUMN IF EXISTS last_trade_price,
          DROP COLUMN IF EXISTS best_ask,
          DROP COLUMN IF EXISTS best_bid,
          DROP COLUMN IF EXISTS no_price,
          DROP COLUMN IF EXISTS yes_price
        """,
    ]
