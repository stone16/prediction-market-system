from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import pytest


VERSIONS_DIR = Path(__file__).resolve().parents[2] / "alembic" / "versions"
SNAPSHOTS_MIGRATION_PATH = VERSIONS_DIR / "0007_market_price_snapshots.py"
SUBSCRIPTIONS_MIGRATION_PATH = VERSIONS_DIR / "0008_market_subscriptions.py"


def _load_migration_module(path: Path, module_name: str) -> ModuleType:
    assert path.exists(), f"migration file missing: {path}"
    sys.modules.pop(module_name, None)
    spec = importlib.util.spec_from_file_location(module_name, path)
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


def test_market_price_snapshots_migration_shape(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_migration_module(
        SNAPSHOTS_MIGRATION_PATH,
        "test_alembic_0007_market_price_snapshots",
    )
    fake_connection = _FakeConnection()
    monkeypatch.setattr(module.op, "get_bind", lambda: fake_connection)

    assert module.revision == "0007_market_price_snapshots"
    assert module.down_revision == "0006_markets_price_fields"

    module.upgrade()

    assert len(fake_connection.statements) == 2
    create_table = fake_connection.statements[0]
    assert "CREATE TABLE IF NOT EXISTS market_price_snapshots" in create_table
    assert "condition_id TEXT NOT NULL" in create_table
    assert "snapshot_at TIMESTAMPTZ NOT NULL" in create_table
    assert "yes_price NUMERIC(6,4)" in create_table
    assert "no_price NUMERIC(6,4)" in create_table
    assert "best_bid NUMERIC(6,4)" in create_table
    assert "best_ask NUMERIC(6,4)" in create_table
    assert "last_trade_price NUMERIC(6,4)" in create_table
    assert "liquidity NUMERIC" in create_table
    assert "volume_24h NUMERIC" in create_table
    assert "PRIMARY KEY (condition_id, snapshot_at)" in create_table
    assert (
        "FOREIGN KEY (condition_id) REFERENCES markets(condition_id) ON DELETE CASCADE"
        in create_table
    )

    create_index = fake_connection.statements[1]
    assert "CREATE INDEX IF NOT EXISTS idx_price_snapshots_recent" in create_index
    assert "ON market_price_snapshots (condition_id, snapshot_at DESC)" in create_index

    fake_connection.statements.clear()
    module.downgrade()
    assert fake_connection.statements == [
        "DROP TABLE IF EXISTS market_price_snapshots CASCADE"
    ]


def test_market_subscriptions_migration_shape(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_migration_module(
        SUBSCRIPTIONS_MIGRATION_PATH,
        "test_alembic_0008_market_subscriptions",
    )
    fake_connection = _FakeConnection()
    monkeypatch.setattr(module.op, "get_bind", lambda: fake_connection)

    assert module.revision == "0008_market_subscriptions"
    assert module.down_revision == "0007_market_price_snapshots"

    module.upgrade()

    assert fake_connection.statements == [
        """
CREATE TABLE IF NOT EXISTS market_subscriptions (
  token_id TEXT NOT NULL PRIMARY KEY,
  source TEXT NOT NULL CHECK (source IN ('user')),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  FOREIGN KEY (token_id) REFERENCES tokens(token_id) ON DELETE CASCADE
)
"""
    ]

    fake_connection.statements.clear()
    module.downgrade()
    assert fake_connection.statements == [
        "DROP TABLE IF EXISTS market_subscriptions CASCADE"
    ]
