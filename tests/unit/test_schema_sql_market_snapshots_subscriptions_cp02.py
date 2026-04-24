from __future__ import annotations

from pathlib import Path


SCHEMA_SQL_PATH = Path(__file__).resolve().parents[2] / "schema.sql"


def test_schema_sql_declares_market_price_snapshots_table() -> None:
    schema_sql = SCHEMA_SQL_PATH.read_text()

    assert "CREATE TABLE IF NOT EXISTS market_price_snapshots" in schema_sql
    assert "condition_id TEXT NOT NULL" in schema_sql
    assert "snapshot_at TIMESTAMPTZ NOT NULL" in schema_sql
    assert "yes_price NUMERIC(6,4)" in schema_sql
    assert "no_price NUMERIC(6,4)" in schema_sql
    assert "best_bid NUMERIC(6,4)" in schema_sql
    assert "best_ask NUMERIC(6,4)" in schema_sql
    assert "last_trade_price NUMERIC(6,4)" in schema_sql
    assert "liquidity NUMERIC" in schema_sql
    assert "volume_24h NUMERIC" in schema_sql
    assert "PRIMARY KEY (condition_id, snapshot_at)" in schema_sql
    assert (
        "FOREIGN KEY (condition_id) REFERENCES markets(condition_id) ON DELETE CASCADE"
        in schema_sql
    )
    assert "CREATE INDEX IF NOT EXISTS idx_price_snapshots_recent" in schema_sql
    assert "ON market_price_snapshots (condition_id, snapshot_at DESC)" in schema_sql


def test_schema_sql_declares_market_subscriptions_table() -> None:
    schema_sql = SCHEMA_SQL_PATH.read_text()

    assert "CREATE TABLE IF NOT EXISTS market_subscriptions" in schema_sql
    assert "token_id TEXT NOT NULL PRIMARY KEY" in schema_sql
    assert "source TEXT NOT NULL CHECK (source IN ('user'))" in schema_sql
    assert "created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()" in schema_sql
    assert (
        "FOREIGN KEY (token_id) REFERENCES tokens(token_id) ON DELETE CASCADE"
        in schema_sql
    )
