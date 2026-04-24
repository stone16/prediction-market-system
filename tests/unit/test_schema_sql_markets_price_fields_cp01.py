from __future__ import annotations

from pathlib import Path


SCHEMA_SQL_PATH = Path(__file__).resolve().parents[2] / "schema.sql"


def test_schema_sql_declares_markets_current_price_fields_and_index() -> None:
    schema_sql = SCHEMA_SQL_PATH.read_text()

    assert "ADD COLUMN IF NOT EXISTS yes_price NUMERIC(6,4)" in schema_sql
    assert "ADD COLUMN IF NOT EXISTS no_price NUMERIC(6,4)" in schema_sql
    assert "ADD COLUMN IF NOT EXISTS best_bid NUMERIC(6,4)" in schema_sql
    assert "ADD COLUMN IF NOT EXISTS best_ask NUMERIC(6,4)" in schema_sql
    assert "ADD COLUMN IF NOT EXISTS last_trade_price NUMERIC(6,4)" in schema_sql
    assert "ADD COLUMN IF NOT EXISTS liquidity NUMERIC" in schema_sql
    assert "ADD COLUMN IF NOT EXISTS spread_bps INTEGER" in schema_sql
    assert "ADD COLUMN IF NOT EXISTS price_updated_at TIMESTAMPTZ" in schema_sql
    assert "CREATE INDEX IF NOT EXISTS idx_markets_price_updated_at" in schema_sql
    assert "ON markets (price_updated_at DESC)" in schema_sql
    assert "WHERE price_updated_at IS NOT NULL" in schema_sql
