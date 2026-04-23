"""Add denormalized current price fields to markets."""

from __future__ import annotations

from alembic import op
from sqlalchemy.engine import Connection


revision = "0006_markets_price_fields"
down_revision = "0005_strategies_share_metadata"
branch_labels = None
depends_on = None


MARKETS_PRICE_COLUMNS_SQL = """
ALTER TABLE markets
  ADD COLUMN yes_price NUMERIC(6,4),
  ADD COLUMN no_price NUMERIC(6,4),
  ADD COLUMN best_bid NUMERIC(6,4),
  ADD COLUMN best_ask NUMERIC(6,4),
  ADD COLUMN last_trade_price NUMERIC(6,4),
  ADD COLUMN liquidity NUMERIC,
  ADD COLUMN spread_bps INTEGER,
  ADD COLUMN price_updated_at TIMESTAMPTZ
"""

MARKETS_PRICE_UPDATED_INDEX_SQL = """
CREATE INDEX idx_markets_price_updated_at
  ON markets (price_updated_at DESC)
  WHERE price_updated_at IS NOT NULL
"""

DROP_MARKETS_PRICE_COLUMNS_SQL = """
        ALTER TABLE markets
          DROP COLUMN IF EXISTS price_updated_at,
          DROP COLUMN IF EXISTS spread_bps,
          DROP COLUMN IF EXISTS liquidity,
          DROP COLUMN IF EXISTS last_trade_price,
          DROP COLUMN IF EXISTS best_ask,
          DROP COLUMN IF EXISTS best_bid,
          DROP COLUMN IF EXISTS no_price,
          DROP COLUMN IF EXISTS yes_price
        """


def upgrade() -> None:
    connection: Connection = op.get_bind()
    connection.exec_driver_sql(MARKETS_PRICE_COLUMNS_SQL)
    connection.exec_driver_sql(MARKETS_PRICE_UPDATED_INDEX_SQL)


def downgrade() -> None:
    connection: Connection = op.get_bind()
    connection.exec_driver_sql("DROP INDEX IF EXISTS idx_markets_price_updated_at")
    connection.exec_driver_sql(DROP_MARKETS_PRICE_COLUMNS_SQL)
