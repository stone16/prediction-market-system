"""Add market price snapshots table."""

from __future__ import annotations

from alembic import op
from sqlalchemy.engine import Connection


revision = "0007_market_price_snapshots"
down_revision = "0006_markets_price_fields"
branch_labels = None
depends_on = None


MARKET_PRICE_SNAPSHOTS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS market_price_snapshots (
  condition_id TEXT NOT NULL,
  snapshot_at TIMESTAMPTZ NOT NULL,
  yes_price NUMERIC(6,4),
  no_price NUMERIC(6,4),
  best_bid NUMERIC(6,4),
  best_ask NUMERIC(6,4),
  last_trade_price NUMERIC(6,4),
  liquidity NUMERIC,
  volume_24h NUMERIC,
  PRIMARY KEY (condition_id, snapshot_at),
  FOREIGN KEY (condition_id) REFERENCES markets(condition_id) ON DELETE CASCADE
)
"""

PRICE_SNAPSHOTS_RECENT_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_price_snapshots_recent
  ON market_price_snapshots (condition_id, snapshot_at DESC)
"""


def upgrade() -> None:
    connection: Connection = op.get_bind()
    connection.exec_driver_sql(MARKET_PRICE_SNAPSHOTS_TABLE_SQL)
    connection.exec_driver_sql(PRICE_SNAPSHOTS_RECENT_INDEX_SQL)


def downgrade() -> None:
    connection: Connection = op.get_bind()
    connection.exec_driver_sql("DROP TABLE IF EXISTS market_price_snapshots CASCADE")
