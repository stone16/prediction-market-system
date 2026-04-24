"""Add persisted user market subscriptions table."""

from __future__ import annotations

from alembic import op
from sqlalchemy.engine import Connection


revision = "0008_market_subscriptions"
down_revision = "0007_market_price_snapshots"
branch_labels = None
depends_on = None


MARKET_SUBSCRIPTIONS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS market_subscriptions (
  token_id TEXT NOT NULL PRIMARY KEY,
  source TEXT NOT NULL CHECK (source IN ('user')),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  FOREIGN KEY (token_id) REFERENCES tokens(token_id) ON DELETE CASCADE
)
"""


def upgrade() -> None:
    connection: Connection = op.get_bind()
    connection.exec_driver_sql(MARKET_SUBSCRIPTIONS_TABLE_SQL)


def downgrade() -> None:
    connection: Connection = op.get_bind()
    connection.exec_driver_sql("DROP TABLE IF EXISTS market_subscriptions CASCADE")
