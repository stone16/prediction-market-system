"""Add the durable decisions table for accept-idea lifecycle state.

The decisions table is an inner-ring lifecycle shell. The full TradeDecision
payload remains in a sidecar table managed by DecisionStore so the accept-flow
checkpoint can read durable decisions without widening this shell now.
"""

from __future__ import annotations

from typing import Final

from alembic import op
from sqlalchemy.engine import Connection


revision = "0004_decisions_table"
down_revision = "0003_order_intents"
branch_labels = None
depends_on = None

DECISIONS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS decisions (
    decision_id TEXT PRIMARY KEY,
    opportunity_id TEXT NOT NULL,
    strategy_id TEXT NOT NULL,
    strategy_version_id TEXT NOT NULL,
    status TEXT NOT NULL
        CHECK (status IN ('pending', 'accepted', 'rejected', 'expired')),
    factor_snapshot_hash TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT decisions_strategy_tags_nonempty
        CHECK (strategy_id != '' AND strategy_version_id != '')
)
"""

DECISIONS_INDEXES_SQL: Final[tuple[str, ...]] = (
    """
    CREATE INDEX IF NOT EXISTS idx_decisions_status_created
        ON decisions (status, created_at DESC)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_decisions_strategy_version
        ON decisions (strategy_id, strategy_version_id)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_decisions_opportunity
        ON decisions (opportunity_id)
    """,
)


def upgrade() -> None:
    connection: Connection = op.get_bind()
    connection.exec_driver_sql(DECISIONS_TABLE_SQL)
    for statement in DECISIONS_INDEXES_SQL:
        connection.exec_driver_sql(statement)


def downgrade() -> None:
    connection: Connection = op.get_bind()
    connection.exec_driver_sql("DROP TABLE IF EXISTS decisions CASCADE")
