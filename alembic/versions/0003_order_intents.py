"""Add the order_intents idempotency table.

This table records acquisition/release metadata for controller decision
deduplication without touching the outer or middle rings.
"""

from __future__ import annotations

from typing import Final

from alembic import op
from sqlalchemy.engine import Connection


revision = "0003_order_intents"
down_revision = "0002_unit_split"
branch_labels = None
depends_on = None

ORDER_INTENT_OUTCOMES: Final[tuple[str, ...]] = (
    "matched",
    "invalid",
    "rejected",
    "venue_rejection",
    "cancelled_ttl",
    "cancelled_limit_invalidated",
    "cancelled_session_end",
)

ORDER_INTENTS_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS order_intents (
    decision_id TEXT PRIMARY KEY,
    strategy_id TEXT NOT NULL CHECK (strategy_id != ''),
    strategy_version_id TEXT NOT NULL CHECK (strategy_version_id != ''),
    acquired_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    released_at TIMESTAMPTZ,
    worker_host TEXT,
    worker_pid INTEGER,
    outcome TEXT,
    CONSTRAINT order_intents_outcome_check
        CHECK (outcome IS NULL OR outcome IN ({", ".join(f"'{outcome}'" for outcome in ORDER_INTENT_OUTCOMES)}))
)
"""

ORDER_INTENTS_INDEXES_SQL: Final[tuple[str, ...]] = (
    """
    CREATE INDEX IF NOT EXISTS idx_order_intents_strategy_acquired_at_desc
        ON order_intents (strategy_id, acquired_at DESC)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_order_intents_released_at_nulls_first
        ON order_intents (released_at NULLS FIRST)
    """,
)


def upgrade() -> None:
    connection: Connection = op.get_bind()
    connection.exec_driver_sql(ORDER_INTENTS_TABLE_SQL)
    for statement in ORDER_INTENTS_INDEXES_SQL:
        connection.exec_driver_sql(statement)


def downgrade() -> None:
    connection: Connection = op.get_bind()
    connection.exec_driver_sql('DROP TABLE IF EXISTS order_intents')
