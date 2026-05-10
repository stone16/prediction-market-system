"""Add market relation cache."""

from __future__ import annotations

from alembic import op
from sqlalchemy.engine import Connection


revision = "0016_market_relations"
down_revision = "0015_strategy_meta_evidence"
branch_labels = None
depends_on = None


MARKET_RELATIONS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS market_relations (
    id SERIAL PRIMARY KEY,
    market_id_a TEXT NOT NULL,
    market_id_b TEXT NOT NULL,
    relation_type TEXT NOT NULL CHECK (
        relation_type IN ('subset', 'contradiction', 'independent', 'similar')
    ),
    confidence FLOAT NOT NULL,
    detected_at TIMESTAMPTZ NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb
)
"""

MARKET_RELATIONS_PAIR_TYPE_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_market_relations_pair_type
    ON market_relations (market_id_a, market_id_b, relation_type)
"""


def upgrade() -> None:
    connection: Connection = op.get_bind()
    connection.exec_driver_sql(MARKET_RELATIONS_TABLE_SQL)
    connection.exec_driver_sql(MARKET_RELATIONS_PAIR_TYPE_INDEX_SQL)


def downgrade() -> None:
    connection: Connection = op.get_bind()
    connection.exec_driver_sql("DROP TABLE IF EXISTS market_relations")
