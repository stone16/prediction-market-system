"""Add market risk metadata for grouped exposure caps."""

from __future__ import annotations

from alembic import op
from sqlalchemy.engine import Connection


revision = "0021_market_risk_metadata"
down_revision = "0020_eval_secondary_baselines"
branch_labels = None
depends_on = None


def upgrade() -> None:
    connection: Connection = op.get_bind()
    connection.exec_driver_sql(
        """
        ALTER TABLE markets
            ADD COLUMN IF NOT EXISTS risk_group_id TEXT,
            ADD COLUMN IF NOT EXISTS category TEXT,
            ADD COLUMN IF NOT EXISTS event_id TEXT
        """
    )


def downgrade() -> None:
    connection: Connection = op.get_bind()
    connection.exec_driver_sql(
        """
        ALTER TABLE markets
            DROP COLUMN IF EXISTS event_id,
            DROP COLUMN IF EXISTS category,
            DROP COLUMN IF EXISTS risk_group_id
        """
    )
