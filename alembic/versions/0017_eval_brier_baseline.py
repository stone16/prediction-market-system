"""Add eval Brier baseline columns."""

from __future__ import annotations

from alembic import op
from sqlalchemy.engine import Connection


revision = "0017_eval_brier_baseline"
down_revision = "0017_quote_eval_records"
branch_labels = None
depends_on = None


def upgrade() -> None:
    connection: Connection = op.get_bind()
    connection.exec_driver_sql(
        """
        ALTER TABLE eval_records
            ADD COLUMN IF NOT EXISTS baseline_prob_estimate DOUBLE PRECISION,
            ADD COLUMN IF NOT EXISTS baseline_brier_score DOUBLE PRECISION
        """
    )


def downgrade() -> None:
    connection: Connection = op.get_bind()
    connection.exec_driver_sql(
        """
        ALTER TABLE eval_records
            DROP COLUMN IF EXISTS baseline_brier_score,
            DROP COLUMN IF EXISTS baseline_prob_estimate
        """
    )
