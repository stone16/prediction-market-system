"""Add secondary eval baseline maps."""

from __future__ import annotations

from alembic import op
from sqlalchemy.engine import Connection


revision = "0020_eval_secondary_baselines"
down_revision = "0019_strategy_run_slice_counts"
branch_labels = None
depends_on = None


def upgrade() -> None:
    connection: Connection = op.get_bind()
    connection.exec_driver_sql(
        """
        ALTER TABLE eval_records
            ADD COLUMN IF NOT EXISTS baseline_prob_estimates JSONB NOT NULL DEFAULT '{}'::jsonb,
            ADD COLUMN IF NOT EXISTS baseline_brier_scores JSONB NOT NULL DEFAULT '{}'::jsonb
        """
    )


def downgrade() -> None:
    connection: Connection = op.get_bind()
    connection.exec_driver_sql(
        """
        ALTER TABLE eval_records
            DROP COLUMN IF EXISTS baseline_brier_scores,
            DROP COLUMN IF EXISTS baseline_prob_estimates
        """
    )
