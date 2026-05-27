"""Add sample counts to strategy run slices."""

from __future__ import annotations

from alembic import op
from sqlalchemy.engine import Connection


revision = "0019_strategy_run_slice_counts"
down_revision = "0018_strategy_run_slices"
branch_labels = None
depends_on = None


def upgrade() -> None:
    connection: Connection = op.get_bind()
    connection.exec_driver_sql(
        """
        ALTER TABLE strategy_run_slices
            ADD COLUMN IF NOT EXISTS opportunity_count INTEGER NOT NULL DEFAULT 0,
            ADD COLUMN IF NOT EXISTS decision_count INTEGER NOT NULL DEFAULT 0,
            ADD COLUMN IF NOT EXISTS fill_count INTEGER NOT NULL DEFAULT 0
        """
    )
    connection.exec_driver_sql(
        """
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1
                FROM pg_constraint
                WHERE conname = 'strategy_run_slices_counts_check'
            ) THEN
                ALTER TABLE strategy_run_slices
                    ADD CONSTRAINT strategy_run_slices_counts_check
                    CHECK (
                        opportunity_count >= 0
                        AND decision_count >= 0
                        AND fill_count >= 0
                    );
            END IF;
        END
        $$;
        """
    )


def downgrade() -> None:
    connection: Connection = op.get_bind()
    connection.exec_driver_sql(
        """
        ALTER TABLE strategy_run_slices
            DROP CONSTRAINT IF EXISTS strategy_run_slices_counts_check,
            DROP COLUMN IF EXISTS fill_count,
            DROP COLUMN IF EXISTS decision_count,
            DROP COLUMN IF EXISTS opportunity_count
        """
    )
