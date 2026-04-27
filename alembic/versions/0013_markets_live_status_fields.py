"""Add live tradability status fields to markets."""

from __future__ import annotations

from alembic import op


revision = "0013_market_status_fields"
down_revision = "0012_cancelled_market_resolved"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE markets
          ADD COLUMN IF NOT EXISTS active BOOLEAN,
          ADD COLUMN IF NOT EXISTS closed BOOLEAN,
          ADD COLUMN IF NOT EXISTS accepting_orders BOOLEAN,
          ADD COLUMN IF NOT EXISTS status_updated_at TIMESTAMPTZ
        """
    )


def downgrade() -> None:
    op.execute(
        """
        ALTER TABLE markets
          DROP COLUMN IF EXISTS status_updated_at,
          DROP COLUMN IF EXISTS accepting_orders,
          DROP COLUMN IF EXISTS closed,
          DROP COLUMN IF EXISTS active
        """
    )
