"""Add deterministic economic intent keys to order_intents."""

from __future__ import annotations

from alembic import op


revision = "0010_order_intent_key"
down_revision = "0009_submission_unknown_outcome"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE order_intents ADD COLUMN IF NOT EXISTS intent_key TEXT")
    op.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_order_intents_intent_key_unique
            ON order_intents (intent_key)
            WHERE intent_key IS NOT NULL
        """
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_order_intents_intent_key_unique")
    op.execute("ALTER TABLE order_intents DROP COLUMN IF EXISTS intent_key")
