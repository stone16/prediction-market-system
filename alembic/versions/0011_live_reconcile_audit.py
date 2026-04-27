"""Add live reconciliation and execution audit fields."""

from __future__ import annotations

from typing import Final

from alembic import op


revision = "0011_live_reconcile_audit"
down_revision = "0010_order_intent_key"
branch_labels = None
depends_on = None


DECISION_STATUSES: Final[tuple[str, ...]] = (
    "pending",
    "accepted",
    "queued",
    "submitted",
    "partially_filled",
    "filled",
    "rejected",
    "venue_rejected",
    "cancelled",
    "expired",
    "submission_unknown",
    "reconciled",
)


def _decision_status_check(statuses: tuple[str, ...]) -> str:
    quoted = ", ".join(f"'{status}'" for status in statuses)
    return (
        "ALTER TABLE decisions "
        "ADD CONSTRAINT decisions_status_check "
        f"CHECK (status IN ({quoted}))"
    )


def upgrade() -> None:
    op.execute("ALTER TABLE order_intents ADD COLUMN IF NOT EXISTS reconciled_at TIMESTAMPTZ")
    op.execute("ALTER TABLE order_intents ADD COLUMN IF NOT EXISTS reconciliation_note TEXT")
    op.execute("ALTER TABLE order_intents ADD COLUMN IF NOT EXISTS reconciled_by TEXT")
    op.execute("ALTER TABLE order_intents ADD COLUMN IF NOT EXISTS venue_order_id TEXT")
    op.execute("ALTER TABLE order_intents ADD COLUMN IF NOT EXISTS reconciliation_status TEXT")
    op.execute(
        """
        ALTER TABLE order_intents
        DROP CONSTRAINT IF EXISTS order_intents_reconciliation_status_check
        """
    )
    op.execute(
        """
        ALTER TABLE order_intents
        ADD CONSTRAINT order_intents_reconciliation_status_check
        CHECK (
            reconciliation_status IS NULL
            OR reconciliation_status IN ('filled', 'not_found', 'open')
        )
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_order_intents_submission_unknown_unresolved
            ON order_intents (acquired_at DESC)
            WHERE outcome = 'submission_unknown' AND reconciled_at IS NULL
        """
    )

    op.execute("ALTER TABLE decisions DROP CONSTRAINT IF EXISTS decisions_status_check")
    op.execute(_decision_status_check(DECISION_STATUSES))

    op.execute("ALTER TABLE orders ADD COLUMN IF NOT EXISTS status TEXT")
    op.execute("ALTER TABLE orders ADD COLUMN IF NOT EXISTS raw_status TEXT")
    op.execute("ALTER TABLE orders ADD COLUMN IF NOT EXISTS token_id TEXT")
    op.execute("ALTER TABLE orders ADD COLUMN IF NOT EXISTS venue TEXT")
    op.execute("ALTER TABLE orders ADD COLUMN IF NOT EXISTS action TEXT")
    op.execute("ALTER TABLE orders ADD COLUMN IF NOT EXISTS outcome TEXT")
    op.execute("ALTER TABLE orders ADD COLUMN IF NOT EXISTS time_in_force TEXT")
    op.execute(
        "ALTER TABLE orders ADD COLUMN IF NOT EXISTS pre_submit_quote_json JSONB NOT NULL DEFAULT '{}'::jsonb"
    )
    op.execute("ALTER TABLE orders ADD COLUMN IF NOT EXISTS intent_key TEXT")


def downgrade() -> None:
    op.execute("ALTER TABLE orders DROP COLUMN IF EXISTS intent_key")
    op.execute("ALTER TABLE orders DROP COLUMN IF EXISTS pre_submit_quote_json")
    op.execute("ALTER TABLE orders DROP COLUMN IF EXISTS time_in_force")
    op.execute("ALTER TABLE orders DROP COLUMN IF EXISTS outcome")
    op.execute("ALTER TABLE orders DROP COLUMN IF EXISTS action")
    op.execute("ALTER TABLE orders DROP COLUMN IF EXISTS venue")
    op.execute("ALTER TABLE orders DROP COLUMN IF EXISTS token_id")
    op.execute("ALTER TABLE orders DROP COLUMN IF EXISTS raw_status")
    op.execute("ALTER TABLE orders DROP COLUMN IF EXISTS status")

    op.execute("ALTER TABLE decisions DROP CONSTRAINT IF EXISTS decisions_status_check")
    op.execute(
        "ALTER TABLE decisions "
        "ADD CONSTRAINT decisions_status_check "
        "CHECK (status IN ('pending', 'accepted', 'rejected', 'expired'))"
    )

    op.execute("DROP INDEX IF EXISTS idx_order_intents_submission_unknown_unresolved")
    op.execute(
        "ALTER TABLE order_intents DROP CONSTRAINT IF EXISTS "
        "order_intents_reconciliation_status_check"
    )
    op.execute("ALTER TABLE order_intents DROP COLUMN IF EXISTS reconciliation_status")
    op.execute("ALTER TABLE order_intents DROP COLUMN IF EXISTS venue_order_id")
    op.execute("ALTER TABLE order_intents DROP COLUMN IF EXISTS reconciled_by")
    op.execute("ALTER TABLE order_intents DROP COLUMN IF EXISTS reconciliation_note")
    op.execute("ALTER TABLE order_intents DROP COLUMN IF EXISTS reconciled_at")
