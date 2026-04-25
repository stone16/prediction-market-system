"""Add submission_unknown to order_intents outcome enum.

`submission_unknown` surfaces from the Polymarket adapter when the SDK
call times out — the order may have reached the venue, and we must
keep the dedup intent in a distinct, manually-reconcilable state.
Without this migration, releasing an intent with that outcome would
hit the existing `order_intents_outcome_check` constraint.

This migration drops and recreates the check constraint with the
additional allowed value. Existing rows are unaffected (the constraint
only validates new writes).
"""

from __future__ import annotations

from typing import Final

from alembic import op


revision = "0009_submission_unknown_outcome"
down_revision = "0008_market_subscriptions"
branch_labels = None
depends_on = None


_OUTCOMES_WITH_SUBMISSION_UNKNOWN: Final[tuple[str, ...]] = (
    "matched",
    "invalid",
    "rejected",
    "venue_rejection",
    "submission_unknown",
    "cancelled_ttl",
    "cancelled_limit_invalidated",
    "cancelled_session_end",
)

_OUTCOMES_WITHOUT_SUBMISSION_UNKNOWN: Final[tuple[str, ...]] = (
    "matched",
    "invalid",
    "rejected",
    "venue_rejection",
    "cancelled_ttl",
    "cancelled_limit_invalidated",
    "cancelled_session_end",
)


def _check_constraint_sql(outcomes: tuple[str, ...]) -> str:
    quoted = ", ".join(f"'{outcome}'" for outcome in outcomes)
    return (
        "ALTER TABLE order_intents "
        "ADD CONSTRAINT order_intents_outcome_check "
        f"CHECK (outcome IS NULL OR outcome IN ({quoted}))"
    )


def upgrade() -> None:
    op.execute(
        "ALTER TABLE order_intents DROP CONSTRAINT IF EXISTS order_intents_outcome_check"
    )
    op.execute(_check_constraint_sql(_OUTCOMES_WITH_SUBMISSION_UNKNOWN))


def downgrade() -> None:
    op.execute(
        "ALTER TABLE order_intents DROP CONSTRAINT IF EXISTS order_intents_outcome_check"
    )
    # Reconcile any rows that adopted the new outcome before re-adding
    # the stricter constraint. Setting outcome=NULL preserves the audit
    # trail (acquired_at, decision_id, worker info) but takes the row
    # back to an "intent unreleased" state, which is the correct
    # semantic — a `submission_unknown` outcome means the operator never
    # confirmed the venue state, so leaving it released under a
    # different outcome would falsely imply finality.
    op.execute(
        "UPDATE order_intents "
        "SET outcome = NULL, released_at = NULL "
        "WHERE outcome = 'submission_unknown'"
    )
    op.execute(_check_constraint_sql(_OUTCOMES_WITHOUT_SUBMISSION_UNKNOWN))
