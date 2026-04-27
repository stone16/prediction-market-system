"""Allow market-resolved cancellation as an order intent outcome."""

from __future__ import annotations

from typing import Final

from alembic import op


revision = "0012_cancelled_market_resolved"
down_revision = "0011_live_reconcile_audit"
branch_labels = None
depends_on = None


ORDER_INTENT_OUTCOMES: Final[tuple[str, ...]] = (
    "matched",
    "invalid",
    "rejected",
    "venue_rejection",
    "submission_unknown",
    "cancelled_ttl",
    "cancelled_limit_invalidated",
    "cancelled_session_end",
    "cancelled_market_resolved",
)

_DOWNGRADE_OUTCOMES: Final[tuple[str, ...]] = (
    "matched",
    "invalid",
    "rejected",
    "venue_rejection",
    "submission_unknown",
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
    op.execute(_check_constraint_sql(ORDER_INTENT_OUTCOMES))


def downgrade() -> None:
    op.execute(
        "ALTER TABLE order_intents DROP CONSTRAINT IF EXISTS order_intents_outcome_check"
    )
    op.execute(
        "UPDATE order_intents "
        "SET outcome = NULL, released_at = NULL "
        "WHERE outcome = 'cancelled_market_resolved'"
    )
    op.execute(_check_constraint_sql(_DOWNGRADE_OUTCOMES))
