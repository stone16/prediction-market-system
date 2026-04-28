"""Add inner-ring strategy judgement and execution artifacts."""

from __future__ import annotations

from typing import Final

from alembic import op
from sqlalchemy.engine import Connection


revision = "0014_strategy_artifacts"
down_revision = "0013_market_status_fields"
branch_labels = None
depends_on = None

MAX_REASONING_SUMMARY_CHARS: Final = 4000

JUDGEMENT_ARTIFACT_TYPES: Final[tuple[str, ...]] = (
    "approved_intent",
    "rejected_candidate",
)
EXECUTION_ARTIFACT_TYPES: Final[tuple[str, ...]] = (
    "accepted_execution_plan",
    "rejected_execution_plan",
)

STRATEGY_JUDGEMENT_ARTIFACTS_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS strategy_judgement_artifacts (
    artifact_id TEXT PRIMARY KEY,
    strategy_id TEXT NOT NULL,
    strategy_version_id TEXT NOT NULL,
    artifact_type TEXT NOT NULL,
    observation_refs JSONB NOT NULL DEFAULT '[]'::jsonb,
    candidate_id TEXT NOT NULL,
    judgement_id TEXT,
    judgement_summary TEXT NOT NULL CHECK (char_length(judgement_summary) <= {MAX_REASONING_SUMMARY_CHARS}),
    evidence_refs JSONB NOT NULL DEFAULT '[]'::jsonb,
    assumptions JSONB NOT NULL DEFAULT '[]'::jsonb,
    rejection_reasons JSONB NOT NULL DEFAULT '[]'::jsonb,
    intent_payload JSONB NOT NULL DEFAULT '{{}}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT strategy_judgement_artifacts_strategy_identity_check
        CHECK (strategy_id <> '' AND strategy_version_id <> ''),
    CONSTRAINT strategy_judgement_artifacts_type_check
        CHECK (artifact_type IN ({", ".join(f"'{artifact_type}'" for artifact_type in JUDGEMENT_ARTIFACT_TYPES)}))
)
"""

STRATEGY_EXECUTION_ARTIFACTS_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS strategy_execution_artifacts (
    artifact_id TEXT PRIMARY KEY,
    strategy_id TEXT NOT NULL,
    strategy_version_id TEXT NOT NULL,
    artifact_type TEXT NOT NULL,
    intent_id TEXT NOT NULL,
    plan_id TEXT NOT NULL,
    execution_policy TEXT,
    execution_plan_payload JSONB NOT NULL DEFAULT '{{}}'::jsonb,
    risk_decision_payload JSONB NOT NULL DEFAULT '{{}}'::jsonb,
    venue_response_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
    reconciliation_status TEXT,
    post_trade_status TEXT,
    evidence_refs JSONB NOT NULL DEFAULT '[]'::jsonb,
    rejection_reasons JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT strategy_execution_artifacts_strategy_identity_check
        CHECK (strategy_id <> '' AND strategy_version_id <> ''),
    CONSTRAINT strategy_execution_artifacts_type_check
        CHECK (artifact_type IN ({", ".join(f"'{artifact_type}'" for artifact_type in EXECUTION_ARTIFACT_TYPES)}))
)
"""

STRATEGY_ARTIFACT_INDEXES_SQL: Final[tuple[str, ...]] = (
    """
    CREATE INDEX IF NOT EXISTS idx_strategy_judgement_artifacts_strategy_created_at
        ON strategy_judgement_artifacts (strategy_id, strategy_version_id, created_at DESC)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_strategy_judgement_artifacts_candidate
        ON strategy_judgement_artifacts (candidate_id)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_strategy_execution_artifacts_strategy_created_at
        ON strategy_execution_artifacts (strategy_id, strategy_version_id, created_at DESC)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_strategy_execution_artifacts_intent
        ON strategy_execution_artifacts (intent_id)
    """,
)


def upgrade() -> None:
    connection: Connection = op.get_bind()
    connection.exec_driver_sql(STRATEGY_JUDGEMENT_ARTIFACTS_TABLE_SQL)
    connection.exec_driver_sql(STRATEGY_EXECUTION_ARTIFACTS_TABLE_SQL)
    for statement in STRATEGY_ARTIFACT_INDEXES_SQL:
        connection.exec_driver_sql(statement)


def downgrade() -> None:
    connection: Connection = op.get_bind()
    connection.exec_driver_sql("DROP TABLE IF EXISTS strategy_execution_artifacts")
    connection.exec_driver_sql("DROP TABLE IF EXISTS strategy_judgement_artifacts")
