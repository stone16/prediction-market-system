"""Add runtime heartbeat evidence for paper soak continuity."""

from __future__ import annotations

from alembic import op
from sqlalchemy.engine import Connection


revision = "0022_runtime_heartbeats"
down_revision = "0021_market_risk_metadata"
branch_labels = None
depends_on = None


def upgrade() -> None:
    connection: Connection = op.get_bind()
    connection.exec_driver_sql(
        """
        CREATE TABLE IF NOT EXISTS runtime_heartbeats (
            heartbeat_id BIGSERIAL PRIMARY KEY,
            run_id TEXT NOT NULL,
            mode TEXT NOT NULL,
            started_at TIMESTAMPTZ NOT NULL,
            observed_at TIMESTAMPTZ NOT NULL,
            strategy_fingerprint TEXT,
            component_status_json JSONB NOT NULL DEFAULT '{}'::jsonb
        )
        """
    )
    connection.exec_driver_sql(
        """
        CREATE INDEX IF NOT EXISTS idx_runtime_heartbeats_run_observed
            ON runtime_heartbeats (run_id, observed_at)
        """
    )


def downgrade() -> None:
    connection: Connection = op.get_bind()
    connection.exec_driver_sql(
        "DROP INDEX IF EXISTS idx_runtime_heartbeats_run_observed"
    )
    connection.exec_driver_sql("DROP TABLE IF EXISTS runtime_heartbeats")
