"""Add strategy meta-evidence capture and analytics surfaces."""

from __future__ import annotations

from alembic import op
from sqlalchemy.engine import Connection


revision = "0015_strategy_meta_evidence"
down_revision = "0014_strategy_artifacts"
branch_labels = None
depends_on = None


def upgrade() -> None:
    connection: Connection = op.get_bind()
    connection.exec_driver_sql(
        """
        ALTER TABLE strategy_versions
            ADD COLUMN IF NOT EXISTS metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb
        """
    )
    connection.exec_driver_sql(
        """
        ALTER TABLE eval_records
            ADD COLUMN IF NOT EXISTS edge_at_decision DOUBLE PRECISION NOT NULL DEFAULT 0.0,
            ADD COLUMN IF NOT EXISTS spread_bps_at_decision INTEGER
        """
    )
    connection.exec_driver_sql(
        """
        CREATE INDEX IF NOT EXISTS idx_eval_records_strategy_identity_recorded_at
            ON eval_records(strategy_id, strategy_version_id, recorded_at DESC)
        """
    )
    connection.exec_driver_sql(
        """
        CREATE TABLE IF NOT EXISTS strategy_performance_peaks (
            strategy_id TEXT NOT NULL,
            strategy_version_id TEXT NOT NULL,
            peak_sharpe_7d DOUBLE PRECISION NOT NULL,
            peak_sharpe_30d DOUBLE PRECISION NOT NULL,
            peak_hit_rate DOUBLE PRECISION NOT NULL,
            recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (strategy_id, strategy_version_id)
        )
        """
    )
    connection.exec_driver_sql(
        """
        CREATE TABLE IF NOT EXISTS alpha_competition_snapshots (
            snapshot_id TEXT PRIMARY KEY,
            strategy_id TEXT NOT NULL,
            strategy_version_id TEXT NOT NULL,
            snapshot_date DATE NOT NULL,
            mean_edge_30d DOUBLE PRECISION,
            mean_spread_bps_30d DOUBLE PRECISION,
            edge_trend_slope_90d DOUBLE PRECISION,
            spread_trend_slope_90d DOUBLE PRECISION,
            sample_count_30d INTEGER NOT NULL,
            trend_status TEXT NOT NULL CHECK (trend_status IN ('warming_up', 'active')),
            days_collected INTEGER NOT NULL,
            short_term_slope_30d DOUBLE PRECISION,
            short_term_slope_60d DOUBLE PRECISION,
            interpretation TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            UNIQUE (strategy_id, strategy_version_id, snapshot_date)
        )
        """
    )


def downgrade() -> None:
    connection: Connection = op.get_bind()
    connection.exec_driver_sql("DROP TABLE IF EXISTS alpha_competition_snapshots")
    connection.exec_driver_sql("DROP TABLE IF EXISTS strategy_performance_peaks")
    connection.exec_driver_sql(
        "DROP INDEX IF EXISTS idx_eval_records_strategy_identity_recorded_at"
    )
    connection.exec_driver_sql(
        """
        ALTER TABLE eval_records
            DROP COLUMN IF EXISTS spread_bps_at_decision,
            DROP COLUMN IF EXISTS edge_at_decision
        """
    )
    connection.exec_driver_sql(
        """
        ALTER TABLE strategy_versions
            DROP COLUMN IF EXISTS metadata_json
        """
    )
