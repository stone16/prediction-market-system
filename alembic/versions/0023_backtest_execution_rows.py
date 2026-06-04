"""Persist row-level backtest execution evidence."""

from __future__ import annotations

from alembic import op
from sqlalchemy.engine import Connection


revision = "0023_backtest_execution_rows"
down_revision = "0022_runtime_heartbeats"
branch_labels = None
depends_on = None


def upgrade() -> None:
    connection: Connection = op.get_bind()
    connection.exec_driver_sql(
        """
        CREATE TABLE IF NOT EXISTS backtest_execution_rows (
            backtest_execution_row_id UUID PRIMARY KEY,
            run_id UUID NOT NULL REFERENCES backtest_runs(run_id) ON DELETE CASCADE,
            strategy_id TEXT NOT NULL,
            strategy_version_id TEXT NOT NULL,
            decision_id TEXT NOT NULL,
            market_id TEXT NOT NULL,
            status TEXT NOT NULL,
            slippage_bps DOUBLE PRECISION,
            pnl DOUBLE PRECISION,
            rejection_reason TEXT,
            created_at TIMESTAMPTZ NOT NULL,
            CONSTRAINT backtest_execution_rows_strategy_identity_check
                CHECK (strategy_id != '' AND strategy_version_id != ''),
            CONSTRAINT backtest_execution_rows_decision_identity_check
                CHECK (decision_id != '' AND market_id != ''),
            CONSTRAINT backtest_execution_rows_status_check
                CHECK (status IN ('filled', 'rejected')),
            CONSTRAINT backtest_execution_rows_status_payload_check
                CHECK (
                    (
                        status = 'filled'
                        AND slippage_bps IS NOT NULL
                        AND rejection_reason IS NULL
                    )
                    OR (
                        status = 'rejected'
                        AND slippage_bps IS NULL
                        AND rejection_reason IS NOT NULL
                        AND rejection_reason != ''
                    )
                ),
            CONSTRAINT backtest_execution_rows_unique_decision
                UNIQUE (run_id, strategy_id, strategy_version_id, decision_id)
        )
        """
    )
    connection.exec_driver_sql(
        """
        CREATE INDEX IF NOT EXISTS idx_backtest_execution_rows_run_strategy
            ON backtest_execution_rows (
                run_id,
                strategy_id,
                strategy_version_id,
                created_at,
                decision_id
            )
        """
    )


def downgrade() -> None:
    connection: Connection = op.get_bind()
    connection.exec_driver_sql("DROP TABLE IF EXISTS backtest_execution_rows")
