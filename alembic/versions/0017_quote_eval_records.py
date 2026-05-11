"""Add quote-based evaluation records."""

from __future__ import annotations

from alembic import op
from sqlalchemy.engine import Connection


revision = "0017_quote_eval_records"
down_revision = "0016_market_relations"
branch_labels = None
depends_on = None


def upgrade() -> None:
    connection: Connection = op.get_bind()
    connection.exec_driver_sql(
        """
        CREATE TABLE IF NOT EXISTS quote_eval_records (
            fill_id TEXT NOT NULL,
            decision_id TEXT NOT NULL,
            market_id TEXT NOT NULL,
            token_id TEXT,
            strategy_id TEXT NOT NULL,
            strategy_version_id TEXT NOT NULL,
            prob_estimate DOUBLE PRECISION NOT NULL,
            quote_price DOUBLE PRECISION NOT NULL,
            quote_source TEXT NOT NULL,
            quote_lag_seconds INTEGER NOT NULL,
            quote_score DOUBLE PRECISION NOT NULL,
            mtm_pnl DOUBLE PRECISION NOT NULL,
            book_ts TIMESTAMPTZ NOT NULL,
            recorded_at TIMESTAMPTZ NOT NULL,
            citations JSONB NOT NULL DEFAULT '[]'::jsonb,
            category TEXT,
            model_id TEXT,
            PRIMARY KEY (fill_id, quote_lag_seconds),
            CONSTRAINT quote_eval_records_strategy_identity_check
                CHECK (strategy_id != '' AND strategy_version_id != '')
        )
        """
    )
    connection.exec_driver_sql(
        """
        CREATE INDEX IF NOT EXISTS idx_quote_eval_records_strategy_identity_recorded_at
            ON quote_eval_records(strategy_id, strategy_version_id, recorded_at DESC)
        """
    )


def downgrade() -> None:
    connection: Connection = op.get_bind()
    connection.exec_driver_sql(
        "DROP INDEX IF EXISTS idx_quote_eval_records_strategy_identity_recorded_at"
    )
    connection.exec_driver_sql("DROP TABLE IF EXISTS quote_eval_records")
