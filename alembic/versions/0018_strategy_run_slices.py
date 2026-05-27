"""Add walk-forward strategy run slice metrics."""

from __future__ import annotations

from alembic import op
from sqlalchemy.engine import Connection


revision = "0018_strategy_run_slices"
down_revision = "0017_eval_brier_baseline"
branch_labels = None
depends_on = None


STRATEGY_RUN_SLICES_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS strategy_run_slices (
    strategy_run_slice_id UUID PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES backtest_runs(run_id) ON DELETE CASCADE,
    strategy_id TEXT NOT NULL,
    strategy_version_id TEXT NOT NULL,
    slice_label TEXT NOT NULL,
    slice_start TIMESTAMPTZ NOT NULL,
    slice_end TIMESTAMPTZ NOT NULL,
    slice_kind TEXT NOT NULL DEFAULT 'out_of_sample',
    brier DOUBLE PRECISION,
    pnl_cum DOUBLE PRECISION,
    drawdown_max DOUBLE PRECISION,
    fill_rate DOUBLE PRECISION,
    slippage_bps DOUBLE PRECISION,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT strategy_run_slices_strategy_identity_check
        CHECK (strategy_id != '' AND strategy_version_id != ''),
    CONSTRAINT strategy_run_slices_label_check
        CHECK (slice_label != ''),
    CONSTRAINT strategy_run_slices_window_check
        CHECK (slice_start < slice_end),
    CONSTRAINT strategy_run_slices_kind_check
        CHECK (slice_kind IN ('out_of_sample', 'walk_forward', 'category', 'liquidity')),
    CONSTRAINT strategy_run_slices_unique_label
        UNIQUE (run_id, strategy_id, strategy_version_id, slice_label)
)
"""

STRATEGY_RUN_SLICES_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_strategy_run_slices_run_strategy_identity
    ON strategy_run_slices(run_id, strategy_id, strategy_version_id, slice_start)
"""


def upgrade() -> None:
    connection: Connection = op.get_bind()
    connection.exec_driver_sql(STRATEGY_RUN_SLICES_TABLE_SQL)
    connection.exec_driver_sql(STRATEGY_RUN_SLICES_INDEX_SQL)


def downgrade() -> None:
    connection: Connection = op.get_bind()
    connection.exec_driver_sql("DROP TABLE IF EXISTS strategy_run_slices")
