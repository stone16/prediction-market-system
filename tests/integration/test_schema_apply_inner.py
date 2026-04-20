from __future__ import annotations

import os
import subprocess
import uuid
from pathlib import Path
import re
from urllib.parse import urlsplit, urlunsplit

import pytest


SCHEMA_PATH = Path("schema.sql")
PMS_TEST_DATABASE_URL = os.environ.get("PMS_TEST_DATABASE_URL")
STRATEGY_TABLES = [
    "eval_records",
    "feedback",
    "fills",
    "opportunities",
    "orders",
]
CHECK_CONSTRAINTS = [
    "eval_records_strategy_identity_check",
    "feedback_strategy_identity_check",
    "fills_strategy_identity_check",
    "opportunities_strategy_identity_check",
    "orders_strategy_identity_check",
]
STRATEGY_INDEXES = [
    "idx_eval_records_strategy_identity",
    "idx_feedback_strategy_identity",
    "idx_fills_strategy_identity",
    "idx_opportunities_strategy_identity",
    "idx_orders_strategy_identity",
]
INNER_RING_NOT_NULL_TABLES = [
    *STRATEGY_TABLES,
    "strategy_runs",
    "backtest_live_comparisons",
]
REMEDIATION_MESSAGE = """CP04 remediation required before enforcing strategy identity columns.
Run:
UPDATE feedback SET strategy_id = 'default', strategy_version_id = 'default-v1' WHERE strategy_id IS NULL OR strategy_version_id IS NULL;
UPDATE eval_records SET strategy_id = 'default', strategy_version_id = 'default-v1' WHERE strategy_id IS NULL OR strategy_version_id IS NULL;
UPDATE orders SET strategy_id = 'default', strategy_version_id = 'default-v1' WHERE strategy_id IS NULL OR strategy_version_id IS NULL;
UPDATE fills SET strategy_id = 'default', strategy_version_id = 'default-v1' WHERE strategy_id IS NULL OR strategy_version_id IS NULL;
Then re-run schema.sql."""

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.environ.get("PMS_RUN_INTEGRATION") != "1",
        reason="set PMS_RUN_INTEGRATION=1 to run PostgreSQL schema integration tests",
    ),
    pytest.mark.skipif(
        PMS_TEST_DATABASE_URL is None,
        reason="set PMS_TEST_DATABASE_URL to a PostgreSQL URI with CREATE DATABASE privileges",
    ),
]


def _replace_database(database_url: str, database_name: str) -> str:
    parts = urlsplit(database_url)
    return urlunsplit(
        (
            parts.scheme,
            parts.netloc,
            f"/{database_name}",
            parts.query,
            parts.fragment,
        )
    )


def _run_psql(
    database_url: str,
    *args: str,
    input_sql: str | None = None,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["psql", database_url, "--set", "ON_ERROR_STOP=1", *args],
        input=input_sql,
        text=True,
        capture_output=True,
        check=True,
    )


def _extract_block(schema_text: str, begin: str, end: str) -> str:
    match = re.search(rf"(?s){re.escape(begin)}\s*(.*?)\s*{re.escape(end)}", schema_text)
    assert match is not None, f"{begin} / {end} block not found"
    return match.group(1)


def _legacy_nullable_inner_ring_schema() -> str:
    return """
BEGIN;
CREATE TABLE IF NOT EXISTS strategies (
    strategy_id TEXT PRIMARY KEY,
    active_version_id TEXT NULL
);
CREATE TABLE IF NOT EXISTS strategy_versions (
    strategy_version_id TEXT PRIMARY KEY,
    strategy_id TEXT NOT NULL,
    config_json JSONB NOT NULL,
    UNIQUE (strategy_id, strategy_version_id)
);
CREATE TABLE IF NOT EXISTS feedback (
    feedback_id TEXT PRIMARY KEY,
    target TEXT NOT NULL,
    source TEXT NOT NULL,
    message TEXT NOT NULL,
    severity TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    resolved BOOLEAN NOT NULL DEFAULT FALSE,
    resolved_at TIMESTAMPTZ,
    category TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    strategy_id TEXT NULL,
    strategy_version_id TEXT NULL
);
CREATE TABLE IF NOT EXISTS eval_records (
    decision_id TEXT PRIMARY KEY,
    market_id TEXT NOT NULL,
    prob_estimate DOUBLE PRECISION NOT NULL,
    resolved_outcome DOUBLE PRECISION NOT NULL,
    brier_score DOUBLE PRECISION NOT NULL,
    fill_status TEXT NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL,
    citations JSONB NOT NULL DEFAULT '[]'::jsonb,
    category TEXT,
    model_id TEXT,
    pnl DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    slippage_bps DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    filled BOOLEAN NOT NULL DEFAULT TRUE,
    strategy_id TEXT NULL,
    strategy_version_id TEXT NULL
);
CREATE TABLE IF NOT EXISTS orders (
    order_id TEXT PRIMARY KEY,
    market_id TEXT NOT NULL,
    ts TIMESTAMPTZ NOT NULL,
    strategy_id TEXT NULL,
    strategy_version_id TEXT NULL
);
CREATE TABLE IF NOT EXISTS fills (
    fill_id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL,
    market_id TEXT NOT NULL,
    ts TIMESTAMPTZ NOT NULL,
    strategy_id TEXT NULL,
    strategy_version_id TEXT NULL
);
INSERT INTO strategies (strategy_id, active_version_id)
VALUES ('default', 'default-v1')
ON CONFLICT (strategy_id) DO NOTHING;
INSERT INTO strategy_versions (strategy_version_id, strategy_id, config_json)
VALUES (
    'default-v1',
    'default',
    '{"config":{"strategy_id":"default","factor_composition":[],"metadata":[]},"risk":{"max_position_notional_usdc":100.0,"max_daily_drawdown_pct":2.5,"min_order_size_usdc":1.0},"eval_spec":{"metrics":["brier"]},"forecaster":{"forecasters":[]},"market_selection":{"venue":"polymarket","resolution_time_max_horizon_days":7,"volume_min_usdc":500.0}}'::jsonb
)
ON CONFLICT (strategy_version_id) DO NOTHING;
INSERT INTO eval_records (
    decision_id,
    market_id,
    prob_estimate,
    resolved_outcome,
    brier_score,
    fill_status,
    recorded_at,
    citations,
    strategy_id,
    strategy_version_id
) VALUES (
    'legacy-null-decision',
    'market-1',
    0.6,
    1.0,
    0.16,
    'filled',
    now(),
    '["legacy"]'::jsonb,
    NULL,
    NULL
);
COMMIT;
"""


def test_schema_sql_applies_inner_ring_strategy_identity_constraints() -> None:
    assert PMS_TEST_DATABASE_URL is not None

    schema_text = SCHEMA_PATH.read_text()
    inner_ring = _extract_block(
        schema_text,
        "-- BEGIN INNER-RING PRODUCT SHELLS",
        "-- END INNER-RING PRODUCT SHELLS",
    )
    assert "CREATE TABLE IF NOT EXISTS opportunities" in inner_ring
    assert inner_ring.count("strategy_id TEXT NOT NULL") == len(INNER_RING_NOT_NULL_TABLES)
    assert inner_ring.count("strategy_version_id TEXT NOT NULL") == len(
        INNER_RING_NOT_NULL_TABLES
    )

    admin_database_url = _replace_database(PMS_TEST_DATABASE_URL, "postgres")
    temp_database = f"pms_cp04_{uuid.uuid4().hex[:12]}"
    temp_database_url = _replace_database(PMS_TEST_DATABASE_URL, temp_database)

    try:
        _run_psql(admin_database_url, "-c", f"CREATE DATABASE {temp_database}")
        _run_psql(temp_database_url, "-f", str(SCHEMA_PATH))
        _run_psql(temp_database_url, "-f", str(SCHEMA_PATH))

        columns_result = _run_psql(
            temp_database_url,
            "-At",
            "-F",
            "|",
            "-c",
            """
            SELECT table_name, column_name, is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name IN ('feedback', 'eval_records', 'orders', 'fills', 'opportunities')
              AND column_name IN ('strategy_id', 'strategy_version_id')
            ORDER BY table_name ASC, column_name ASC
            """,
        )
        actual_columns = [
            tuple(line.split("|", 2))
            for line in columns_result.stdout.splitlines()
        ]

        assert actual_columns == [
            ("eval_records", "strategy_id", "NO"),
            ("eval_records", "strategy_version_id", "NO"),
            ("feedback", "strategy_id", "NO"),
            ("feedback", "strategy_version_id", "NO"),
            ("fills", "strategy_id", "NO"),
            ("fills", "strategy_version_id", "NO"),
            ("opportunities", "strategy_id", "NO"),
            ("opportunities", "strategy_version_id", "NO"),
            ("orders", "strategy_id", "NO"),
            ("orders", "strategy_version_id", "NO"),
        ]

        constraints_result = _run_psql(
            temp_database_url,
            "-At",
            "-c",
            """
            SELECT conname
            FROM pg_constraint
            WHERE conname IN (
                'feedback_strategy_identity_check',
                'eval_records_strategy_identity_check',
                'orders_strategy_identity_check',
                'fills_strategy_identity_check',
                'opportunities_strategy_identity_check'
            )
            ORDER BY conname ASC
            """,
        )
        assert constraints_result.stdout.splitlines() == sorted(CHECK_CONSTRAINTS)

        indexes_result = _run_psql(
            temp_database_url,
            "-At",
            "-c",
            """
            SELECT indexname
            FROM pg_indexes
            WHERE schemaname = 'public'
              AND indexname IN (
                'idx_feedback_strategy_identity',
                'idx_eval_records_strategy_identity',
                'idx_orders_strategy_identity',
                'idx_fills_strategy_identity',
                'idx_opportunities_strategy_identity'
            )
            ORDER BY indexname ASC
            """,
        )
        assert indexes_result.stdout.splitlines() == sorted(STRATEGY_INDEXES)

        _run_psql(
            temp_database_url,
            "-c",
            """
            INSERT INTO eval_records (
                decision_id,
                market_id,
                prob_estimate,
                resolved_outcome,
                brier_score,
                fill_status,
                recorded_at,
                citations,
                strategy_id,
                strategy_version_id
            ) VALUES (
                'decision-index',
                'market-1',
                0.6,
                1.0,
                0.16,
                'filled',
                now(),
                '["seed"]'::jsonb,
                'default',
                'default-v1'
            )
            """,
        )
        explain_result = _run_psql(
            temp_database_url,
            "-At",
            "-c",
            """
            SET enable_seqscan = off;
            EXPLAIN SELECT *
            FROM eval_records
            WHERE strategy_id = 'default' AND strategy_version_id = 'default-v1';
            """,
        )
        assert "idx_eval_records_strategy_identity" in explain_result.stdout

        with pytest.raises(subprocess.CalledProcessError) as error:
            _run_psql(
                temp_database_url,
                "-c",
                """
                INSERT INTO eval_records (
                    decision_id,
                    market_id,
                    prob_estimate,
                    resolved_outcome,
                    brier_score,
                    fill_status,
                    recorded_at,
                    citations,
                    strategy_id,
                    strategy_version_id
                ) VALUES (
                    'decision-empty',
                    'market-1',
                    0.6,
                    1.0,
                    0.16,
                    'filled',
                    now(),
                    '["seed"]'::jsonb,
                    '',
                    ''
                )
                """,
            )
        assert "eval_records_strategy_identity_check" in error.value.stderr
    finally:
        _run_psql(
            admin_database_url,
            "-c",
            f"DROP DATABASE IF EXISTS {temp_database} WITH (FORCE)",
        )


def test_schema_sql_probe_blocks_null_strategy_rows_before_not_null_upgrade() -> None:
    assert PMS_TEST_DATABASE_URL is not None

    admin_database_url = _replace_database(PMS_TEST_DATABASE_URL, "postgres")
    temp_database = f"pms_cp04_probe_{uuid.uuid4().hex[:12]}"
    temp_database_url = _replace_database(PMS_TEST_DATABASE_URL, temp_database)

    try:
        _run_psql(admin_database_url, "-c", f"CREATE DATABASE {temp_database}")
        _run_psql(
            temp_database_url,
            input_sql=_legacy_nullable_inner_ring_schema(),
        )

        with pytest.raises(subprocess.CalledProcessError) as error:
            _run_psql(temp_database_url, "-f", str(SCHEMA_PATH))

        assert REMEDIATION_MESSAGE in error.value.stderr
    finally:
        _run_psql(
            admin_database_url,
            "-c",
            f"DROP DATABASE IF EXISTS {temp_database} WITH (FORCE)",
        )
