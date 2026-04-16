from __future__ import annotations

import os
import re
import subprocess
import uuid
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

import pytest


SCHEMA_PATH = Path("schema.sql")
PMS_TEST_DATABASE_URL = os.environ.get("PMS_TEST_DATABASE_URL")

EXPECTED_COLUMNS: dict[str, list[tuple[str, str]]] = {
    "feedback": [
        ("feedback_id", "text"),
        ("target", "text"),
        ("source", "text"),
        ("message", "text"),
        ("severity", "text"),
        ("created_at", "timestamp with time zone"),
        ("resolved", "boolean"),
        ("resolved_at", "timestamp with time zone"),
        ("category", "text"),
        ("metadata", "jsonb"),
        ("strategy_id", "text"),
        ("strategy_version_id", "text"),
    ],
    "eval_records": [
        ("decision_id", "text"),
        ("market_id", "text"),
        ("prob_estimate", "double precision"),
        ("resolved_outcome", "double precision"),
        ("brier_score", "double precision"),
        ("fill_status", "text"),
        ("recorded_at", "timestamp with time zone"),
        ("citations", "jsonb"),
        ("category", "text"),
        ("model_id", "text"),
        ("pnl", "double precision"),
        ("slippage_bps", "double precision"),
        ("filled", "boolean"),
        ("strategy_id", "text"),
        ("strategy_version_id", "text"),
    ],
    "orders": [
        ("order_id", "text"),
        ("market_id", "text"),
        ("ts", "timestamp with time zone"),
        ("strategy_id", "text"),
        ("strategy_version_id", "text"),
    ],
    "fills": [
        ("fill_id", "text"),
        ("order_id", "text"),
        ("market_id", "text"),
        ("ts", "timestamp with time zone"),
        ("strategy_id", "text"),
        ("strategy_version_id", "text"),
    ],
}

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


def test_schema_sql_applies_inner_ring_tables() -> None:
    assert PMS_TEST_DATABASE_URL is not None

    schema_text = SCHEMA_PATH.read_text()
    outer_ring = _extract_block(
        schema_text,
        "-- BEGIN OUTER RING",
        "-- END OUTER RING",
    )
    inner_ring = _extract_block(
        schema_text,
        "-- BEGIN INNER-RING PRODUCT SHELLS",
        "-- END INNER-RING PRODUCT SHELLS",
    )

    assert "strategy_id" not in outer_ring
    assert "strategy_version_id" not in outer_ring

    for table_name in EXPECTED_COLUMNS:
        assert f"CREATE TABLE IF NOT EXISTS {table_name}" in inner_ring

    assert inner_ring.count("strategy_id TEXT NULL") == 4
    assert inner_ring.count("strategy_version_id TEXT NULL") == 4

    admin_database_url = _replace_database(PMS_TEST_DATABASE_URL, "postgres")
    temp_database = f"pms_cp02_{uuid.uuid4().hex[:12]}"
    temp_database_url = _replace_database(PMS_TEST_DATABASE_URL, temp_database)

    try:
        _run_psql(admin_database_url, "-c", f"CREATE DATABASE {temp_database}")
        _run_psql(temp_database_url, "-f", str(SCHEMA_PATH))

        columns_query = """
        SELECT table_name, column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name IN ('feedback', 'eval_records', 'orders', 'fills')
        ORDER BY table_name, ordinal_position
        """
        columns_result = _run_psql(
            temp_database_url,
            "-At",
            "-F",
            "|",
            "-c",
            columns_query,
        )

        actual_columns: dict[str, list[tuple[str, str]]] = {}
        for line in columns_result.stdout.splitlines():
            table_name, column_name, data_type = line.split("|")
            actual_columns.setdefault(table_name, []).append((column_name, data_type))

        assert actual_columns == EXPECTED_COLUMNS

        insert_sql = """
        INSERT INTO feedback (
            feedback_id,
            target,
            source,
            message,
            severity,
            created_at,
            strategy_id,
            strategy_version_id
        ) VALUES (
            'feedback-1',
            'controller',
            'evaluator',
            'needs review',
            'warning',
            now(),
            NULL,
            NULL
        );

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
            'decision-1',
            'market-1',
            0.6,
            1.0,
            0.16,
            'filled',
            now(),
            '["seed"]'::jsonb,
            NULL,
            NULL
        );

        INSERT INTO orders (
            order_id,
            market_id,
            ts,
            strategy_id,
            strategy_version_id
        ) VALUES (
            'order-1',
            'market-1',
            now(),
            NULL,
            NULL
        );

        INSERT INTO fills (
            fill_id,
            order_id,
            market_id,
            ts,
            strategy_id,
            strategy_version_id
        ) VALUES (
            'fill-1',
            'order-1',
            'market-1',
            now(),
            NULL,
            NULL
        );
        """
        _run_psql(temp_database_url, "-c", insert_sql)

        counts_result = _run_psql(
            temp_database_url,
            "-At",
            "-F",
            "|",
            "-c",
            """
            SELECT 'feedback', COUNT(*) FROM feedback
            UNION ALL
            SELECT 'eval_records', COUNT(*) FROM eval_records
            UNION ALL
            SELECT 'orders', COUNT(*) FROM orders
            UNION ALL
            SELECT 'fills', COUNT(*) FROM fills
            ORDER BY 1
            """,
        )
        actual_counts = dict(
            (table_name, int(count))
            for table_name, count in (
                line.split("|", 1) for line in counts_result.stdout.splitlines()
            )
        )

        assert actual_counts == {
            "eval_records": 1,
            "feedback": 1,
            "fills": 1,
            "orders": 1,
        }
    finally:
        _run_psql(
            admin_database_url,
            "-c",
            f"DROP DATABASE IF EXISTS {temp_database} WITH (FORCE)",
        )
