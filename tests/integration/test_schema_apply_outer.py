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
    "markets": [
        ("condition_id", "text"),
        ("slug", "text"),
        ("question", "text"),
        ("venue", "text"),
        ("resolves_at", "timestamp with time zone"),
        ("created_at", "timestamp with time zone"),
        ("last_seen_at", "timestamp with time zone"),
    ],
    "tokens": [
        ("token_id", "text"),
        ("condition_id", "text"),
        ("outcome", "text"),
    ],
    "book_snapshots": [
        ("id", "bigint"),
        ("market_id", "text"),
        ("token_id", "text"),
        ("ts", "timestamp with time zone"),
        ("hash", "text"),
        ("source", "text"),
    ],
    "book_levels": [
        ("snapshot_id", "bigint"),
        ("market_id", "text"),
        ("side", "text"),
        ("price", "double precision"),
        ("size", "double precision"),
    ],
    "price_changes": [
        ("id", "bigint"),
        ("market_id", "text"),
        ("token_id", "text"),
        ("ts", "timestamp with time zone"),
        ("side", "text"),
        ("price", "double precision"),
        ("size", "double precision"),
        ("best_bid", "double precision"),
        ("best_ask", "double precision"),
        ("hash", "text"),
    ],
    "trades": [
        ("id", "bigint"),
        ("market_id", "text"),
        ("token_id", "text"),
        ("ts", "timestamp with time zone"),
        ("price", "double precision"),
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


def _outer_ring_block(schema_text: str) -> str:
    match = re.search(
        r"(?s)-- BEGIN OUTER RING\s*(.*?)\s*-- END OUTER RING",
        schema_text,
    )
    assert match is not None, "schema.sql must delimit the outer ring block"
    return match.group(1)


def test_schema_sql_applies_outer_ring_tables() -> None:
    assert PMS_TEST_DATABASE_URL is not None

    schema_text = SCHEMA_PATH.read_text()
    outer_ring = _outer_ring_block(schema_text)

    assert "sensor_sessions" not in outer_ring
    assert "strategy_id" not in outer_ring
    assert "strategy_version_id" not in outer_ring

    for table_name in EXPECTED_COLUMNS:
        assert f"CREATE TABLE IF NOT EXISTS {table_name}" in outer_ring

    admin_database_url = _replace_database(PMS_TEST_DATABASE_URL, "postgres")
    temp_database = f"pms_cp01_{uuid.uuid4().hex[:12]}"
    temp_database_url = _replace_database(PMS_TEST_DATABASE_URL, temp_database)

    try:
        _run_psql(admin_database_url, "-c", f"CREATE DATABASE {temp_database}")
        _run_psql(temp_database_url, "-f", str(SCHEMA_PATH))
        _run_psql(temp_database_url, "-f", str(SCHEMA_PATH))

        columns_query = """
        SELECT table_name, column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name IN (
            'markets',
            'tokens',
            'book_snapshots',
            'book_levels',
            'price_changes',
            'trades'
          )
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

        constraints_query = """
        SELECT c.relname, pg_get_constraintdef(ct.oid)
        FROM pg_constraint AS ct
        JOIN pg_class AS c ON c.oid = ct.conrelid
        WHERE c.relname IN (
            'markets',
            'tokens',
            'book_snapshots',
            'book_levels',
            'price_changes'
        )
          AND ct.contype IN ('c', 'u')
        ORDER BY c.relname, pg_get_constraintdef(ct.oid)
        """
        constraints_result = _run_psql(
            temp_database_url,
            "-At",
            "-F",
            "|",
            "-c",
            constraints_query,
        )
        definitions_by_table: dict[str, list[str]] = {}
        for line in constraints_result.stdout.splitlines():
            table_name, definition = line.split("|", 1)
            definitions_by_table.setdefault(table_name, []).append(definition.lower())

        assert any(
            "venue" in definition
            and "polymarket" in definition
            and "kalshi" in definition
            for definition in definitions_by_table["markets"]
        )
        assert any(
            "outcome" in definition
            and "yes" in definition
            and "no" in definition
            for definition in definitions_by_table["tokens"]
        )
        assert any(
            "source" in definition
            and "subscribe" in definition
            and "reconnect" in definition
            and "checkpoint" in definition
            for definition in definitions_by_table["book_snapshots"]
        )
        assert any(
            "side" in definition and "buy" in definition and "sell" in definition
            for definition in definitions_by_table["book_levels"]
        )
        assert any(
            "side" in definition and "buy" in definition and "sell" in definition
            for definition in definitions_by_table["price_changes"]
        )
        assert not any(
            "unique (market_id, ts, price, side)" in definition
            for definition in definitions_by_table["price_changes"]
        )
    finally:
        _run_psql(
            admin_database_url,
            "-c",
            f"DROP DATABASE IF EXISTS {temp_database} WITH (FORCE)",
        )
