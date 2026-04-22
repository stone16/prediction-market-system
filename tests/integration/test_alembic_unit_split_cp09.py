from __future__ import annotations

import difflib
import os
import shutil
import subprocess
import uuid
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

import pytest


ROOT = Path(__file__).resolve().parents[2]
SCHEMA_PATH = ROOT / "schema.sql"
PMS_TEST_DATABASE_URL = os.environ.get("PMS_TEST_DATABASE_URL")

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.environ.get("PMS_RUN_INTEGRATION") != "1",
        reason="set PMS_RUN_INTEGRATION=1 to run PostgreSQL integration tests",
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


def _run_psql(database_url: str, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["psql", database_url, "--set", "ON_ERROR_STOP=1", *args],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=True,
    )


def _run_pg_dump(database_url: str) -> str:
    command = [
        "pg_dump",
        database_url,
        "--schema-only",
        "--no-owner",
        "--no-privileges",
    ]
    result = subprocess.run(
        command,
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode == 0:
        return result.stdout
    if "server version mismatch" not in result.stderr:
        result.check_returncode()

    server_version = subprocess.run(
        [
            "psql",
            database_url,
            "-At",
            "-c",
            "SHOW server_version",
        ],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=True,
    )
    major_version = server_version.stdout.strip().split(".", 1)[0]
    candidate_paths = [
        shutil.which(f"pg_dump@{major_version}"),
        shutil.which(f"pg_dump-{major_version}"),
        f"/opt/homebrew/opt/postgresql@{major_version}/bin/pg_dump",
        f"/usr/local/opt/postgresql@{major_version}/bin/pg_dump",
        f"/opt/homebrew/opt/libpq/bin/pg_dump",
    ]
    pg_dump_binary = next(
        (path for path in candidate_paths if path and Path(path).exists()),
        None,
    )
    assert pg_dump_binary is not None, result.stderr

    matched_result = subprocess.run(
        [
            pg_dump_binary,
            database_url,
            "--schema-only",
            "--no-owner",
            "--no-privileges",
        ],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=True,
    )
    return matched_result.stdout


def _run_alembic(database_url: str, *args: str) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env["DATABASE_URL"] = database_url
    env.pop("PMS_DATABASE_URL", None)
    return subprocess.run(
        ["uv", "run", "alembic", *args],
        cwd=ROOT,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )


def _normalized_pg_dump_statements(schema_dump: str) -> list[str]:
    ignored_prefixes = (
        "SET ",
        "SELECT pg_catalog.set_config",
        "CREATE SCHEMA public",
        "COMMENT ON SCHEMA public",
    )
    statements: list[str] = []
    current: list[str] = []

    for raw_line in schema_dump.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("--") or line.startswith("\\"):
            continue
        current.append(line)
        if line.endswith(";"):
            statement = " ".join(current)
            current = []
            if statement.startswith(ignored_prefixes):
                continue
            statement = " ".join(statement.split()).removesuffix(";")
            if "alembic_version" in statement:
                continue
            statements.append(statement)

    if current:
        statement = " ".join(" ".join(current).split()).removesuffix(";")
        if statement and "alembic_version" not in statement:
            statements.append(statement)

    return sorted(statements)


def _fetch_value(database_url: str, sql: str) -> str:
    return _run_psql(database_url, "-At", "-F", "|", "-c", sql).stdout.strip()


def _fetch_lines(database_url: str, sql: str) -> list[str]:
    return [line for line in _fetch_value(database_url, sql).splitlines() if line]


def test_alembic_unit_split_matches_schema_sql_via_pg_dump() -> None:
    assert PMS_TEST_DATABASE_URL is not None

    admin_database_url = _replace_database(PMS_TEST_DATABASE_URL, "postgres")
    schema_database = f"pms_cp09_schema_{uuid.uuid4().hex[:8]}"
    alembic_database = f"pms_cp09_migration_{uuid.uuid4().hex[:8]}"
    schema_database_url = _replace_database(PMS_TEST_DATABASE_URL, schema_database)
    alembic_database_url = _replace_database(PMS_TEST_DATABASE_URL, alembic_database)

    try:
        _run_psql(admin_database_url, "-c", f"CREATE DATABASE {schema_database}")
        _run_psql(admin_database_url, "-c", f"CREATE DATABASE {alembic_database}")

        _run_psql(schema_database_url, "-f", str(SCHEMA_PATH))
        upgrade = _run_alembic(alembic_database_url, "upgrade", "head")
        assert upgrade.returncode == 0, upgrade.stderr

        schema_statements = _normalized_pg_dump_statements(_run_pg_dump(schema_database_url))
        alembic_statements = _normalized_pg_dump_statements(_run_pg_dump(alembic_database_url))
        diff_lines = list(
            difflib.unified_diff(
                schema_statements,
                alembic_statements,
                fromfile="schema.sql",
                tofile="alembic-upgrade-head",
                lineterm="",
            )
        )

        assert diff_lines == []
        joined_statements = "\n".join(schema_statements)
        assert "requested_notional_usdc" in joined_statements
        assert "filled_notional_usdc" in joined_statements
        assert "remaining_notional_usdc" in joined_statements
        assert "fill_notional_usdc" in joined_statements
        assert "fill_quantity" in joined_statements
    finally:
        _run_psql(
            admin_database_url,
            "-c",
            f"DROP DATABASE IF EXISTS {schema_database} WITH (FORCE)",
        )
        _run_psql(
            admin_database_url,
            "-c",
            f"DROP DATABASE IF EXISTS {alembic_database} WITH (FORCE)",
        )


def test_alembic_unit_split_migrates_legacy_rows_and_downgrades_cleanly() -> None:
    assert PMS_TEST_DATABASE_URL is not None

    admin_database_url = _replace_database(PMS_TEST_DATABASE_URL, "postgres")
    temp_database = f"pms_cp09_legacy_{uuid.uuid4().hex[:8]}"
    temp_database_url = _replace_database(PMS_TEST_DATABASE_URL, temp_database)

    try:
        _run_psql(admin_database_url, "-c", f"CREATE DATABASE {temp_database}")
        baseline = _run_alembic(temp_database_url, "upgrade", "0001_baseline")
        assert baseline.returncode == 0, baseline.stderr

        _run_psql(
            temp_database_url,
            "-c",
            """
            ALTER TABLE orders
                ADD COLUMN requested_size DOUBLE PRECISION NOT NULL DEFAULT 0.0,
                ADD COLUMN filled_size DOUBLE PRECISION NOT NULL DEFAULT 0.0,
                ADD COLUMN remaining_size DOUBLE PRECISION NOT NULL DEFAULT 0.0;
            ALTER TABLE fills
                ADD COLUMN fill_size DOUBLE PRECISION NOT NULL DEFAULT 0.0,
                ADD COLUMN filled_contracts DOUBLE PRECISION;
            CREATE INDEX idx_orders_requested_size ON orders (requested_size);
            CREATE INDEX idx_fills_fill_size ON fills (fill_size);
            INSERT INTO orders (
                order_id,
                market_id,
                ts,
                strategy_id,
                strategy_version_id,
                requested_size,
                filled_size,
                remaining_size
            ) VALUES (
                'order-legacy',
                'market-legacy',
                now(),
                'default',
                'default-v1',
                100.0,
                25.0,
                75.0
            );
            INSERT INTO fills (
                fill_id,
                order_id,
                market_id,
                ts,
                strategy_id,
                strategy_version_id,
                fill_size,
                filled_contracts
            ) VALUES (
                'fill-legacy',
                'order-legacy',
                'market-legacy',
                now(),
                'default',
                'default-v1',
                12.5,
                50.0
            );
            """,
        )

        upgrade = _run_alembic(temp_database_url, "upgrade", "head")
        assert upgrade.returncode == 0, upgrade.stderr

        order_row = _fetch_value(
            temp_database_url,
            """
            SELECT requested_notional_usdc, filled_notional_usdc, remaining_notional_usdc, filled_quantity
            FROM orders
            WHERE order_id = 'order-legacy'
            """,
        )
        fill_row = _fetch_value(
            temp_database_url,
            """
            SELECT fill_notional_usdc, fill_quantity
            FROM fills
            WHERE fill_id = 'fill-legacy'
            """,
        )
        order_columns = _fetch_lines(
            temp_database_url,
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = 'orders'
            ORDER BY column_name
            """,
        )
        fill_columns = _fetch_lines(
            temp_database_url,
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = 'fills'
            ORDER BY column_name
            """,
        )
        indexes = _fetch_lines(
            temp_database_url,
            """
            SELECT indexname
            FROM pg_indexes
            WHERE schemaname = 'public' AND tablename IN ('orders', 'fills')
            ORDER BY indexname
            """,
        )

        assert order_row == "100|25|75|0"
        assert fill_row == "12.5|50"
        assert "requested_notional_usdc" in order_columns
        assert "filled_notional_usdc" in order_columns
        assert "remaining_notional_usdc" in order_columns
        assert "filled_quantity" in order_columns
        assert "requested_size" not in order_columns
        assert "filled_size" not in order_columns
        assert "remaining_size" not in order_columns
        assert "fill_notional_usdc" in fill_columns
        assert "fill_quantity" in fill_columns
        assert "fill_size" not in fill_columns
        assert "filled_contracts" not in fill_columns
        assert "idx_orders_requested_notional_usdc" in indexes
        assert "idx_fills_fill_notional_usdc" in indexes
        assert "idx_orders_requested_size" not in indexes
        assert "idx_fills_fill_size" not in indexes

        downgrade = _run_alembic(temp_database_url, "downgrade", "0001_baseline")
        assert downgrade.returncode == 0, downgrade.stderr

        reverted_order_row = _fetch_value(
            temp_database_url,
            """
            SELECT requested_size, filled_size, remaining_size
            FROM orders
            WHERE order_id = 'order-legacy'
            """,
        )
        reverted_fill_row = _fetch_value(
            temp_database_url,
            """
            SELECT fill_size, filled_contracts
            FROM fills
            WHERE fill_id = 'fill-legacy'
            """,
        )
        reverted_order_columns = _fetch_lines(
            temp_database_url,
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = 'orders'
            ORDER BY column_name
            """,
        )
        reverted_fill_columns = _fetch_lines(
            temp_database_url,
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = 'fills'
            ORDER BY column_name
            """,
        )

        assert reverted_order_row == "100|25|75"
        assert reverted_fill_row == "12.5|50"
        assert "requested_size" in reverted_order_columns
        assert "filled_size" in reverted_order_columns
        assert "remaining_size" in reverted_order_columns
        assert "requested_notional_usdc" not in reverted_order_columns
        assert "filled_notional_usdc" not in reverted_order_columns
        assert "remaining_notional_usdc" not in reverted_order_columns
        assert "filled_quantity" not in reverted_order_columns
        assert "fill_size" in reverted_fill_columns
        assert "filled_contracts" in reverted_fill_columns
        assert "fill_notional_usdc" not in reverted_fill_columns
        assert "fill_quantity" not in reverted_fill_columns
    finally:
        _run_psql(
            admin_database_url,
            "-c",
            f"DROP DATABASE IF EXISTS {temp_database} WITH (FORCE)",
        )
