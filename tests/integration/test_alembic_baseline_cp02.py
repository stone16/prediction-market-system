from __future__ import annotations

import difflib
import os
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
    result = subprocess.run(
        [
            "pg_dump",
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
    return result.stdout


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


def test_alembic_baseline_matches_schema_sql_via_pg_dump() -> None:
    assert PMS_TEST_DATABASE_URL is not None

    admin_database_url = _replace_database(PMS_TEST_DATABASE_URL, "postgres")
    schema_database = f"pms_alembic_schema_{uuid.uuid4().hex[:8]}"
    alembic_database = f"pms_alembic_migration_{uuid.uuid4().hex[:8]}"
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


def test_alembic_baseline_roundtrip_is_idempotent() -> None:
    assert PMS_TEST_DATABASE_URL is not None

    admin_database_url = _replace_database(PMS_TEST_DATABASE_URL, "postgres")
    temp_database = f"pms_alembic_roundtrip_{uuid.uuid4().hex[:8]}"
    temp_database_url = _replace_database(PMS_TEST_DATABASE_URL, temp_database)

    try:
        _run_psql(admin_database_url, "-c", f"CREATE DATABASE {temp_database}")

        first_upgrade = _run_alembic(temp_database_url, "upgrade", "head")
        second_upgrade = _run_alembic(temp_database_url, "upgrade", "head")
        downgrade = _run_alembic(temp_database_url, "downgrade", "base")

        assert first_upgrade.returncode == 0, first_upgrade.stderr
        assert second_upgrade.returncode == 0, second_upgrade.stderr
        assert downgrade.returncode == 0, downgrade.stderr

        tables = _run_psql(
            temp_database_url,
            "-At",
            "-c",
            """
            SELECT tablename
            FROM pg_tables
            WHERE schemaname = 'public'
            ORDER BY tablename
            """,
        )

        remaining_tables = [line for line in tables.stdout.splitlines() if line]
        assert remaining_tables in ([], ["alembic_version"])
    finally:
        _run_psql(
            admin_database_url,
            "-c",
            f"DROP DATABASE IF EXISTS {temp_database} WITH (FORCE)",
        )
