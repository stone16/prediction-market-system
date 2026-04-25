from __future__ import annotations

import os
import subprocess
import uuid
from urllib.parse import urlsplit, urlunsplit

import pytest


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
        text=True,
        capture_output=True,
        check=True,
    )


def _run_alembic(database_url: str, *args: str) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env["DATABASE_URL"] = database_url
    env.pop("PMS_DATABASE_URL", None)
    return subprocess.run(
        ["uv", "run", "alembic", *args],
        text=True,
        capture_output=True,
        check=False,
        env=env,
    )


def test_alembic_decisions_table_upgrade_and_downgrade_round_trip() -> None:
    assert PMS_TEST_DATABASE_URL is not None

    admin_database_url = _replace_database(PMS_TEST_DATABASE_URL, "postgres")
    temp_database = f"pms_cp07_decisions_{uuid.uuid4().hex[:8]}"
    temp_database_url = _replace_database(PMS_TEST_DATABASE_URL, temp_database)

    try:
        _run_psql(admin_database_url, "-c", f"CREATE DATABASE {temp_database}")
        upgrade = _run_alembic(temp_database_url, "upgrade", "0004_decisions_table")
        assert upgrade.returncode == 0, upgrade.stderr

        columns = _run_psql(
            temp_database_url,
            "-At",
            "-F",
            "|",
            "-c",
            """
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = 'decisions'
            ORDER BY ordinal_position
            """,
        )
        assert columns.stdout.splitlines() == [
            "decision_id|text|NO",
            "opportunity_id|text|NO",
            "strategy_id|text|NO",
            "strategy_version_id|text|NO",
            "status|text|NO",
            "factor_snapshot_hash|text|YES",
            "created_at|timestamp with time zone|NO",
            "updated_at|timestamp with time zone|NO",
            "expires_at|timestamp with time zone|NO",
        ]

        indexes = _run_psql(
            temp_database_url,
            "-At",
            "-c",
            """
            SELECT indexname
            FROM pg_indexes
            WHERE schemaname = 'public' AND tablename = 'decisions'
            ORDER BY indexname
            """,
        )
        assert indexes.stdout.splitlines() == [
            "decisions_pkey",
            "idx_decisions_opportunity",
            "idx_decisions_status_created",
            "idx_decisions_strategy_version",
        ]

        downgrade = _run_alembic(temp_database_url, "downgrade", "-1")
        assert downgrade.returncode == 0, downgrade.stderr

        table_count = _run_psql(
            temp_database_url,
            "-At",
            "-c",
            """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = 'decisions'
            """,
        )
        assert table_count.stdout.strip() == "0"
    finally:
        _run_psql(
            admin_database_url,
            "-c",
            f"DROP DATABASE IF EXISTS {temp_database} WITH (FORCE)",
        )
