from __future__ import annotations

import os
import subprocess
import uuid
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

import pytest


ROOT = Path(__file__).resolve().parents[2]
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


def _run_alembic(
    *args: str,
    env_overrides: dict[str, str | None] | None = None,
) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    if env_overrides:
        for key, value in env_overrides.items():
            if value is None:
                env.pop(key, None)
            else:
                env[key] = value

    return subprocess.run(
        ["uv", "run", "alembic", *args],
        cwd=ROOT,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )


def test_alembic_scaffold_commands_are_noops_without_migrations() -> None:
    assert PMS_TEST_DATABASE_URL is not None

    admin_database_url = _replace_database(PMS_TEST_DATABASE_URL, "postgres")
    temp_database = f"pms_alembic_cp01_{uuid.uuid4().hex[:12]}"
    temp_database_url = _replace_database(PMS_TEST_DATABASE_URL, temp_database)

    try:
        _run_psql(admin_database_url, "-c", f"CREATE DATABASE {temp_database}")

        command_env = {
            "DATABASE_URL": temp_database_url,
            "PMS_DATABASE_URL": None,
        }

        heads = _run_alembic("heads", env_overrides=command_env)
        current = _run_alembic("current", env_overrides=command_env)
        upgrade = _run_alembic("upgrade", "head", env_overrides=command_env)
        downgrade = _run_alembic("downgrade", "base", env_overrides=command_env)

        assert heads.returncode == 0, heads.stderr
        assert current.returncode == 0, current.stderr
        assert upgrade.returncode == 0, upgrade.stderr
        assert downgrade.returncode == 0, downgrade.stderr
        assert heads.stdout.strip().endswith("(head)")
        assert current.stdout.strip() == ""
    finally:
        _run_psql(
            admin_database_url,
            "-c",
            f"DROP DATABASE IF EXISTS {temp_database} WITH (FORCE)",
        )


def test_alembic_current_reports_missing_database_url_cleanly() -> None:
    result = _run_alembic(
        "current",
        env_overrides={
            "DATABASE_URL": None,
            "PMS_DATABASE_URL": None,
        },
    )

    assert result.returncode != 0
    assert "DATABASE_URL" in result.stderr
    assert "PMS_DATABASE_URL" in result.stderr
