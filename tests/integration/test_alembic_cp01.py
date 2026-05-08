from __future__ import annotations

import os
from pathlib import Path
import shutil
import subprocess
import uuid
from urllib.parse import urlsplit, urlunsplit

import pytest


REPO_ROOT = Path(__file__).resolve().parents[2]
PMS_TEST_DATABASE_URL = os.environ.get("PMS_TEST_DATABASE_URL")

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.environ.get("PMS_RUN_INTEGRATION") != "1",
        reason="set PMS_RUN_INTEGRATION=1 to run PostgreSQL integration tests",
    ),
    pytest.mark.skipif(
        not PMS_TEST_DATABASE_URL,
        reason="set PMS_TEST_DATABASE_URL to the compose-backed PostgreSQL URI",
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
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
        timeout=60,
    )


def _alembic_command() -> list[str]:
    executable = REPO_ROOT / ".venv" / "bin" / "alembic"
    if executable.exists():
        return [str(executable)]

    resolved = shutil.which("alembic")
    if resolved is None:
        pytest.fail("alembic executable is not available in the active environment")
    return [resolved]


def _run_alembic(database_url: str, *args: str) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env["DATABASE_URL"] = database_url
    env.pop("PMS_DATABASE__DSN", None)
    env.pop("PMS_DATABASE_URL", None)
    return subprocess.run(
        [*_alembic_command(), *args],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
        env=env,
        timeout=60,
    )


def test_alembic_cp01_reports_version_and_repository_head() -> None:
    assert PMS_TEST_DATABASE_URL
    version = _run_alembic(PMS_TEST_DATABASE_URL, "--version")
    heads = _run_alembic(PMS_TEST_DATABASE_URL, "heads")

    assert version.returncode == 0, version.stdout + version.stderr
    assert "alembic" in version.stdout.lower()
    assert heads.returncode == 0, heads.stdout + heads.stderr
    head_lines = [line for line in heads.stdout.splitlines() if line.strip()]
    assert len(head_lines) == 1
    assert head_lines[0].endswith("(head)")


def test_alembic_cp01_migration_commands_roundtrip_on_temp_database() -> None:
    assert PMS_TEST_DATABASE_URL
    admin_database_url = _replace_database(PMS_TEST_DATABASE_URL, "postgres")
    temp_database = f"pms_alembic_cp01_{uuid.uuid4().hex[:12]}"
    temp_database_url = _replace_database(PMS_TEST_DATABASE_URL, temp_database)

    try:
        _run_psql(admin_database_url, "-c", f"CREATE DATABASE {temp_database}")

        current = _run_alembic(temp_database_url, "current")
        upgrade = _run_alembic(temp_database_url, "upgrade", "head")
        downgrade = _run_alembic(temp_database_url, "downgrade", "base")

        assert current.returncode == 0, current.stdout + current.stderr
        assert current.stdout.strip() == ""
        assert upgrade.returncode == 0, upgrade.stdout + upgrade.stderr
        assert downgrade.returncode == 0, downgrade.stdout + downgrade.stderr
    finally:
        _run_psql(
            admin_database_url,
            "-c",
            f"DROP DATABASE IF EXISTS {temp_database} WITH (FORCE)",
        )
