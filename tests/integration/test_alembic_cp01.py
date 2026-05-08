from __future__ import annotations

import os
from pathlib import Path
import shutil
import subprocess

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
        PMS_TEST_DATABASE_URL is None,
        reason="set PMS_TEST_DATABASE_URL to the compose-backed PostgreSQL URI",
    ),
]


def _alembic_command() -> list[str]:
    executable = REPO_ROOT / ".venv" / "bin" / "alembic"
    if executable.exists():
        return [str(executable)]

    resolved = shutil.which("alembic")
    if resolved is None:
        pytest.fail("alembic executable is not available in the active environment")
    return [resolved]


def _run_alembic(*args: str) -> subprocess.CompletedProcess[str]:
    assert PMS_TEST_DATABASE_URL is not None
    env = os.environ.copy()
    env["DATABASE_URL"] = PMS_TEST_DATABASE_URL
    return subprocess.run(
        [*_alembic_command(), *args],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )


def test_alembic_cp01_reports_version_and_empty_heads() -> None:
    version = _run_alembic("--version")
    heads = _run_alembic("heads")

    assert version.returncode == 0, version.stdout + version.stderr
    assert "alembic" in version.stdout.lower()
    assert heads.returncode == 0, heads.stdout + heads.stderr
    assert heads.stdout.strip() == ""


def test_alembic_cp01_no_migration_commands_are_noops() -> None:
    current = _run_alembic("current")
    upgrade = _run_alembic("upgrade", "head")
    downgrade = _run_alembic("downgrade", "base")

    assert current.returncode == 0, current.stdout + current.stderr
    assert current.stdout.strip() == ""
    assert upgrade.returncode == 0, upgrade.stdout + upgrade.stderr
    assert downgrade.returncode == 0, downgrade.stdout + downgrade.stderr
