from __future__ import annotations

import os
import subprocess
import uuid

import pytest

from tests.integration.test_schema_check_cp03 import (
    _replace_database,
    _run_alembic,
    _run_pms_api,
    _run_psql,
    _wait_for_status_ok,
)


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


def test_readme_quickstart_commands_boot_pms_api() -> None:
    assert PMS_TEST_DATABASE_URL is not None

    admin_database_url = _replace_database(PMS_TEST_DATABASE_URL, "postgres")
    temp_database = f"pms_readme_quickstart_{uuid.uuid4().hex[:8]}"
    temp_database_url = _replace_database(PMS_TEST_DATABASE_URL, temp_database)
    process: subprocess.Popen[str] | None = None

    try:
        _run_psql(admin_database_url, "-c", f"CREATE DATABASE {temp_database}")
        upgrade = _run_alembic(temp_database_url, "upgrade", "head")

        assert upgrade.returncode == 0, upgrade.stderr

        process = _run_pms_api(temp_database_url, 8014)
        _wait_for_status_ok(process, 8014)
    finally:
        if process is not None:
            process.terminate()
            process.communicate(timeout=10)
        _run_psql(
            admin_database_url,
            "-c",
            f"DROP DATABASE IF EXISTS {temp_database} WITH (FORCE)",
        )
