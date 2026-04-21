from __future__ import annotations

from collections.abc import Iterator
import os
import socket
import subprocess
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit
from urllib.request import urlopen

import pytest


ROOT = Path(__file__).resolve().parents[2]
ALEMBIC_VERSIONS_DIR = ROOT / "alembic" / "versions"
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


def _run_alembic(database_url: str, *args: str) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env["DATABASE_URL"] = database_url
    env["PMS_AUTO_START"] = "0"
    env.pop("PMS_DATABASE_URL", None)
    return subprocess.run(
        ["uv", "run", "alembic", *args],
        cwd=ROOT,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _run_pms_api(database_url: str, port: int) -> subprocess.Popen[str]:
    env = os.environ.copy()
    env["DATABASE_URL"] = database_url
    env["PMS_AUTO_START"] = "0"
    env.pop("PMS_DATABASE_URL", None)
    return subprocess.Popen(
        [
            "uv",
            "run",
            "pms-api",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "--log-level",
            "warning",
        ],
        cwd=ROOT,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


def _wait_for_status_ok(process: subprocess.Popen[str], port: int) -> None:
    deadline = time.monotonic() + 20.0
    last_error: Exception | None = None

    while time.monotonic() < deadline:
        if process.poll() is not None:
            stdout, stderr = process.communicate()
            msg = f"pms-api exited early\nstdout:\n{stdout}\nstderr:\n{stderr}"
            raise AssertionError(msg)
        try:
            with urlopen(f"http://127.0.0.1:{port}/status", timeout=0.5) as response:
                assert response.status == 200
                return
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            time.sleep(0.1)

    process.terminate()
    stdout, stderr = process.communicate(timeout=10)
    raise AssertionError(
        f"/status never became ready: {last_error}\nstdout:\n{stdout}\nstderr:\n{stderr}"
    )


@contextmanager
def _temporary_revision_file() -> Iterator[str]:
    revision_id = f"0002_cp03_{uuid.uuid4().hex[:8]}"
    path = ALEMBIC_VERSIONS_DIR / f"{revision_id}.py"
    path.write_text(
        "\n".join(
            [
                "from __future__ import annotations",
                "",
                f"revision = \"{revision_id}\"",
                "down_revision = \"0001_baseline\"",
                "branch_labels = None",
                "depends_on = None",
                "",
                "def upgrade() -> None:",
                "    return",
                "",
                "def downgrade() -> None:",
                "    return",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    try:
        yield revision_id
    finally:
        path.unlink(missing_ok=True)


def test_pms_api_boots_when_schema_is_current() -> None:
    assert PMS_TEST_DATABASE_URL is not None

    admin_database_url = _replace_database(PMS_TEST_DATABASE_URL, "postgres")
    temp_database = f"pms_schema_head_{uuid.uuid4().hex[:8]}"
    temp_database_url = _replace_database(PMS_TEST_DATABASE_URL, temp_database)
    process: subprocess.Popen[str] | None = None
    port = _free_port()

    try:
        _run_psql(admin_database_url, "-c", f"CREATE DATABASE {temp_database}")
        upgrade = _run_alembic(temp_database_url, "upgrade", "head")
        assert upgrade.returncode == 0, upgrade.stderr

        process = _run_pms_api(temp_database_url, port)
        _wait_for_status_ok(process, port)
    finally:
        if process is not None:
            process.terminate()
            process.communicate(timeout=10)
        _run_psql(
            admin_database_url,
            "-c",
            f"DROP DATABASE IF EXISTS {temp_database} WITH (FORCE)",
        )


def test_pms_api_boot_fails_when_schema_is_behind_head() -> None:
    assert PMS_TEST_DATABASE_URL is not None

    admin_database_url = _replace_database(PMS_TEST_DATABASE_URL, "postgres")
    temp_database = f"pms_schema_old_{uuid.uuid4().hex[:8]}"
    temp_database_url = _replace_database(PMS_TEST_DATABASE_URL, temp_database)

    try:
        _run_psql(admin_database_url, "-c", f"CREATE DATABASE {temp_database}")
        with _temporary_revision_file() as revision_id:
            upgrade = _run_alembic(temp_database_url, "upgrade", "head")
            downgrade = _run_alembic(temp_database_url, "downgrade", "-1")

            assert upgrade.returncode == 0, upgrade.stderr
            assert downgrade.returncode == 0, downgrade.stderr

            process = _run_pms_api(temp_database_url, _free_port())
            stdout, stderr = process.communicate(timeout=20)

            assert process.returncode != 0
            assert "schema out of date" in stdout + stderr
            assert "0001_baseline" in stdout + stderr
            assert str(revision_id) in stdout + stderr
    finally:
        _run_psql(
            admin_database_url,
            "-c",
            f"DROP DATABASE IF EXISTS {temp_database} WITH (FORCE)",
        )


def test_pms_api_boot_fails_when_schema_is_missing() -> None:
    assert PMS_TEST_DATABASE_URL is not None

    admin_database_url = _replace_database(PMS_TEST_DATABASE_URL, "postgres")
    temp_database = f"pms_schema_missing_{uuid.uuid4().hex[:8]}"
    temp_database_url = _replace_database(PMS_TEST_DATABASE_URL, temp_database)

    try:
        _run_psql(admin_database_url, "-c", f"CREATE DATABASE {temp_database}")

        process = _run_pms_api(temp_database_url, _free_port())
        stdout, stderr = process.communicate(timeout=20)

        assert process.returncode != 0
        assert "schema not initialized" in stdout + stderr
        assert "alembic upgrade head" in stdout + stderr
    finally:
        _run_psql(
            admin_database_url,
            "-c",
            f"DROP DATABASE IF EXISTS {temp_database} WITH (FORCE)",
        )
