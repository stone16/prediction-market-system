from __future__ import annotations

import asyncio
import os
from pathlib import Path
import socket
import subprocess
import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlsplit

import asyncpg
import pytest


class _TestAsyncpgConnection:
    async def execute(self, query: str, *args: object) -> str:
        del query, args
        return "INSERT 0 1"

    async def fetch(self, query: str, *args: object) -> list[dict[str, object]]:
        del query, args
        return []

    async def fetchrow(self, query: str, *args: object) -> None:
        del query, args
        return None

    async def fetchval(self, query: str, *args: object) -> None:
        del query, args
        return None

    def transaction(self) -> "_TestAsyncpgTransactionContext":
        return _TestAsyncpgTransactionContext()


class _TestAsyncpgTransactionContext:
    async def __aenter__(self) -> "_TestAsyncpgTransactionContext":
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        return None


class _TestAsyncpgConnectionContext:
    def __init__(self, pool: "_TestAsyncpgPool") -> None:
        self._pool = pool

    async def __aenter__(self) -> _TestAsyncpgConnection:
        del self._pool
        return _TestAsyncpgConnection()

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        return None


@dataclass
class _TestAsyncpgPool:
    close_calls: int = 0
    closed: bool = False

    async def close(self) -> None:
        self.close_calls += 1
        self.closed = True

    def acquire(self) -> _TestAsyncpgConnectionContext:
        return _TestAsyncpgConnectionContext(self)


@pytest.fixture(autouse=True)
def _stub_runner_asyncpg_pool(
    monkeypatch: pytest.MonkeyPatch,
    request: pytest.FixtureRequest,
) -> None:
    if (
        request.node.get_closest_marker("integration") is not None
        and os.environ.get("PMS_RUN_INTEGRATION") == "1"
    ):
        return

    async def fake_create_pool(*, dsn: str, min_size: int, max_size: int) -> _TestAsyncpgPool:
        return _TestAsyncpgPool()

    monkeypatch.setattr("pms.runner.asyncpg.create_pool", fake_create_pool)


ROOT = Path(__file__).resolve().parents[1]
COMPOSE_FILE = ROOT / "compose.yml"
DEFAULT_COMPOSE_POSTGRES_DSN = "postgresql://postgres:postgres@127.0.0.1:5432/pms_test"


def _run_compose(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["docker", "compose", "-f", str(COMPOSE_FILE), *args],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )


def _compose_postgres_container_id() -> str:
    result = _run_compose("ps", "-q", "postgres")
    if result.returncode != 0:
        raise RuntimeError(
            "docker compose ps -q postgres failed: "
            f"stdout={result.stdout!r} stderr={result.stderr!r}"
        )
    return result.stdout.strip()


def _compose_postgres_port_binding() -> str | None:
    result = _run_compose("port", "postgres", "5432")
    if result.returncode != 0:
        return None
    binding = result.stdout.strip()
    return binding or None


def _is_tcp_port_open(host: str, port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(0.2)
        return sock.connect_ex((host, port)) == 0


async def _wait_for_postgres(dsn: str, *, timeout_s: float = 20.0) -> None:
    deadline = time.monotonic() + timeout_s
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            connection = await asyncpg.connect(dsn)
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            await asyncio.sleep(0.25)
            continue
        await connection.close()
        return
    raise RuntimeError(f"compose postgres never became ready for {dsn}: {last_error}")


@pytest.fixture(scope="session")
def compose_postgres_dsn() -> str:
    dsn = os.environ.get("PMS_TEST_DATABASE_URL", DEFAULT_COMPOSE_POSTGRES_DSN)
    parts = urlsplit(dsn)
    host = parts.hostname or "127.0.0.1"
    port = int(parts.port or 5432)
    if host not in {"127.0.0.1", "localhost"} or port != 5432:
        return dsn

    container_id = _compose_postgres_container_id()
    port_open = _is_tcp_port_open(host, port)
    if not container_id:
        up = _run_compose("up", "-d", "postgres")
        if up.returncode != 0:
            if port_open:
                raise RuntimeError(
                    "compose collision: localhost:5432 is already bound and "
                    "docker compose could not start postgres; refusing to run "
                    f"CP20 smoke against the wrong PostgreSQL. stderr={up.stderr!r}"
                )
            raise RuntimeError(
                "docker compose up -d postgres failed: "
                f"stdout={up.stdout!r} stderr={up.stderr!r}"
            )
        container_id = _compose_postgres_container_id()
        if not container_id:
            raise RuntimeError("docker compose up reported success but postgres has no container id")

    binding = _compose_postgres_port_binding()
    if binding is None or not binding.endswith(":5432"):
        raise RuntimeError(
            "compose postgres is not exposing port 5432 as expected; "
            f"observed binding={binding!r}"
        )

    asyncio.run(_wait_for_postgres(dsn))
    return dsn
