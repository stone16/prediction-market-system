from __future__ import annotations

import asyncio
from typing import Any

import pytest

import tests.conftest as root_conftest


def _run_coroutine(coro: Any) -> object:
    while True:
        try:
            return coro.send(None)
        except StopIteration as exc:
            return exc.value


def test_resolve_compose_postgres_dsn_short_circuits_when_default_dsn_is_reachable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    async def _reachable(dsn: str, *, timeout_s: float = 20.0) -> None:
        calls.append(f"{dsn}|{timeout_s}")

    monkeypatch.delenv("PMS_TEST_DATABASE_URL", raising=False)
    monkeypatch.setattr(asyncio, "run", _run_coroutine)
    monkeypatch.setattr(root_conftest, "_wait_for_postgres", _reachable)
    monkeypatch.setattr(
        root_conftest,
        "_compose_postgres_container_id",
        lambda: pytest.fail("compose inspection should not run when DSN is reachable"),
    )

    resolved = root_conftest._resolve_compose_postgres_dsn()

    assert resolved == root_conftest.DEFAULT_COMPOSE_POSTGRES_DSN
    assert calls == [f"{root_conftest.DEFAULT_COMPOSE_POSTGRES_DSN}|1.0"]


def test_resolve_compose_postgres_dsn_rewrites_to_published_host_port(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    async def _wait(dsn: str, *, timeout_s: float = 20.0) -> None:
        calls.append(f"{dsn}|{timeout_s}")
        if timeout_s == 1.0:
            raise RuntimeError("not ready on default port")

    monkeypatch.delenv("PMS_TEST_DATABASE_URL", raising=False)
    monkeypatch.setattr(asyncio, "run", _run_coroutine)
    monkeypatch.setattr(root_conftest, "_wait_for_postgres", _wait)
    monkeypatch.setattr(root_conftest, "_compose_postgres_container_id", lambda: "container-123")
    monkeypatch.setattr(
        root_conftest,
        "_compatible_running_postgres_container_id",
        lambda: None,
    )
    monkeypatch.setattr(root_conftest, "_compose_postgres_port_binding", lambda: "0.0.0.0:55432")

    resolved = root_conftest._resolve_compose_postgres_dsn()

    assert resolved == "postgresql://postgres:postgres@127.0.0.1:55432/pms_test"
    assert calls == [
        f"{root_conftest.DEFAULT_COMPOSE_POSTGRES_DSN}|1.0",
        "postgresql://postgres:postgres@127.0.0.1:55432/pms_test|20.0",
    ]
