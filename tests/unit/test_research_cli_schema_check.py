from __future__ import annotations

from typing import Any

import pytest

import pms.research.cli as cli


class _FakePool:
    def __init__(self) -> None:
        self.closed = False

    async def close(self) -> None:
        self.closed = True


@pytest.mark.asyncio
async def test_worker_boot_checks_schema_before_processing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pool = _FakePool()
    schema_check_calls: list[_FakePool] = []

    async def _fake_create_pool(_database_url: str | None) -> _FakePool:
        return pool

    async def _fake_ensure_schema_current(candidate: _FakePool) -> None:
        schema_check_calls.append(candidate)

    class _FakeRunner:
        def __init__(self, **_: Any) -> None:
            return

    monkeypatch.setattr(cli, "_create_pool", _fake_create_pool)
    monkeypatch.setattr(cli, "ensure_schema_current", _fake_ensure_schema_current)
    monkeypatch.setattr(cli, "BacktestRunner", _FakeRunner)
    monkeypatch.setattr(cli, "EvaluationReportGenerator", lambda _pool: object())
    monkeypatch.setattr(cli, "_install_signal_handlers", lambda _loop, _request_stop: ())

    exit_code = await cli._run_worker(
        cli._WorkerArgs(database_url=None, poll_interval=0.0, max_runs=0),
    )

    assert exit_code == 0
    assert schema_check_calls == [pool]
    assert pool.closed is True

