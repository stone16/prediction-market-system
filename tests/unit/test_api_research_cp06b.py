from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from pms.api.research_routes import (
    _record_to_json,
    enqueue_backtest_runs,
    scan_orphaned_backtest_runs,
)
from pms.research.sweep import QueuedSweepRun


_SWEEP_YAML = """
base_spec:
  strategy_versions:
    - ["alpha", "alpha-v1"]
  dataset:
    source: "fixture"
    version: "v1"
    coverage_start: "2026-04-01T00:00:00+00:00"
    coverage_end: "2026-04-30T00:00:00+00:00"
    market_universe_filter:
      market_ids: ["market-a"]
    data_quality_gaps: []
  execution_model:
    fee_rate: 0.0
    slippage_bps: 5.0
    latency_ms: 0.0
    staleness_ms: 60000.0
    fill_policy: "immediate_or_cancel"
  risk_policy:
    max_position_notional_usdc: 100.0
    max_daily_drawdown_pct: 2.5
    min_order_size_usdc: 1.0
  date_range_start: "2026-04-01T00:00:00+00:00"
  date_range_end: "2026-04-30T00:00:00+00:00"
exec_config:
  chunk_days: 3
  time_budget: 900
parameter_grid:
  strategy_versions:
    - [["alpha", "alpha-v1"]]
    - [["beta", "beta-v1"]]
""".strip()


@pytest.mark.asyncio
async def test_enqueue_backtest_runs_parses_yaml_and_returns_run_ids(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: dict[str, Any] = {}
    fake_pool = object()

    class FakeSweep:
        def __init__(self, *, pool: object) -> None:
            observed["pool"] = pool

        def enumerate_variants(
            self,
            base_spec: object,
            parameter_grid: dict[str, object],
        ) -> list[object]:
            observed["base_spec"] = base_spec
            observed["parameter_grid"] = parameter_grid
            return [base_spec, base_spec]

        async def enqueue(
            self,
            specs: list[object],
            exec_config: object,
        ) -> list[QueuedSweepRun]:
            observed["specs"] = specs
            observed["exec_config"] = exec_config
            return [
                QueuedSweepRun(run_id="run-1", spec_hash="hash-1", inserted=True),
                QueuedSweepRun(run_id="run-2", spec_hash="hash-2", inserted=True),
            ]

    monkeypatch.setattr("pms.api.research_routes.ParameterSweep", FakeSweep)

    payload = await enqueue_backtest_runs(fake_pool, _SWEEP_YAML)

    assert observed["pool"] is fake_pool
    assert observed["base_spec"].strategy_versions == (("alpha", "alpha-v1"),)
    assert observed["parameter_grid"] == {
        "strategy_versions": (
            [["alpha", "alpha-v1"]],
            [["beta", "beta-v1"]],
        )
    }
    assert observed["exec_config"].chunk_days == 3
    assert observed["exec_config"].time_budget == 900
    assert observed["specs"] == [observed["base_spec"], observed["base_spec"]]
    assert payload == {
        "run_ids": ["run-1", "run-2"],
        "unique_run_count": 2,
        "runs": [
            {"run_id": "run-1", "spec_hash": "hash-1", "inserted": True},
            {"run_id": "run-2", "spec_hash": "hash-2", "inserted": True},
        ],
    }


@dataclass
class _FakeConnection:
    running_rows: list[dict[str, object]]
    executed: list[tuple[str, tuple[object, ...]]] = field(default_factory=list)

    async def fetch(self, query: str, *args: object) -> list[dict[str, object]]:
        del args
        assert "FROM backtest_runs" in query
        return list(self.running_rows)

    async def execute(self, query: str, *args: object) -> str:
        self.executed.append((query, args))
        return "UPDATE 1"


class _FakeAcquireContext:
    def __init__(self, connection: _FakeConnection) -> None:
        self._connection = connection

    async def __aenter__(self) -> _FakeConnection:
        return self._connection

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        return None


@dataclass
class _FakePool:
    connection: _FakeConnection

    def acquire(self) -> _FakeAcquireContext:
        return _FakeAcquireContext(self.connection)


@pytest.mark.asyncio
async def test_scan_orphaned_backtest_runs_marks_dead_workers_failed() -> None:
    connection = _FakeConnection(
        running_rows=[
            {"run_id": "run-dead", "worker_pid": 999999},
            {"run_id": "run-alive", "worker_pid": 1234},
        ]
    )
    seen_pids: list[int] = []

    def fake_pid_probe(pid: int) -> None:
        seen_pids.append(pid)
        if pid == 999999:
            raise ProcessLookupError(pid)

    await scan_orphaned_backtest_runs(
        _FakePool(connection),
        pid_probe=fake_pid_probe,
    )

    assert seen_pids == [999999, 1234]
    assert len(connection.executed) == 1
    update_sql, update_args = connection.executed[0]
    assert "UPDATE backtest_runs" in update_sql
    assert update_args == ("orphaned (worker process gone)", "run-dead")


def test_api_package_does_not_reference_backtest_runner() -> None:
    api_sources = sorted(Path("src/pms/api").rglob("*.py"))
    assert api_sources
    for path in api_sources:
        assert "BacktestRunner" not in path.read_text(encoding="utf-8")


def test_record_to_json_only_decodes_known_json_columns() -> None:
    payload = _record_to_json(
        {
            "spec_json": '{"strategy_versions":[["alpha","alpha-v1"]]}',
            "failure_reason": '{"unterminated"',
            "queued_at": datetime(2026, 4, 20, 12, 0, tzinfo=UTC),
        }
    )

    assert payload == {
        "spec_json": {"strategy_versions": [["alpha", "alpha-v1"]]},
        "failure_reason": '{"unterminated"',
        "queued_at": "2026-04-20T12:00:00+00:00",
    }
