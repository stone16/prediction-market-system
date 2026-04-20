from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from datetime import datetime
import json
import os
from typing import Any
from uuid import UUID

import asyncpg
import yaml

from pms.research.spec_codec import deserialize_backtest_spec, deserialize_execution_config
from pms.research.sweep import ParameterSweep, QueuedSweepRun


_ORPHANED_FAILURE_REASON = "orphaned (worker process gone)"
PidProbe = Callable[[int], None]


async def enqueue_backtest_runs(
    pg_pool: asyncpg.Pool,
    sweep_yaml: str,
) -> dict[str, object]:
    payload = _load_yaml_payload(sweep_yaml)
    base_spec = deserialize_backtest_spec(payload["base_spec"])
    exec_config = deserialize_execution_config(payload.get("exec_config", {}))
    parameter_grid = _parameter_grid(payload.get("parameter_grid", {}))

    sweep = ParameterSweep(pool=pg_pool)
    variants = sweep.enumerate_variants(base_spec, parameter_grid)
    queued_runs = await sweep.enqueue(variants, exec_config)

    return {
        "run_ids": [queued_run.run_id for queued_run in queued_runs],
        "unique_run_count": len({queued_run.spec_hash for queued_run in queued_runs}),
        "runs": [_serialize_queued_run(queued_run) for queued_run in queued_runs],
    }


async def fetch_backtest_run(
    pg_pool: asyncpg.Pool,
    run_id: str,
) -> dict[str, object] | None:
    async with pg_pool.acquire() as connection:
        row = await connection.fetchrow(
            """
            SELECT
                run_id,
                spec_hash,
                status,
                strategy_ids,
                date_range_start,
                date_range_end,
                exec_config_json,
                spec_json,
                queued_at,
                started_at,
                finished_at,
                failure_reason,
                worker_pid,
                worker_host
            FROM backtest_runs
            WHERE run_id = $1::uuid
            """,
            run_id,
        )
    return None if row is None else _record_to_json(row)


async def list_backtest_strategy_runs(
    pg_pool: asyncpg.Pool,
    run_id: str,
) -> list[dict[str, object]]:
    async with pg_pool.acquire() as connection:
        rows = await connection.fetch(
            """
            SELECT
                strategy_run_id,
                run_id,
                strategy_id,
                strategy_version_id,
                brier,
                pnl_cum,
                drawdown_max,
                fill_rate,
                slippage_bps,
                opportunity_count,
                decision_count,
                fill_count,
                portfolio_target_json,
                started_at,
                finished_at
            FROM strategy_runs
            WHERE run_id = $1::uuid
            ORDER BY started_at ASC NULLS LAST, strategy_id ASC, strategy_version_id ASC
            """,
            run_id,
        )
    return [_record_to_json(row) for row in rows]


async def scan_orphaned_backtest_runs(
    pg_pool: asyncpg.Pool,
    *,
    pid_probe: PidProbe | None = None,
) -> int:
    probe = _pid_exists if pid_probe is None else pid_probe
    async with pg_pool.acquire() as connection:
        rows = await connection.fetch(
            """
            SELECT run_id, worker_pid
            FROM backtest_runs
            WHERE status = 'running' AND worker_pid IS NOT NULL
            ORDER BY started_at ASC NULLS LAST, queued_at ASC, run_id ASC
            """
        )
        orphaned_run_ids: list[str] = []
        for row in rows:
            worker_pid = row["worker_pid"]
            if not isinstance(worker_pid, int):
                continue
            if _pid_missing(worker_pid, probe):
                orphaned_run_ids.append(str(row["run_id"]))

        for orphaned_run_id in orphaned_run_ids:
            await connection.execute(
                """
                UPDATE backtest_runs
                SET
                    status = 'failed',
                    failure_reason = $1,
                    finished_at = COALESCE(finished_at, now())
                WHERE run_id = $2::uuid AND status = 'running'
                """,
                _ORPHANED_FAILURE_REASON,
                orphaned_run_id,
            )

    return len(orphaned_run_ids)


def orphaned_failure_reason() -> str:
    return _ORPHANED_FAILURE_REASON


def _load_yaml_payload(sweep_yaml: str) -> dict[str, object]:
    loaded = yaml.safe_load(sweep_yaml)
    if not isinstance(loaded, dict):
        msg = "sweep spec must decode to a mapping"
        raise TypeError(msg)
    return dict(loaded)


def _parameter_grid(raw_value: object) -> dict[str, Sequence[object]]:
    if raw_value is None:
        return {}
    if not isinstance(raw_value, dict):
        msg = "parameter_grid must decode to a mapping"
        raise TypeError(msg)
    normalized: dict[str, Sequence[object]] = {}
    for key, value in raw_value.items():
        if isinstance(value, (str, bytes, bytearray)) or not isinstance(value, Sequence):
            msg = f"parameter_grid[{key!r}] must be a sequence"
            raise TypeError(msg)
        normalized[str(key)] = tuple(value)
    return normalized


def _serialize_queued_run(queued_run: QueuedSweepRun) -> dict[str, object]:
    return {
        "run_id": queued_run.run_id,
        "spec_hash": queued_run.spec_hash,
        "inserted": queued_run.inserted,
    }


def _pid_exists(pid: int) -> None:
    os.kill(pid, 0)


def _pid_missing(pid: int, probe: PidProbe) -> bool:
    try:
        probe(pid)
    except ProcessLookupError:
        return True
    except PermissionError:
        return False
    return False


def _record_to_json(record: Mapping[str, object]) -> dict[str, object]:
    return {key: _jsonify(value) for key, value in dict(record).items()}


def _jsonify(value: object) -> object:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, list):
        return [_jsonify(item) for item in value]
    if isinstance(value, tuple):
        return [_jsonify(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _jsonify(item) for key, item in value.items()}
    if isinstance(value, str) and _looks_like_json(value):
        decoded = json.loads(value)
        return _jsonify(decoded)
    return value


def _looks_like_json(value: str) -> bool:
    stripped = value.strip()
    return stripped.startswith("{") or stripped.startswith("[")


__all__ = [
    "enqueue_backtest_runs",
    "fetch_backtest_run",
    "list_backtest_strategy_runs",
    "orphaned_failure_reason",
    "scan_orphaned_backtest_runs",
]
