"""Export strict research backtest execution CSV artifacts from PostgreSQL."""

from __future__ import annotations

import argparse
import asyncio
import csv
import math
import os
import stat
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal, cast
from uuid import uuid4

import asyncpg

from scripts.artifact_path_safety import (
    require_path_outside_working_tree,
    require_private_parent,
)


EXECUTION_COLUMNS = (
    "decision_id",
    "strategy_id",
    "strategy_version_id",
    "market_id",
    "status",
    "slippage_bps",
    "pnl",
    "rejection_reason",
)
ExecutionStatus = Literal["filled", "rejected"]


@dataclass(frozen=True, slots=True)
class _BacktestExecution:
    decision_id: str
    strategy_id: str
    strategy_version_id: str
    market_id: str
    status: ExecutionStatus
    slippage_bps: float | None
    pnl: float
    rejection_reason: str
    created_at: datetime


def write_backtest_execution_csv(
    rows: Sequence[Mapping[str, object]],
    output_path: Path,
) -> None:
    executions = tuple(
        _parse_execution_row(row, row_number=row_number)
        for row_number, row in enumerate(rows, start=1)
    )
    if not executions:
        msg = "backtest execution export has no rows"
        raise ValueError(msg)
    _require_unique_decision_ids(executions)
    output_rows = [
        _execution_csv_row(execution)
        for execution in sorted(
            executions,
            key=lambda row: (
                row.created_at.timestamp(),
                row.decision_id,
            ),
        )
    ]
    _prepare_output_path(output_path, label="backtest execution export")
    _write_csv_no_follow(
        output_path,
        fieldnames=EXECUTION_COLUMNS,
        rows=output_rows,
        label="backtest execution export",
    )


async def _fetch_backtest_execution_rows(
    *,
    database_url: str,
    run_id: str,
) -> tuple[Mapping[str, object], ...]:
    connection = await asyncpg.connect(database_url)
    try:
        run_row = await connection.fetchrow(
            """
            SELECT status
            FROM backtest_runs
            WHERE run_id = $1::uuid
            """,
            run_id,
        )
        if run_row is None:
            msg = f"backtest run not found: {run_id}"
            raise ValueError(msg)
        status = str(cast(object, run_row["status"]))
        if status != "completed":
            msg = f"backtest run {run_id} is not completed: status={status}"
            raise ValueError(msg)
        rows = await connection.fetch(
            """
            SELECT
                decision_id,
                strategy_id,
                strategy_version_id,
                market_id,
                status,
                slippage_bps,
                pnl,
                rejection_reason,
                created_at
            FROM backtest_execution_rows
            WHERE run_id = $1::uuid
            ORDER BY created_at, decision_id
            """,
            run_id,
        )
    finally:
        await connection.close()
    return tuple(cast(Mapping[str, object], row) for row in rows)


def _parse_execution_row(
    row: Mapping[str, object],
    *,
    row_number: int,
) -> _BacktestExecution:
    decision_id = _required_text(row, "decision_id", row_number=row_number)
    status = _status(row, row_number=row_number)
    slippage_bps = _optional_float(row, "slippage_bps", row_number=row_number)
    rejection_reason = _optional_text(
        row,
        "rejection_reason",
        row_number=row_number,
    )
    if status == "filled":
        if slippage_bps is None:
            msg = f"backtest execution row {row_number}: filled rows require slippage_bps"
            raise ValueError(msg)
        if rejection_reason is not None:
            msg = (
                f"backtest execution row {row_number}: filled rows must not include "
                "rejection_reason"
            )
            raise ValueError(msg)
    else:
        if slippage_bps is not None:
            msg = (
                f"backtest execution row {row_number}: rejected rows must not include "
                "slippage_bps"
            )
            raise ValueError(msg)
        if rejection_reason is None:
            msg = (
                f"backtest execution row {row_number}: rejected rows require "
                "rejection_reason"
            )
            raise ValueError(msg)
    return _BacktestExecution(
        decision_id=decision_id,
        strategy_id=_strategy_component(row, "strategy_id", row_number=row_number),
        strategy_version_id=_strategy_component(
            row,
            "strategy_version_id",
            row_number=row_number,
        ),
        market_id=_required_text(row, "market_id", row_number=row_number),
        status=status,
        slippage_bps=slippage_bps,
        pnl=_required_float(row, "pnl", row_number=row_number),
        rejection_reason=rejection_reason or "",
        created_at=_datetime_value(row, "created_at", row_number=row_number),
    )


def _execution_csv_row(execution: _BacktestExecution) -> dict[str, str]:
    return {
        "decision_id": execution.decision_id,
        "strategy_id": execution.strategy_id,
        "strategy_version_id": execution.strategy_version_id,
        "market_id": execution.market_id,
        "status": execution.status,
        "slippage_bps": (
            "" if execution.slippage_bps is None else _format_float(execution.slippage_bps)
        ),
        "pnl": _format_float(execution.pnl),
        "rejection_reason": execution.rejection_reason,
    }


def _require_unique_decision_ids(rows: Sequence[_BacktestExecution]) -> None:
    seen: set[str] = set()
    for row in rows:
        if row.decision_id in seen:
            msg = f"duplicate backtest decision_id {row.decision_id!r}"
            raise ValueError(msg)
        seen.add(row.decision_id)


def _status(row: Mapping[str, object], *, row_number: int) -> ExecutionStatus:
    status = _required_text(row, "status", row_number=row_number).lower()
    if status not in {"filled", "rejected"}:
        msg = f"backtest execution row {row_number}: status must be filled or rejected"
        raise ValueError(msg)
    return cast(ExecutionStatus, status)


def _strategy_component(
    row: Mapping[str, object],
    field_name: str,
    *,
    row_number: int,
) -> str:
    value = _required_text(row, field_name, row_number=row_number)
    if "," in value or "@" in value:
        msg = (
            f"backtest execution row {row_number}: {field_name} must not contain "
            "',' or '@'"
        )
        raise ValueError(msg)
    return value


def _required_text(
    row: Mapping[str, object],
    field_name: str,
    *,
    row_number: int,
) -> str:
    value = _optional_text(row, field_name, row_number=row_number)
    if value is None:
        msg = f"backtest execution row {row_number}: missing {field_name}"
        raise ValueError(msg)
    return value


def _optional_text(
    row: Mapping[str, object],
    field_name: str,
    *,
    row_number: int,
) -> str | None:
    raw = row.get(field_name)
    if raw is None:
        return None
    if not isinstance(raw, str):
        msg = f"backtest execution row {row_number}: {field_name} must be text"
        raise ValueError(msg)
    value = raw.strip()
    return value if value else None


def _required_float(
    row: Mapping[str, object],
    field_name: str,
    *,
    row_number: int,
) -> float:
    value = _optional_float(row, field_name, row_number=row_number)
    if value is None:
        msg = f"backtest execution row {row_number}: missing numeric {field_name}"
        raise ValueError(msg)
    return value


def _optional_float(
    row: Mapping[str, object],
    field_name: str,
    *,
    row_number: int,
) -> float | None:
    raw = row.get(field_name)
    if raw is None:
        return None
    if isinstance(raw, str) and raw.strip() == "":
        return None
    if isinstance(raw, bool):
        msg = f"backtest execution row {row_number}: {field_name} must be numeric"
        raise ValueError(msg)
    try:
        numeric = float(cast(float | int | str, raw))
    except (TypeError, ValueError) as exc:
        msg = f"backtest execution row {row_number}: {field_name} must be numeric"
        raise ValueError(msg) from exc
    if not math.isfinite(numeric):
        msg = f"backtest execution row {row_number}: {field_name} must be finite"
        raise ValueError(msg)
    return numeric


def _datetime_value(
    row: Mapping[str, object],
    field_name: str,
    *,
    row_number: int,
) -> datetime:
    raw = row.get(field_name)
    if isinstance(raw, datetime):
        parsed = raw
    elif isinstance(raw, str):
        try:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError as exc:
            msg = f"backtest execution row {row_number}: invalid {field_name}"
            raise ValueError(msg) from exc
    else:
        msg = f"backtest execution row {row_number}: missing {field_name}"
        raise ValueError(msg)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        msg = f"backtest execution row {row_number}: {field_name} must include timezone"
        raise ValueError(msg)
    return parsed.astimezone(UTC)


def _format_float(value: float) -> str:
    return f"{value:.6f}"


def _prepare_output_path(path: Path, *, label: str) -> None:
    require_path_outside_working_tree(path, label=label)
    require_private_parent(path, label=label)
    if path.exists() and not stat.S_ISREG(path.lstat().st_mode):
        msg = f"{label} output path is not a regular file: {path}"
        raise OSError(msg)
    if path.exists() and path.lstat().st_nlink != 1:
        msg = f"{label} output path is not a single-link file: {path}"
        raise OSError(msg)


def _write_csv_no_follow(
    path: Path,
    *,
    fieldnames: Sequence[str],
    rows: Sequence[Mapping[str, str]],
    label: str,
) -> None:
    temp_path = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    fd = os.open(temp_path, flags, 0o600)
    try:
        with os.fdopen(fd, "w", newline="", encoding="utf-8") as f:
            fd = -1
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        os.replace(temp_path, path)
        os.chmod(path, 0o600)
    except Exception:
        try:
            temp_path.unlink(missing_ok=True)
        finally:
            if fd >= 0:
                os.close(fd)
        raise
    if not stat.S_ISREG(path.lstat().st_mode):
        msg = f"{label} output path is not a regular file: {path}"
        raise OSError(msg)
    if path.lstat().st_nlink != 1:
        msg = f"{label} output path is not a single-link file: {path}"
        raise OSError(msg)


def main(argv: Sequence[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    database_url = _database_url_from_args(args)
    if database_url is None:
        print(
            "error: --database-url is required when PMS_DATABASE__DSN, "
            "DATABASE_URL, and PMS_DATABASE_URL are unset",
            file=sys.stderr,
        )
        return 2
    try:
        rows = asyncio.run(
            _fetch_backtest_execution_rows(
                database_url=database_url,
                run_id=cast(str, args.run_id),
            )
        )
        write_backtest_execution_csv(rows, Path(cast(str, args.output)))
    except (OSError, ValueError, asyncpg.PostgresError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    print(f"Backtest execution export written to {args.output}", file=sys.stderr)
    return 0


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Export strict backtest execution CSV artifacts from PostgreSQL."
    )
    parser.add_argument(
        "--database-url",
        default=None,
        help=(
            "PostgreSQL DSN. Defaults to PMS_DATABASE__DSN, DATABASE_URL, "
            "then PMS_DATABASE_URL."
        ),
    )
    parser.add_argument(
        "--run-id",
        required=True,
        help="Completed backtest_runs.run_id to export.",
    )
    parser.add_argument("--output", required=True)
    return parser


def _database_url_from_args(args: argparse.Namespace) -> str | None:
    if isinstance(args.database_url, str) and args.database_url:
        return args.database_url
    for env_name in ("PMS_DATABASE__DSN", "DATABASE_URL", "PMS_DATABASE_URL"):
        value = os.environ.get(env_name)
        if value:
            return value
    return None


if __name__ == "__main__":
    raise SystemExit(main())
