"""Export strict PAPER execution CSV artifacts from PMS API payloads.

This bridge produces the two CSV inputs required by the launch execution
artifact flow:

* ``paper-execution-export.csv`` for ``paper_backtest_execution_diff.py``.
* ``paper-execution-telemetry.csv`` for ``execution_model_from_telemetry.py``.

It deliberately fails closed for launch-critical fields such as explicit PnL
and adverse-selection evidence instead of inventing defaults.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import stat
import sys
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import cast
from uuid import uuid4

from scripts.artifact_path_safety import (
    require_path_outside_working_tree,
    require_private_parent,
)


DEFAULT_API_BASE_URL = "http://127.0.0.1:8000"
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
TELEMETRY_COLUMNS = (
    "slippage_bps",
    "latency_ms",
    "adverse_selection_bps",
)
_TERMINAL_REJECTED_STATUSES = frozenset({"rejected", "expired", "invalid"})


@dataclass(frozen=True, slots=True)
class _FilledExecution:
    decision_id: str
    strategy_id: str
    strategy_version_id: str
    market_id: str
    slippage_bps: float
    pnl: float
    latency_ms: float
    adverse_selection_bps: float | None


@dataclass(frozen=True, slots=True)
class _RejectedExecution:
    decision_id: str
    strategy_id: str
    strategy_version_id: str
    market_id: str
    pnl: float
    rejection_reason: str


def export_paper_execution_artifacts(
    *,
    decisions: Sequence[Mapping[str, object]],
    trades: Sequence[Mapping[str, object]],
    execution_output: Path,
    telemetry_output: Path,
    require_adverse_selection: bool = False,
    allow_open: bool = False,
    strategy_id: str | None = None,
    strategy_version_id: str | None = None,
) -> None:
    """Write strict execution and telemetry CSV artifacts from API payloads."""
    executions = _execution_rows_from_payloads(
        decisions=decisions,
        trades=trades,
        require_adverse_selection=require_adverse_selection,
        allow_open=allow_open,
        strategy_scope=_strategy_scope(
            strategy_id=strategy_id,
            strategy_version_id=strategy_version_id,
        ),
    )
    execution_rows = [_execution_csv_row(execution) for execution in executions]
    telemetry_rows = [
        _telemetry_csv_row(execution)
        for execution in executions
        if isinstance(execution, _FilledExecution)
    ]
    if not telemetry_rows:
        msg = "paper execution telemetry export has no filled rows"
        raise ValueError(msg)

    _prepare_output_path(execution_output, label="paper execution export")
    _prepare_output_path(telemetry_output, label="paper execution telemetry")
    _write_csv_no_follow(
        execution_output,
        fieldnames=EXECUTION_COLUMNS,
        rows=execution_rows,
        label="paper execution export",
    )
    _write_csv_no_follow(
        telemetry_output,
        fieldnames=TELEMETRY_COLUMNS,
        rows=telemetry_rows,
        label="paper execution telemetry",
    )


def _execution_rows_from_payloads(
    *,
    decisions: Sequence[Mapping[str, object]],
    trades: Sequence[Mapping[str, object]],
    require_adverse_selection: bool,
    allow_open: bool,
    strategy_scope: tuple[str, str] | None,
) -> tuple[_FilledExecution | _RejectedExecution, ...]:
    scoped_decisions = _decisions_in_strategy_scope(decisions, strategy_scope)
    scoped_decision_ids = {
        _required_text(decision, "decision_id", label="decision")
        for decision in scoped_decisions
    }
    trades_by_decision = _trades_by_decision_id(
        trades,
        decision_ids=scoped_decision_ids,
    )
    rows: list[_FilledExecution | _RejectedExecution] = []
    for decision in sorted(
        scoped_decisions,
        key=lambda row: (
            _datetime_value(row, "created_at", label="decision").timestamp(),
            _required_text(row, "decision_id", label="decision"),
        ),
    ):
        decision_id = _required_text(decision, "decision_id", label="decision")
        trade = trades_by_decision.get(decision_id)
        if trade is not None:
            rows.append(
                _filled_execution(
                    decision,
                    trade,
                    require_adverse_selection=require_adverse_selection,
                )
            )
            continue

        status = _required_text(decision, "status", label=decision_id).lower()
        if status in _TERMINAL_REJECTED_STATUSES:
            rows.append(_rejected_execution(decision))
            continue
        if allow_open:
            continue
        msg = f"non-terminal PAPER decision without fill: {decision_id} status={status}"
        raise ValueError(msg)
    if not rows:
        if strategy_scope is None:
            msg = "paper execution export has no terminal decisions"
        else:
            strategy_id, strategy_version_id = strategy_scope
            msg = (
                "paper execution export has no terminal decisions for "
                f"{strategy_id}@{strategy_version_id}"
            )
        raise ValueError(msg)
    return tuple(rows)


def _strategy_scope(
    *,
    strategy_id: str | None,
    strategy_version_id: str | None,
) -> tuple[str, str] | None:
    if strategy_id is None and strategy_version_id is None:
        return None
    if strategy_id is None or strategy_version_id is None:
        msg = "strategy-id and strategy-version-id must be provided together"
        raise ValueError(msg)
    return (
        _strategy_identity_value(strategy_id, "strategy_id"),
        _strategy_identity_value(strategy_version_id, "strategy_version_id"),
    )


def _decisions_in_strategy_scope(
    decisions: Sequence[Mapping[str, object]],
    strategy_scope: tuple[str, str] | None,
) -> tuple[Mapping[str, object], ...]:
    if strategy_scope is None:
        return tuple(decisions)
    strategy_id, strategy_version_id = strategy_scope
    return tuple(
        decision
        for decision in decisions
        if _strategy_component(decision, "strategy_id") == strategy_id
        and _strategy_component(decision, "strategy_version_id") == strategy_version_id
    )


def _filled_execution(
    decision: Mapping[str, object],
    trade: Mapping[str, object],
    *,
    require_adverse_selection: bool,
) -> _FilledExecution:
    decision_id = _required_text(decision, "decision_id", label="decision")
    fill_price = _required_float(trade, "fill_price", label=decision_id)
    limit_price = _required_float(decision, "limit_price", label=decision_id)
    action = _action(decision)
    if action == "SELL":
        raw_slippage = limit_price - fill_price
    else:
        raw_slippage = fill_price - limit_price
    slippage_bps = max(0.0, raw_slippage / limit_price * 10_000.0)
    filled_at = _datetime_value(trade, "filled_at", label=decision_id)
    created_at = _datetime_value(decision, "created_at", label=decision_id)
    latency_ms = max(0.0, (filled_at - created_at).total_seconds() * 1000.0)
    evidence = _decision_evidence(decision)
    adverse_selection_bps = _optional_float_from_sources(
        (evidence, trade),
        ("adverse_selection_bps",),
    )
    if require_adverse_selection and adverse_selection_bps is None:
        msg = f"filled PAPER decision {decision_id} missing adverse_selection_bps"
        raise ValueError(msg)
    return _FilledExecution(
        decision_id=decision_id,
        strategy_id=_strategy_component(decision, "strategy_id"),
        strategy_version_id=_strategy_component(decision, "strategy_version_id"),
        market_id=_required_text(decision, "market_id", label=decision_id),
        slippage_bps=slippage_bps,
        pnl=_required_pnl(decision, trade),
        latency_ms=latency_ms,
        adverse_selection_bps=adverse_selection_bps,
    )


def _rejected_execution(decision: Mapping[str, object]) -> _RejectedExecution:
    decision_id = _required_text(decision, "decision_id", label="decision")
    evidence = _decision_evidence(decision)
    rejection_reason = _optional_text_from_sources(
        (evidence, decision),
        ("rejection_reason",),
    )
    if rejection_reason is None:
        msg = f"rejected PAPER decision {decision_id} missing rejection_reason"
        raise ValueError(msg)
    return _RejectedExecution(
        decision_id=decision_id,
        strategy_id=_strategy_component(decision, "strategy_id"),
        strategy_version_id=_strategy_component(decision, "strategy_version_id"),
        market_id=_required_text(decision, "market_id", label=decision_id),
        pnl=_required_pnl(decision, {}),
        rejection_reason=rejection_reason,
    )


def _execution_csv_row(
    execution: _FilledExecution | _RejectedExecution,
) -> dict[str, str]:
    if isinstance(execution, _FilledExecution):
        return {
            "decision_id": execution.decision_id,
            "strategy_id": execution.strategy_id,
            "strategy_version_id": execution.strategy_version_id,
            "market_id": execution.market_id,
            "status": "filled",
            "slippage_bps": _format_float(execution.slippage_bps),
            "pnl": _format_float(execution.pnl),
            "rejection_reason": "",
        }
    return {
        "decision_id": execution.decision_id,
        "strategy_id": execution.strategy_id,
        "strategy_version_id": execution.strategy_version_id,
        "market_id": execution.market_id,
        "status": "rejected",
        "slippage_bps": "",
        "pnl": _format_float(execution.pnl),
        "rejection_reason": execution.rejection_reason,
    }


def _telemetry_csv_row(execution: _FilledExecution) -> dict[str, str]:
    return {
        "slippage_bps": _format_float(execution.slippage_bps),
        "latency_ms": _format_float(execution.latency_ms),
        "adverse_selection_bps": (
            "" if execution.adverse_selection_bps is None else _format_float(execution.adverse_selection_bps)
        ),
    }


def _trades_by_decision_id(
    trades: Sequence[Mapping[str, object]],
    *,
    decision_ids: set[str],
) -> dict[str, Mapping[str, object]]:
    by_id: dict[str, Mapping[str, object]] = {}
    for trade in trades:
        decision_id = _required_text(trade, "decision_id", label="trade")
        if decision_id not in decision_ids:
            continue
        if decision_id in by_id:
            msg = f"multiple trades for PAPER decision {decision_id}"
            raise ValueError(msg)
        by_id[decision_id] = trade
    return by_id


def _required_pnl(
    decision: Mapping[str, object],
    trade: Mapping[str, object],
) -> float:
    evidence = _decision_evidence(decision)
    value = _optional_float_from_sources(
        (evidence, trade),
        ("execution_pnl", "paper_pnl", "pnl"),
    )
    if value is None:
        decision_id = _required_text(decision, "decision_id", label="decision")
        msg = f"PAPER decision {decision_id} missing explicit execution_pnl"
        raise ValueError(msg)
    return value


def _strategy_component(row: Mapping[str, object], field_name: str) -> str:
    value = _required_text(row, field_name, label="decision")
    return _strategy_identity_value(value, field_name)


def _strategy_identity_value(value: str, field_name: str) -> str:
    if "," in value or "@" in value:
        msg = f"{field_name} must not contain ',' or '@'"
        raise ValueError(msg)
    return value


def _action(decision: Mapping[str, object]) -> str:
    action = _optional_text_from_sources((decision,), ("action",))
    if action is None:
        action = _required_text(decision, "side", label="decision")
    normalized = action.upper()
    if normalized not in {"BUY", "SELL"}:
        msg = f"decision action must be BUY or SELL: {action}"
        raise ValueError(msg)
    return normalized


def _decision_evidence(row: Mapping[str, object]) -> Mapping[str, object]:
    raw = row.get("decision_evidence")
    if isinstance(raw, Mapping):
        return cast(Mapping[str, object], raw)
    return {}


def _required_text(
    row: Mapping[str, object],
    field_name: str,
    *,
    label: str,
) -> str:
    value = _optional_text_from_sources((row,), (field_name,))
    if value is None:
        msg = f"{label} missing {field_name}"
        raise ValueError(msg)
    return value


def _optional_text_from_sources(
    source_maps: Sequence[Mapping[str, object]],
    keys: Sequence[str],
) -> str | None:
    for source in source_maps:
        for key in keys:
            raw = source.get(key)
            if isinstance(raw, str) and raw.strip() != "":
                return raw.strip()
    return None


def _required_float(
    row: Mapping[str, object],
    field_name: str,
    *,
    label: str,
) -> float:
    value = _optional_float_from_sources((row,), (field_name,))
    if value is None:
        msg = f"{label} missing numeric {field_name}"
        raise ValueError(msg)
    return value


def _optional_float_from_sources(
    source_maps: Sequence[Mapping[str, object]],
    keys: Sequence[str],
) -> float | None:
    for source in source_maps:
        for key in keys:
            raw = source.get(key)
            if raw is None or isinstance(raw, bool):
                continue
            try:
                value = float(cast(float | int | str, raw))
            except (TypeError, ValueError):
                continue
            if value == float("inf") or value == float("-inf") or value != value:
                continue
            return value
    return None


def _datetime_value(
    row: Mapping[str, object],
    field_name: str,
    *,
    label: str,
) -> datetime:
    raw = _required_text(row, field_name, label=label)
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError as exc:
        msg = f"{label} invalid {field_name}"
        raise ValueError(msg) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        msg = f"{label} {field_name} must include timezone"
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
    try:
        decisions, trades = _load_inputs(args)
        export_paper_execution_artifacts(
            decisions=decisions,
            trades=trades,
            execution_output=Path(cast(str, args.execution_output)),
            telemetry_output=Path(cast(str, args.telemetry_output)),
            require_adverse_selection=bool(args.require_adverse_selection),
            allow_open=bool(args.allow_open),
            strategy_id=cast(str | None, args.strategy_id),
            strategy_version_id=cast(str | None, args.strategy_version_id),
        )
    except (OSError, ValueError, urllib.error.URLError, json.JSONDecodeError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    print(f"Paper execution export written to {args.execution_output}", file=sys.stderr)
    print(f"Paper execution telemetry written to {args.telemetry_output}", file=sys.stderr)
    return 0


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Export strict PAPER execution CSV artifacts from PMS API data."
    )
    parser.add_argument(
        "--api-base-url",
        default=DEFAULT_API_BASE_URL,
        help="PMS API base URL used when JSON inputs are not supplied.",
    )
    parser.add_argument(
        "--api-token",
        default=os.environ.get("PMS_API_TOKEN"),
        help="Bearer token for PMS API. Defaults to PMS_API_TOKEN.",
    )
    parser.add_argument("--decisions-json", help="Captured /decisions JSON path.")
    parser.add_argument("--trades-json", help="Captured /trades JSON path.")
    parser.add_argument("--execution-output", required=True)
    parser.add_argument("--telemetry-output", required=True)
    parser.add_argument(
        "--strategy-id",
        help="Optional strategy_id to include in the export.",
    )
    parser.add_argument(
        "--strategy-version-id",
        help="Required with --strategy-id; immutable strategy_version_id to include.",
    )
    parser.add_argument("--require-adverse-selection", action="store_true")
    parser.add_argument(
        "--allow-open",
        action="store_true",
        help="Skip non-terminal decisions instead of failing.",
    )
    return parser


def _load_inputs(
    args: argparse.Namespace,
) -> tuple[tuple[Mapping[str, object], ...], tuple[Mapping[str, object], ...]]:
    decisions_path = cast(str | None, args.decisions_json)
    trades_path = cast(str | None, args.trades_json)
    if decisions_path is not None or trades_path is not None:
        if decisions_path is None or trades_path is None:
            msg = "--decisions-json and --trades-json must be provided together"
            raise ValueError(msg)
        return (
            _load_decisions_json(Path(decisions_path)),
            _load_trades_json(Path(trades_path)),
        )
    return (
        _fetch_api_pages(
            api_base_url=cast(str, args.api_base_url),
            path="/decisions",
            payload_key=None,
            api_token=cast(str | None, args.api_token),
        ),
        _fetch_api_pages(
            api_base_url=cast(str, args.api_base_url),
            path="/trades",
            payload_key="trades",
            api_token=cast(str | None, args.api_token),
        ),
    )


def _load_decisions_json(path: Path) -> tuple[Mapping[str, object], ...]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, list):
        return tuple(_mapping_items(payload, label="decisions JSON"))
    if isinstance(payload, dict) and isinstance(payload.get("decisions"), list):
        return tuple(_mapping_items(payload["decisions"], label="decisions JSON"))
    msg = "decisions JSON must be a list or an object with a decisions list"
    raise ValueError(msg)


def _load_trades_json(path: Path) -> tuple[Mapping[str, object], ...]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict) and isinstance(payload.get("trades"), list):
        return tuple(_mapping_items(payload["trades"], label="trades JSON"))
    if isinstance(payload, list):
        return tuple(_mapping_items(payload, label="trades JSON"))
    msg = "trades JSON must be a list or an object with a trades list"
    raise ValueError(msg)


def _fetch_api_pages(
    *,
    api_base_url: str,
    path: str,
    payload_key: str | None,
    api_token: str | None,
    page_limit: int = 200,
) -> tuple[Mapping[str, object], ...]:
    rows: list[Mapping[str, object]] = []
    offset = 0
    while True:
        query = urllib.parse.urlencode({"limit": page_limit, "offset": offset})
        payload = _fetch_api_json(
            api_base_url=api_base_url,
            path=f"{path}?{query}",
            api_token=api_token,
        )
        raw_rows: object
        if payload_key is None:
            raw_rows = payload
        elif isinstance(payload, Mapping):
            raw_rows = payload.get(payload_key)
        else:
            raw_rows = None
        page_rows = tuple(_mapping_items(raw_rows, label=path))
        rows.extend(page_rows)
        if len(page_rows) < page_limit:
            break
        offset += page_limit
    return tuple(rows)


def _fetch_api_json(
    *,
    api_base_url: str,
    path: str,
    api_token: str | None,
) -> object:
    request = urllib.request.Request(f"{api_base_url.rstrip('/')}{path}")
    if api_token:
        request.add_header("Authorization", f"Bearer {api_token}")
    with urllib.request.urlopen(request, timeout=30.0) as response:
        return json.loads(response.read().decode("utf-8"))


def _mapping_items(raw_rows: object, *, label: str) -> tuple[Mapping[str, object], ...]:
    if not isinstance(raw_rows, list):
        msg = f"{label} must contain a list"
        raise ValueError(msg)
    rows: list[Mapping[str, object]] = []
    for index, raw_row in enumerate(raw_rows, start=1):
        if not isinstance(raw_row, Mapping):
            msg = f"{label} row {index} must be an object"
            raise ValueError(msg)
        rows.append(cast(Mapping[str, object], raw_row))
    return tuple(rows)


if __name__ == "__main__":
    raise SystemExit(main())
