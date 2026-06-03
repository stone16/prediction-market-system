"""Compare paper execution telemetry against a matching backtest replay export."""

from __future__ import annotations

import argparse
import csv
import io
import json
import os
import stat
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from typing import Literal, cast
from uuid import uuid4


GENERATED_BY = "scripts/paper_backtest_execution_diff.py"
ARTIFACT_MODE = "paper_backtest_execution_diff"
DEFAULT_MIN_MATCHED_DECISIONS = 10
ExecutionStatus = Literal["filled", "rejected"]
REQUIRED_COLUMNS = frozenset({
    "decision_id",
    "strategy_id",
    "strategy_version_id",
    "market_id",
    "status",
    "slippage_bps",
    "pnl",
    "rejection_reason",
})


@dataclass(frozen=True, slots=True)
class ExecutionRow:
    decision_id: str
    strategy_id: str
    strategy_version_id: str
    market_id: str
    status: ExecutionStatus
    slippage_bps: float | None
    pnl: float
    rejection_reason: str


@dataclass(frozen=True, slots=True)
class ExecutionDiff:
    generated_by: str
    artifact_mode: str
    generated_at: str
    strategy_evidence: str
    input_csv_sha256: Mapping[str, str]
    final_go_no_go_valid: bool
    thresholds: Mapping[str, float]
    metrics: Mapping[str, float | int | None]
    paper_only_decision_ids: tuple[str, ...]
    backtest_only_decision_ids: tuple[str, ...]
    status_mismatches: tuple[str, ...]
    failures: tuple[str, ...]


def build_execution_diff(
    *,
    paper_path: Path,
    backtest_path: Path,
    max_fill_rate_delta: float = 0.05,
    max_rejection_rate_delta: float = 0.05,
    max_avg_slippage_bps_delta: float = 5.0,
    max_total_pnl_delta: float = 1.0,
    min_matched_decisions: int = DEFAULT_MIN_MATCHED_DECISIONS,
) -> ExecutionDiff:
    min_matched_decisions_value = _positive_int(
        min_matched_decisions,
        "min_matched_decisions",
    )
    thresholds = {
        "max_fill_rate_delta": _nonnegative_finite(
            max_fill_rate_delta,
            "max_fill_rate_delta",
        ),
        "max_rejection_rate_delta": _nonnegative_finite(
            max_rejection_rate_delta,
            "max_rejection_rate_delta",
        ),
        "max_avg_slippage_bps_delta": _nonnegative_finite(
            max_avg_slippage_bps_delta,
            "max_avg_slippage_bps_delta",
        ),
        "max_total_pnl_delta": _nonnegative_finite(
            max_total_pnl_delta,
            "max_total_pnl_delta",
        ),
        "min_matched_decisions": float(min_matched_decisions_value),
    }
    paper_input = _load_execution_rows(paper_path, label="paper")
    backtest_input = _load_execution_rows(backtest_path, label="backtest")
    paper_rows = paper_input.rows
    backtest_rows = backtest_input.rows
    strategy_evidence = _strategy_evidence(
        paper_rows=paper_rows,
        backtest_rows=backtest_rows,
    )
    paper_by_id = {row.decision_id: row for row in paper_rows}
    backtest_by_id = {row.decision_id: row for row in backtest_rows}

    paper_ids = set(paper_by_id)
    backtest_ids = set(backtest_by_id)
    paper_only = tuple(sorted(paper_ids - backtest_ids))
    backtest_only = tuple(sorted(backtest_ids - paper_ids))
    shared_ids = tuple(sorted(paper_ids & backtest_ids))
    status_mismatches = tuple(
        (
            f"status mismatch {decision_id}: "
            f"paper={paper_by_id[decision_id].status} "
            f"backtest={backtest_by_id[decision_id].status}"
        )
        for decision_id in shared_ids
        if paper_by_id[decision_id].status != backtest_by_id[decision_id].status
    )

    paper_summary = _summary(paper_rows)
    backtest_summary = _summary(backtest_rows)
    avg_slippage_delta = _abs_delta(
        paper_summary["avg_slippage_bps"],
        backtest_summary["avg_slippage_bps"],
    )
    metrics: dict[str, float | int | None] = {
        "paper_decision_count": len(paper_rows),
        "backtest_decision_count": len(backtest_rows),
        "matched_decision_count": len(shared_ids),
        "paper_fill_rate": paper_summary["fill_rate"],
        "backtest_fill_rate": backtest_summary["fill_rate"],
        "fill_rate_delta_abs": abs(
            cast(float, paper_summary["fill_rate"])
            - cast(float, backtest_summary["fill_rate"])
        ),
        "paper_rejection_rate": paper_summary["rejection_rate"],
        "backtest_rejection_rate": backtest_summary["rejection_rate"],
        "rejection_rate_delta_abs": abs(
            cast(float, paper_summary["rejection_rate"])
            - cast(float, backtest_summary["rejection_rate"])
        ),
        "paper_avg_slippage_bps": paper_summary["avg_slippage_bps"],
        "backtest_avg_slippage_bps": backtest_summary["avg_slippage_bps"],
        "avg_slippage_bps_delta_abs": avg_slippage_delta,
        "paper_total_pnl": paper_summary["total_pnl"],
        "backtest_total_pnl": backtest_summary["total_pnl"],
        "total_pnl_delta_abs": abs(
            cast(float, paper_summary["total_pnl"])
            - cast(float, backtest_summary["total_pnl"])
        ),
    }
    failures = _failures(
        metrics=metrics,
        thresholds=thresholds,
        paper_only=paper_only,
        backtest_only=backtest_only,
        status_mismatches=status_mismatches,
        min_matched_decisions=min_matched_decisions_value,
    )
    return ExecutionDiff(
        generated_by=GENERATED_BY,
        artifact_mode=ARTIFACT_MODE,
        generated_at=datetime.now(tz=UTC).isoformat(),
        strategy_evidence=strategy_evidence,
        input_csv_sha256={
            "paper": paper_input.csv_sha256,
            "backtest": backtest_input.csv_sha256,
        },
        final_go_no_go_valid=not failures,
        thresholds=thresholds,
        metrics=metrics,
        paper_only_decision_ids=paper_only,
        backtest_only_decision_ids=backtest_only,
        status_mismatches=status_mismatches,
        failures=tuple(failures),
    )


def save_execution_diff_json(diff: ExecutionDiff, path: Path) -> None:
    _prepare_private_output_parent(path)
    _write_text_no_follow(
        path,
        json.dumps(
            execution_diff_to_json_dict(diff),
            allow_nan=False,
            indent=2,
            sort_keys=True,
        )
        + "\n",
    )


def execution_diff_to_json_dict(diff: ExecutionDiff) -> dict[str, object]:
    return {
        "generated_by": diff.generated_by,
        "artifact_mode": diff.artifact_mode,
        "generated_at": diff.generated_at,
        "strategy_evidence": diff.strategy_evidence,
        "input_csv_sha256": dict(diff.input_csv_sha256),
        "final_go_no_go_valid": diff.final_go_no_go_valid,
        "thresholds": dict(diff.thresholds),
        "metrics": dict(diff.metrics),
        "paper_only_decision_ids": list(diff.paper_only_decision_ids),
        "backtest_only_decision_ids": list(diff.backtest_only_decision_ids),
        "status_mismatches": list(diff.status_mismatches),
        "failures": list(diff.failures),
    }


@dataclass(frozen=True, slots=True)
class _LoadedExecutionRows:
    rows: tuple[ExecutionRow, ...]
    csv_sha256: str


def _load_execution_rows(path: Path, *, label: str) -> _LoadedExecutionRows:
    raw_bytes = _read_bytes_no_follow(path, label=label)
    text = raw_bytes.decode("utf-8")
    with io.StringIO(text, newline="") as f:
        reader = csv.DictReader(f)
        _require_unique_csv_fieldnames(reader.fieldnames)
        fieldnames = set(reader.fieldnames or ())
        missing = REQUIRED_COLUMNS - fieldnames
        if missing:
            missing_display = ", ".join(sorted(missing))
            raise ValueError(
                f"{label} execution CSV missing required columns: {missing_display}"
            )
        rows: list[ExecutionRow] = []
        seen_decision_ids: set[str] = set()
        for row_number, row in enumerate(reader, start=2):
            parsed = _parse_execution_row(row, row_number=row_number, label=label)
            if parsed.decision_id in seen_decision_ids:
                raise ValueError(
                    f"{label} execution row {row_number}: duplicate decision_id "
                    f"{parsed.decision_id!r}"
                )
            seen_decision_ids.add(parsed.decision_id)
            rows.append(parsed)
    if not rows:
        raise ValueError(f"{label} execution CSV must contain at least one row")
    return _LoadedExecutionRows(
        rows=tuple(rows),
        csv_sha256=sha256(raw_bytes).hexdigest(),
    )


def _parse_execution_row(
    row: Mapping[str, str | None],
    *,
    row_number: int,
    label: str,
) -> ExecutionRow:
    decision_id = _required_text(row, "decision_id", row_number=row_number, label=label)
    status_raw = _required_text(row, "status", row_number=row_number, label=label)
    status = status_raw.strip().lower()
    if status not in ("filled", "rejected"):
        raise ValueError(
            f"{label} execution row {row_number}: status must be filled or rejected"
        )
    slippage_bps = _optional_float(row, "slippage_bps", row_number=row_number, label=label)
    if status == "filled" and slippage_bps is None:
        raise ValueError(
            f"{label} execution row {row_number}: filled rows require slippage_bps"
        )
    return ExecutionRow(
        decision_id=decision_id,
        strategy_id=_strategy_identity_component(
            row,
            "strategy_id",
            row_number=row_number,
            label=label,
        ),
        strategy_version_id=_strategy_identity_component(
            row,
            "strategy_version_id",
            row_number=row_number,
            label=label,
        ),
        market_id=_required_text(row, "market_id", row_number=row_number, label=label),
        status=cast(ExecutionStatus, status),
        slippage_bps=slippage_bps,
        pnl=_required_float(row, "pnl", row_number=row_number, label=label),
        rejection_reason=(row.get("rejection_reason") or "").strip(),
    )


def _strategy_evidence(
    *,
    paper_rows: Sequence[ExecutionRow],
    backtest_rows: Sequence[ExecutionRow],
) -> str:
    paper_labels = _strategy_labels(paper_rows)
    backtest_labels = _strategy_labels(backtest_rows)
    if paper_labels != backtest_labels:
        msg = (
            "paper/backtest execution CSV strategy evidence mismatch: "
            f"paper={', '.join(paper_labels)} "
            f"backtest={', '.join(backtest_labels)}"
        )
        raise ValueError(msg)
    return ", ".join(paper_labels)


def _strategy_labels(rows: Sequence[ExecutionRow]) -> tuple[str, ...]:
    return tuple(
        sorted(
            {
                f"{row.strategy_id}@{row.strategy_version_id}"
                for row in rows
            }
        )
    )


def _strategy_identity_component(
    row: Mapping[str, str | None],
    column: str,
    *,
    row_number: int,
    label: str,
) -> str:
    value = _required_text(row, column, row_number=row_number, label=label)
    if "," in value or "@" in value:
        raise ValueError(
            f"{label} execution row {row_number}: {column} must not contain "
            "',' or '@'"
        )
    return value


def _summary(rows: Sequence[ExecutionRow]) -> dict[str, float | None]:
    filled_rows = [row for row in rows if row.status == "filled"]
    rejected_rows = [row for row in rows if row.status == "rejected"]
    slippage_values = [
        row.slippage_bps
        for row in filled_rows
        if row.slippage_bps is not None
    ]
    return {
        "fill_rate": len(filled_rows) / len(rows),
        "rejection_rate": len(rejected_rows) / len(rows),
        "avg_slippage_bps": (
            sum(slippage_values) / len(slippage_values) if slippage_values else None
        ),
        "total_pnl": sum(row.pnl for row in rows),
    }


def _abs_delta(left: float | None, right: float | None) -> float | None:
    if left is None and right is None:
        return 0.0
    if left is None or right is None:
        return None
    return abs(left - right)


def _failures(
    *,
    metrics: Mapping[str, float | int | None],
    thresholds: Mapping[str, float],
    paper_only: tuple[str, ...],
    backtest_only: tuple[str, ...],
    status_mismatches: tuple[str, ...],
    min_matched_decisions: int,
) -> list[str]:
    failures: list[str] = []
    if paper_only:
        failures.append(f"paper-only decision ids: {', '.join(paper_only)}")
    if backtest_only:
        failures.append(f"backtest-only decision ids: {', '.join(backtest_only)}")
    failures.extend(status_mismatches)
    matched_decision_count = int(metrics["matched_decision_count"] or 0)
    if matched_decision_count < min_matched_decisions:
        failures.append(
            "matched_decision_count "
            f"{matched_decision_count} < min_matched_decisions "
            f"{min_matched_decisions}"
        )
    _append_threshold_failure(
        failures,
        metric_name="fill_rate_delta_abs",
        metrics=metrics,
        threshold_name="max_fill_rate_delta",
        thresholds=thresholds,
    )
    _append_threshold_failure(
        failures,
        metric_name="rejection_rate_delta_abs",
        metrics=metrics,
        threshold_name="max_rejection_rate_delta",
        thresholds=thresholds,
    )
    _append_threshold_failure(
        failures,
        metric_name="avg_slippage_bps_delta_abs",
        metrics=metrics,
        threshold_name="max_avg_slippage_bps_delta",
        thresholds=thresholds,
    )
    _append_threshold_failure(
        failures,
        metric_name="total_pnl_delta_abs",
        metrics=metrics,
        threshold_name="max_total_pnl_delta",
        thresholds=thresholds,
    )
    return failures


def _append_threshold_failure(
    failures: list[str],
    *,
    metric_name: str,
    metrics: Mapping[str, float | int | None],
    threshold_name: str,
    thresholds: Mapping[str, float],
) -> None:
    metric_value = metrics[metric_name]
    threshold = thresholds[threshold_name]
    if metric_value is None:
        failures.append(f"{metric_name} unavailable")
        return
    numeric = float(metric_value)
    if numeric > threshold:
        failures.append(f"{metric_name} {numeric:.6g} > {threshold_name} {threshold:.6g}")


def _required_text(
    row: Mapping[str, str | None],
    column: str,
    *,
    row_number: int,
    label: str,
) -> str:
    value = row.get(column)
    if value is None or value.strip() == "":
        raise ValueError(f"{label} execution row {row_number}: missing {column}")
    return value.strip()


def _required_float(
    row: Mapping[str, str | None],
    column: str,
    *,
    row_number: int,
    label: str,
) -> float:
    value = _required_text(row, column, row_number=row_number, label=label)
    return _parse_nonnegative_or_signed_float(
        value,
        column=column,
        row_number=row_number,
        label=label,
    )


def _optional_float(
    row: Mapping[str, str | None],
    column: str,
    *,
    row_number: int,
    label: str,
) -> float | None:
    value = row.get(column)
    if value is None or value.strip() == "":
        return None
    return _parse_nonnegative_or_signed_float(
        value,
        column=column,
        row_number=row_number,
        label=label,
    )


def _parse_nonnegative_or_signed_float(
    value: str,
    *,
    column: str,
    row_number: int,
    label: str,
) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise ValueError(
            f"{label} execution row {row_number}: {column} must be numeric"
        ) from exc
    if parsed != parsed or parsed in (float("inf"), float("-inf")):
        raise ValueError(
            f"{label} execution row {row_number}: {column} must be finite"
        )
    return parsed


def _nonnegative_finite(value: float, field_name: str) -> float:
    numeric = float(value)
    if numeric != numeric or numeric in (float("inf"), float("-inf")) or numeric < 0.0:
        raise ValueError(f"{field_name} must be finite and >= 0")
    return numeric


def _positive_int(value: int, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{field_name} must be an integer > 0")
    return value


def _read_bytes_no_follow(path: Path, *, label: str) -> bytes:
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    fd = -1
    try:
        fd = os.open(path, flags, 0o777)
        path_stat = os.fstat(fd)
        if not stat.S_ISREG(path_stat.st_mode):
            raise OSError(f"{label} execution CSV cannot be read safely: {path}")
        if path_stat.st_nlink != 1:
            raise OSError(f"{label} execution CSV cannot be read safely: {path}")
        with os.fdopen(fd, "rb") as file:
            fd = -1
            return file.read()
    except OSError as exc:
        msg = f"{label} execution CSV cannot be read safely: {path}"
        raise ValueError(msg) from exc
    finally:
        if fd >= 0:
            os.close(fd)


def _require_unique_csv_fieldnames(fieldnames: Sequence[str] | None) -> None:
    if fieldnames is None:
        return
    seen: set[str] = set()
    for fieldname in fieldnames:
        if fieldname in seen:
            msg = f"duplicate CSV column: {fieldname}"
            raise ValueError(msg)
        seen.add(fieldname)


def _write_text_no_follow(path: Path, content: str) -> None:
    _require_regular_file_or_absent(path)
    fd, temp_path = _open_output_temp_file(path)
    published = False
    try:
        os.fchmod(fd, 0o600)
        os.ftruncate(fd, 0)
        os.lseek(fd, 0, os.SEEK_SET)
        with os.fdopen(fd, "w", encoding="utf-8") as file:
            fd = -1
            file.write(content)
            file.flush()
            os.fsync(file.fileno())
        _require_regular_file_or_absent(path)
        os.replace(temp_path, path)
        published = True
        _fsync_parent_directory(path)
    finally:
        if fd >= 0:
            os.close(fd)
        if not published:
            _unlink_regular_single_link_file_if_present(temp_path)


def _open_output_temp_file(path: Path) -> tuple[int, Path]:
    _require_regular_file_or_absent(path)
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    for _ in range(16):
        temp_path = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
        try:
            fd = os.open(temp_path, flags, 0o600)
        except FileExistsError:
            continue
        try:
            _require_open_regular_single_link_file(fd, temp_path)
            os.fchmod(fd, 0o600)
        except BaseException:
            os.close(fd)
            _unlink_regular_single_link_file_if_present(temp_path)
            raise
        return fd, temp_path
    raise FileExistsError(
        f"could not create temporary paper/backtest execution diff for {path}"
    )


def _unlink_regular_single_link_file_if_present(path: Path) -> None:
    try:
        path_stat = path.lstat()
    except FileNotFoundError:
        return
    if not stat.S_ISREG(path_stat.st_mode) or path_stat.st_nlink != 1:
        return
    path.unlink()


def _fsync_parent_directory(path: Path) -> None:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    try:
        fd = os.open(path.parent, flags)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        return
    finally:
        os.close(fd)


def _prepare_private_output_parent(path: Path) -> None:
    parent = path.parent
    try:
        mode = parent.lstat().st_mode
    except FileNotFoundError:
        parent.mkdir(parents=True, mode=0o700, exist_ok=False)
        os.chmod(parent, 0o700)
        return
    if not stat.S_ISDIR(mode):
        raise OSError(
            f"paper/backtest execution diff output parent is not a directory: {parent}"
        )
    permissions = stat.S_IMODE(mode)
    if permissions & 0o077:
        raise OSError(
            f"paper/backtest execution diff output parent {parent} "
            f"is too permissive; run `chmod 700 {parent}`."
        )
    if not permissions & stat.S_IWUSR:
        raise OSError(
            f"paper/backtest execution diff output parent {parent} "
            f"is not owner-writable; run `chmod 700 {parent}`."
        )


def _require_open_regular_single_link_file(fd: int, path: Path) -> None:
    path_stat = os.fstat(fd)
    if not stat.S_ISREG(path_stat.st_mode):
        raise OSError(
            f"paper/backtest execution diff output path is not a regular file: {path}"
        )
    if path_stat.st_nlink != 1:
        raise OSError(
            f"paper/backtest execution diff output path is not a single-link file: {path}"
        )


def _require_regular_file_or_absent(path: Path) -> None:
    try:
        path_stat = path.lstat()
    except FileNotFoundError:
        return
    if not stat.S_ISREG(path_stat.st_mode):
        raise OSError(
            f"paper/backtest execution diff output path is not a regular file: {path}"
        )
    if path_stat.st_nlink != 1:
        raise OSError(
            f"paper/backtest execution diff output path is not a single-link file: {path}"
        )


def main(argv: Sequence[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    try:
        paper_path = Path(args.paper)
        backtest_path = Path(args.backtest)
        output_path = Path(args.output)
        _require_output_distinct_from_inputs(
            output_path,
            input_paths=(paper_path, backtest_path),
        )
        diff = build_execution_diff(
            paper_path=paper_path,
            backtest_path=backtest_path,
            max_fill_rate_delta=args.max_fill_rate_delta,
            max_rejection_rate_delta=args.max_rejection_rate_delta,
            max_avg_slippage_bps_delta=args.max_avg_slippage_bps_delta,
            max_total_pnl_delta=args.max_total_pnl_delta,
            min_matched_decisions=args.min_matched_decisions,
        )
        save_execution_diff_json(diff, output_path)
    except (OSError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    print(f"Paper/backtest execution diff JSON written to {args.output}", file=sys.stderr)
    if args.require_pass and not diff.final_go_no_go_valid:
        return 1
    return 0


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Compare paper and research-backtest execution exports for "
            "fill/rejection/slippage/PnL alignment."
        )
    )
    parser.add_argument("--paper", required=True, help="Paper execution CSV path.")
    parser.add_argument("--backtest", required=True, help="Backtest execution CSV path.")
    parser.add_argument("--output", required=True, help="Diff JSON output path.")
    parser.add_argument("--max-fill-rate-delta", type=float, default=0.05)
    parser.add_argument("--max-rejection-rate-delta", type=float, default=0.05)
    parser.add_argument("--max-avg-slippage-bps-delta", type=float, default=5.0)
    parser.add_argument("--max-total-pnl-delta", type=float, default=1.0)
    parser.add_argument(
        "--min-matched-decisions",
        type=int,
        default=DEFAULT_MIN_MATCHED_DECISIONS,
        help=(
            "Minimum matched decision ids required before the diff can be "
            "final GO."
        ),
    )
    parser.add_argument(
        "--require-pass",
        action="store_true",
        help="Return exit code 1 when the generated diff fails thresholds.",
    )
    return parser


def _require_output_distinct_from_inputs(
    output_path: Path,
    *,
    input_paths: Sequence[Path],
) -> None:
    output_identities = _path_identities(output_path)
    for input_path in input_paths:
        if not _path_identities_overlap(
            output_identities,
            _path_identities(input_path),
        ):
            continue
        msg = (
            "paper/backtest execution diff output path must be distinct "
            f"from input: {output_path}"
        )
        raise ValueError(msg)


def _path_identities(path: Path) -> frozenset[Path]:
    expanded = path.expanduser()
    if not expanded.is_absolute():
        expanded = Path.cwd() / expanded
    return frozenset(
        (
            Path(os.path.abspath(expanded)),
            expanded.resolve(strict=False),
        )
    )


def _path_identities_overlap(left: frozenset[Path], right: frozenset[Path]) -> bool:
    return any(
        _paths_overlap(left_path, right_path)
        for left_path in left
        for right_path in right
    )


def _paths_overlap(left: Path, right: Path) -> bool:
    if left == right:
        return True
    try:
        right.relative_to(left)
    except ValueError:
        pass
    else:
        return True
    try:
        left.relative_to(right)
    except ValueError:
        return False
    return True


if __name__ == "__main__":
    raise SystemExit(main())
