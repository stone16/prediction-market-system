"""Build a research execution-model artifact from paper/live telemetry CSV.

The output JSON is the `execution_model` object shape accepted inside research
backtest specs.  It is intentionally generated through
`ExecutionModel.from_observed_telemetry(...)` so promotion artifacts use the
same validation and percentile rules as the simulator domain model.
"""

from __future__ import annotations

import csv
import io
import json
import os
import stat
import sys
from argparse import ArgumentParser
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import cast
from uuid import uuid4

from pms.research.specs import ExecutionModel, FillPolicy, SUPPORTED_FILL_POLICIES


GENERATED_BY = "scripts/execution_model_from_telemetry.py"
ARTIFACT_MODE = "telemetry_execution_model"
REQUIRED_TELEMETRY_COLUMNS = frozenset({
    "slippage_bps",
    "latency_ms",
})
TELEMETRY_STRATEGY_COLUMNS = frozenset({
    "strategy_id",
    "strategy_version_id",
})


@dataclass(frozen=True)
class ExecutionModelTelemetryEvidence:
    min_samples: int
    telemetry_sample_count: int
    adverse_selection_sample_count: int
    require_adverse_selection: bool


def build_execution_model_from_telemetry_csv(
    path: Path,
    *,
    fee_rate: float,
    staleness_ms: float,
    displayed_depth_fill_ratio: float = 1.0,
    require_adverse_selection: bool = False,
    min_samples: int = 1,
    fill_policy: FillPolicy = "immediate_or_cancel",
    order_ttl_ms: int = 60_000,
    price_invalidation_streak: int = 10,
    replay_window_ms: int = 86_400_000,
    strategy_id: str | None = None,
    strategy_version_id: str | None = None,
) -> ExecutionModel:
    model, _ = build_execution_model_artifact_from_telemetry_csv(
        path,
        fee_rate=fee_rate,
        staleness_ms=staleness_ms,
        displayed_depth_fill_ratio=displayed_depth_fill_ratio,
        require_adverse_selection=require_adverse_selection,
        min_samples=min_samples,
        fill_policy=fill_policy,
        order_ttl_ms=order_ttl_ms,
        price_invalidation_streak=price_invalidation_streak,
        replay_window_ms=replay_window_ms,
        strategy_id=strategy_id,
        strategy_version_id=strategy_version_id,
    )
    return model


def build_execution_model_artifact_from_telemetry_csv(
    path: Path,
    *,
    fee_rate: float,
    staleness_ms: float,
    displayed_depth_fill_ratio: float = 1.0,
    require_adverse_selection: bool = False,
    min_samples: int = 1,
    fill_policy: FillPolicy = "immediate_or_cancel",
    order_ttl_ms: int = 60_000,
    price_invalidation_streak: int = 10,
    replay_window_ms: int = 86_400_000,
    strategy_id: str | None = None,
    strategy_version_id: str | None = None,
) -> tuple[ExecutionModel, ExecutionModelTelemetryEvidence]:
    if min_samples <= 0:
        msg = "min_samples must be an integer > 0"
        raise ValueError(msg)
    strategy_scope = _strategy_scope(
        strategy_id=strategy_id,
        strategy_version_id=strategy_version_id,
    )

    slippage_bps_samples: list[float] = []
    latency_ms_samples: list[float] = []
    adverse_selection_bps_samples: list[float] = []

    with io.StringIO(_read_text_no_follow(path), newline="") as f:
        reader = csv.DictReader(f)
        _require_unique_csv_fieldnames(reader.fieldnames)
        fieldnames = set(reader.fieldnames or ())
        missing = REQUIRED_TELEMETRY_COLUMNS - fieldnames
        if missing:
            missing_display = ", ".join(sorted(missing))
            raise ValueError(
                f"execution telemetry CSV missing required columns: {missing_display}"
            )
        if strategy_scope is not None:
            missing_strategy_columns = TELEMETRY_STRATEGY_COLUMNS - fieldnames
            if missing_strategy_columns:
                missing_display = ", ".join(sorted(missing_strategy_columns))
                raise ValueError(
                    "execution telemetry CSV missing required strategy columns: "
                    f"{missing_display}"
                )

        for row_number, row in enumerate(reader, start=2):
            if strategy_scope is not None:
                _require_telemetry_strategy_scope(
                    row,
                    row_number=row_number,
                    strategy_scope=strategy_scope,
                )
            slippage_bps_samples.append(
                _required_float(row, "slippage_bps", row_number=row_number)
            )
            latency_ms_samples.append(
                _required_float(row, "latency_ms", row_number=row_number)
            )
            raw_adverse_selection = row.get("adverse_selection_bps")
            if raw_adverse_selection is not None and raw_adverse_selection.strip():
                adverse_selection_bps_samples.append(
                    _required_float(
                        row,
                        "adverse_selection_bps",
                        row_number=row_number,
                    )
                )

    if len(slippage_bps_samples) < min_samples or len(latency_ms_samples) < min_samples:
        found = min(len(slippage_bps_samples), len(latency_ms_samples))
        msg = (
            f"execution telemetry CSV must contain at least {min_samples} "
            f"telemetry samples; found {found}"
        )
        raise ValueError(msg)
    if require_adverse_selection and not adverse_selection_bps_samples:
        msg = (
            "execution telemetry CSV must include adverse_selection_bps samples "
            "when require_adverse_selection=True"
        )
        raise ValueError(msg)
    if require_adverse_selection and len(adverse_selection_bps_samples) < min_samples:
        msg = (
            "execution telemetry CSV must include at least "
            f"{min_samples} adverse_selection_bps samples; "
            f"found {len(adverse_selection_bps_samples)}"
        )
        raise ValueError(msg)

    model = ExecutionModel.from_observed_telemetry(
        fee_rate=fee_rate,
        slippage_bps_samples=tuple(slippage_bps_samples),
        latency_ms_samples=tuple(latency_ms_samples),
        staleness_ms=staleness_ms,
        displayed_depth_fill_ratio=displayed_depth_fill_ratio,
        adverse_selection_bps_samples=tuple(adverse_selection_bps_samples),
        fill_policy=fill_policy,
        order_ttl_ms=order_ttl_ms,
        price_invalidation_streak=price_invalidation_streak,
        replay_window_ms=replay_window_ms,
    )
    evidence = ExecutionModelTelemetryEvidence(
        min_samples=min_samples,
        telemetry_sample_count=min(len(slippage_bps_samples), len(latency_ms_samples)),
        adverse_selection_sample_count=len(adverse_selection_bps_samples),
        require_adverse_selection=require_adverse_selection,
    )
    return model, evidence


def save_execution_model_json(
    model: ExecutionModel,
    path: Path,
    *,
    telemetry_evidence: ExecutionModelTelemetryEvidence | None = None,
    strategy_evidence: str | None = None,
) -> None:
    _prepare_private_output_parent(path)
    _write_text_no_follow(
        path,
        json.dumps(
            execution_model_to_json_dict(
                model,
                telemetry_evidence=telemetry_evidence,
                strategy_evidence=strategy_evidence,
            ),
            allow_nan=False,
            indent=2,
            sort_keys=True,
        )
        + "\n",
    )


def execution_model_to_json_dict(
    model: ExecutionModel,
    *,
    telemetry_evidence: ExecutionModelTelemetryEvidence | None = None,
    strategy_evidence: str | None = None,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "generated_by": GENERATED_BY,
        "artifact_mode": ARTIFACT_MODE,
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "fee_rate": model.fee_rate,
        "slippage_bps": model.slippage_bps,
        "latency_ms": model.latency_ms,
        "staleness_ms": model.staleness_ms,
        "fill_policy": model.fill_policy,
        "displayed_depth_fill_ratio": model.displayed_depth_fill_ratio,
        "adverse_selection_bps": model.adverse_selection_bps,
        "order_ttl_ms": model.order_ttl_ms,
        "price_invalidation_streak": model.price_invalidation_streak,
        "replay_window_ms": model.replay_window_ms,
        "calibration_source": model.calibration_source,
    }
    if telemetry_evidence is not None:
        payload.update(
            {
                "min_samples": telemetry_evidence.min_samples,
                "telemetry_sample_count": telemetry_evidence.telemetry_sample_count,
                "adverse_selection_sample_count": (
                    telemetry_evidence.adverse_selection_sample_count
                ),
                "require_adverse_selection": (
                    telemetry_evidence.require_adverse_selection
                ),
            }
        )
    if strategy_evidence is not None:
        payload["strategy_evidence"] = _strategy_evidence_value(strategy_evidence)
    return payload


def _strategy_scope_evidence(
    *,
    strategy_id: str | None,
    strategy_version_id: str | None,
) -> str | None:
    strategy_scope = _strategy_scope(
        strategy_id=strategy_id,
        strategy_version_id=strategy_version_id,
    )
    if strategy_scope is None:
        return None
    scoped_strategy_id, scoped_strategy_version_id = strategy_scope
    return f"{scoped_strategy_id}@{scoped_strategy_version_id}"


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


def _require_telemetry_strategy_scope(
    row: dict[str, str | None],
    *,
    row_number: int,
    strategy_scope: tuple[str, str],
) -> None:
    strategy_id, strategy_version_id = strategy_scope
    observed_strategy_id = _required_text(row, "strategy_id", row_number=row_number)
    if observed_strategy_id != strategy_id:
        msg = (
            f"execution telemetry row {row_number}: strategy_id must match "
            f"{strategy_id}"
        )
        raise ValueError(msg)
    observed_strategy_version_id = _required_text(
        row,
        "strategy_version_id",
        row_number=row_number,
    )
    if observed_strategy_version_id != strategy_version_id:
        msg = (
            f"execution telemetry row {row_number}: strategy_version_id must match "
            f"{strategy_version_id}"
        )
        raise ValueError(msg)


def _strategy_identity_value(value: str, field_name: str) -> str:
    stripped = value.strip()
    if stripped == "":
        raise ValueError(f"{field_name} must not be empty")
    if "," in stripped or "@" in stripped:
        raise ValueError(f"{field_name} must not contain ',' or '@'")
    return stripped


def _strategy_evidence_value(value: str) -> str:
    stripped = value.strip()
    if stripped == "":
        raise ValueError("strategy_evidence must not be empty")
    labels = tuple(label.strip() for label in stripped.split(",") if label.strip())
    if not labels:
        raise ValueError("strategy_evidence must not be empty")
    if len(set(labels)) != len(labels):
        raise ValueError("strategy_evidence must not contain duplicate labels")
    for label in labels:
        if label.lower() == "unknown" or "@" not in label:
            raise ValueError(
                "strategy_evidence must contain concrete "
                "strategy_id@strategy_version_id labels"
            )
    return ", ".join(labels)


def _required_text(
    row: dict[str, str | None],
    column: str,
    *,
    row_number: int,
) -> str:
    raw_value = row.get(column)
    if raw_value is None or not raw_value.strip():
        raise ValueError(f"execution telemetry row {row_number}: missing {column}")
    return raw_value.strip()


def _required_float(
    row: dict[str, str | None],
    column: str,
    *,
    row_number: int,
) -> float:
    raw_value = _required_text(row, column, row_number=row_number)
    try:
        return float(raw_value)
    except ValueError as exc:
        raise ValueError(
            f"execution telemetry row {row_number}: {column} must be numeric"
        ) from exc


def _read_text_no_follow(path: Path) -> str:
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    fd = -1
    try:
        fd = os.open(path, flags, 0o777)
        path_stat = os.fstat(fd)
        if not stat.S_ISREG(path_stat.st_mode):
            raise OSError(
                f"execution telemetry CSV cannot be read safely: {path}"
            )
        if path_stat.st_nlink != 1:
            raise OSError(
                f"execution telemetry CSV cannot be read safely: {path}"
            )
        with os.fdopen(fd, "r", encoding="utf-8") as file:
            fd = -1
            return file.read()
    except OSError as exc:
        msg = f"execution telemetry CSV cannot be read safely: {path}"
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
    raise FileExistsError(f"could not create temporary execution model for {path}")


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
        raise OSError(f"execution model output parent is not a directory: {parent}")
    permissions = stat.S_IMODE(mode)
    if permissions & 0o077:
        raise OSError(
            f"execution model output parent {parent} is too permissive; "
            f"run `chmod 700 {parent}`."
        )
    if not permissions & stat.S_IWUSR:
        raise OSError(
            f"execution model output parent {parent} is not owner-writable; "
            f"run `chmod 700 {parent}`."
        )


def _require_open_regular_single_link_file(fd: int, path: Path) -> None:
    path_stat = os.fstat(fd)
    if not stat.S_ISREG(path_stat.st_mode):
        raise OSError(f"execution model output path is not a regular file: {path}")
    if path_stat.st_nlink != 1:
        raise OSError(f"execution model output path is not a single-link file: {path}")


def _require_regular_file_or_absent(path: Path) -> None:
    try:
        path_stat = path.lstat()
    except FileNotFoundError:
        return
    if not stat.S_ISREG(path_stat.st_mode):
        raise OSError(f"execution model output path is not a regular file: {path}")
    if path_stat.st_nlink != 1:
        raise OSError(f"execution model output path is not a single-link file: {path}")


def main(argv: Sequence[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    try:
        input_path = Path(args.input)
        output_path = Path(args.output)
        _require_output_distinct_from_inputs(
            output_path,
            input_paths=(input_path,),
        )
        model, telemetry_evidence = build_execution_model_artifact_from_telemetry_csv(
            input_path,
            fee_rate=args.fee_rate,
            staleness_ms=args.staleness_ms,
            displayed_depth_fill_ratio=args.displayed_depth_fill_ratio,
            require_adverse_selection=args.require_adverse_selection,
            min_samples=args.min_samples,
            fill_policy=cast(FillPolicy, args.fill_policy),
            order_ttl_ms=args.order_ttl_ms,
            price_invalidation_streak=args.price_invalidation_streak,
            replay_window_ms=args.replay_window_ms,
            strategy_id=cast(str | None, args.strategy_id),
            strategy_version_id=cast(str | None, args.strategy_version_id),
        )
        strategy_evidence = _strategy_scope_evidence(
            strategy_id=cast(str | None, args.strategy_id),
            strategy_version_id=cast(str | None, args.strategy_version_id),
        )
        save_execution_model_json(
            model,
            output_path,
            telemetry_evidence=telemetry_evidence,
            strategy_evidence=strategy_evidence,
        )
    except (OSError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    print(f"Execution model JSON written to {args.output}", file=sys.stderr)
    return 0


def _parser() -> ArgumentParser:
    parser = ArgumentParser(
        description=(
            "Build a telemetry_calibrated research execution_model JSON object "
            "from strict paper/live execution telemetry CSV."
        )
    )
    parser.add_argument("--input", required=True, help="Telemetry CSV input path.")
    parser.add_argument(
        "--output",
        required=True,
        help="Execution model JSON artifact output path.",
    )
    parser.add_argument(
        "--fee-rate",
        type=float,
        required=True,
        help="Venue fee rate as a unit fraction, e.g. 0.04.",
    )
    parser.add_argument(
        "--staleness-ms",
        type=float,
        required=True,
        help="Market-data staleness ceiling in milliseconds.",
    )
    parser.add_argument(
        "--displayed-depth-fill-ratio",
        type=float,
        default=1.0,
        help="Reachable fraction of displayed book depth after queue-position haircut.",
    )
    parser.add_argument(
        "--require-adverse-selection",
        action="store_true",
        help="Fail unless adverse_selection_bps samples are present.",
    )
    parser.add_argument(
        "--min-samples",
        type=int,
        default=1,
        help="Minimum sample count required for calibrated telemetry fields.",
    )
    parser.add_argument(
        "--fill-policy",
        default="immediate_or_cancel",
        choices=sorted(SUPPORTED_FILL_POLICIES),
        help="Backtest fill policy encoded in the execution model.",
    )
    parser.add_argument(
        "--order-ttl-ms",
        type=int,
        default=60_000,
        help="Open-order TTL for policies that can leave orders resting.",
    )
    parser.add_argument(
        "--price-invalidation-streak",
        type=int,
        default=10,
        help="Consecutive invalid replay prices before cancelling a resting order.",
    )
    parser.add_argument(
        "--replay-window-ms",
        type=int,
        default=86_400_000,
        help="Maximum replay lookup window in milliseconds.",
    )
    parser.add_argument(
        "--strategy-id",
        help="Optional strategy_id whose telemetry calibrated this artifact.",
    )
    parser.add_argument(
        "--strategy-version-id",
        help=(
            "Optional strategy_version_id whose telemetry calibrated this "
            "artifact. Must be provided with --strategy-id."
        ),
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
        msg = f"execution model output path must be distinct from input: {output_path}"
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
