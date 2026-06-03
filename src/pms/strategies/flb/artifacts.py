"""Artifact helpers for H1 FLB launch calibration files."""

from __future__ import annotations

import json
import os
import stat
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from hashlib import sha256
from pathlib import Path

from pms.strategies.flb.source import (
    FlbCalibrationModel,
    require_flb_calibration_source_label,
)


FLB_CALIBRATION_PROVENANCE_SUFFIX = ".provenance.json"
FLB_CALIBRATION_PROVENANCE_ARTIFACT_TYPE = "flb_calibration_provenance"
FLB_CALIBRATION_PROVENANCE_GENERATOR = "scripts/flb_data_feasibility.py"
FLB_CALIBRATION_PROVENANCE_SOURCE = "warehouse-csv"

_LONGSHOT_SIGNAL_NAME = "longshot_yes_overpriced_buy_no"
_FAVORITE_SIGNAL_NAME = "favorite_yes_underpriced_buy_yes"
_HEX_DIGITS = frozenset("0123456789abcdef")


@dataclass(frozen=True, slots=True)
class FlbCalibrationProvenance:
    generated_at: datetime
    warehouse_csv_sha256: str
    warehouse_market_count: int
    warehouse_longshot_count: int
    warehouse_favorite_count: int
    calibration_csv_sha256: str
    calibration_source_label: str


def flb_calibration_provenance_path(calibration_path: str | Path) -> Path:
    """Return the required provenance sidecar path for an FLB calibration CSV."""
    path = Path(calibration_path).expanduser()
    return Path(f"{path}{FLB_CALIBRATION_PROVENANCE_SUFFIX}")


def file_sha256_no_follow(path: str | Path, *, label: str) -> str:
    """Hash a single regular file without following links."""
    return sha256(
        _read_bytes_no_follow(Path(path).expanduser(), label=label)
    ).hexdigest()


def flb_calibration_provenance_payload(
    *,
    generated_at: datetime,
    warehouse_csv_sha256: str,
    warehouse_market_count: int,
    warehouse_longshot_count: int,
    warehouse_favorite_count: int,
    calibration_csv_sha256: str,
    calibration_source_label: str,
) -> dict[str, object]:
    """Build the canonical JSON payload for calibration provenance."""
    return {
        "artifact_type": FLB_CALIBRATION_PROVENANCE_ARTIFACT_TYPE,
        "generated_by": FLB_CALIBRATION_PROVENANCE_GENERATOR,
        "source": FLB_CALIBRATION_PROVENANCE_SOURCE,
        "generated_at": generated_at.isoformat(),
        "warehouse_csv_sha256": warehouse_csv_sha256,
        "warehouse_market_count": warehouse_market_count,
        "warehouse_longshot_count": warehouse_longshot_count,
        "warehouse_favorite_count": warehouse_favorite_count,
        "calibration_csv_sha256": calibration_csv_sha256,
        "calibration_source_label": calibration_source_label,
    }


def load_flb_calibration_provenance_json(
    path: str | Path,
    *,
    calibration_csv_sha256: str,
    source_labels: Sequence[str],
    signal_sample_counts: Mapping[str, int],
    min_sample_count: int,
) -> FlbCalibrationProvenance:
    """Load and validate the provenance JSON sidecar for a calibration CSV."""
    provenance_path = Path(path).expanduser()
    if not provenance_path.exists():
        msg = f"FLB calibration provenance JSON does not exist: {provenance_path}"
        raise ValueError(msg)
    try:
        text = _read_bytes_no_follow(
            provenance_path,
            label="FLB calibration provenance JSON",
        ).decode("utf-8")
    except UnicodeDecodeError as exc:
        msg = f"FLB calibration provenance JSON must be UTF-8 text: {provenance_path}"
        raise ValueError(msg) from exc
    return validate_flb_calibration_provenance_json(
        text,
        calibration_csv_sha256=calibration_csv_sha256,
        source_labels=source_labels,
        signal_sample_counts=signal_sample_counts,
        min_sample_count=min_sample_count,
    )


def require_flb_calibration_provenance_for_model(
    calibration_path: str | Path,
    *,
    model: FlbCalibrationModel,
) -> FlbCalibrationProvenance:
    """Validate the provenance sidecar beside a loaded FLB calibration model."""
    path = Path(calibration_path).expanduser()
    return load_flb_calibration_provenance_json(
        flb_calibration_provenance_path(path),
        calibration_csv_sha256=file_sha256_no_follow(
            path,
            label="FLB calibration artifact",
        ),
        source_labels=tuple(row.source_label for row in model.calibrations),
        signal_sample_counts={
            row.signal_name: row.sample_count for row in model.calibrations
        },
        min_sample_count=model.min_sample_count,
    )


def validate_flb_calibration_provenance_json(
    text: str,
    *,
    calibration_csv_sha256: str,
    source_labels: Sequence[str],
    signal_sample_counts: Mapping[str, int],
    min_sample_count: int,
) -> FlbCalibrationProvenance:
    """Validate an FLB calibration provenance payload against the CSV it binds."""
    label = "FLB calibration provenance JSON invalid"
    payload = _load_json_object(text, label=label)

    artifact_type = _required_text(payload, "artifact_type", label=label)
    if artifact_type != FLB_CALIBRATION_PROVENANCE_ARTIFACT_TYPE:
        msg = (
            f"{label}: artifact_type must be "
            f"{FLB_CALIBRATION_PROVENANCE_ARTIFACT_TYPE}"
        )
        raise ValueError(msg)

    generated_by = _required_text(payload, "generated_by", label=label)
    if generated_by != FLB_CALIBRATION_PROVENANCE_GENERATOR:
        msg = f"{label}: generated_by must be {FLB_CALIBRATION_PROVENANCE_GENERATOR}"
        raise ValueError(msg)

    source = _required_text(payload, "source", label=label)
    if source != FLB_CALIBRATION_PROVENANCE_SOURCE:
        msg = f"{label}: source must be {FLB_CALIBRATION_PROVENANCE_SOURCE}"
        raise ValueError(msg)

    generated_at = _required_datetime(payload, "generated_at", label=label)
    warehouse_csv_sha256 = _required_sha256(
        payload,
        "warehouse_csv_sha256",
        label=label,
    )
    warehouse_market_count = _required_positive_int(
        payload,
        "warehouse_market_count",
        label=label,
    )
    warehouse_longshot_count = _required_positive_int(
        payload,
        "warehouse_longshot_count",
        label=label,
    )
    warehouse_favorite_count = _required_positive_int(
        payload,
        "warehouse_favorite_count",
        label=label,
    )
    observed_calibration_sha256 = _required_sha256(
        payload,
        "calibration_csv_sha256",
        label=label,
    )
    if observed_calibration_sha256 != calibration_csv_sha256:
        msg = f"{label}: calibration_csv_sha256 does not match calibration CSV"
        raise ValueError(msg)

    calibration_source_label = _required_text(
        payload,
        "calibration_source_label",
        label=label,
    )
    try:
        require_flb_calibration_source_label(calibration_source_label)
    except ValueError as exc:
        msg = f"{label}: calibration_source_label invalid: {exc}"
        raise ValueError(msg) from exc

    observed_source_labels = set(source_labels)
    if observed_source_labels != {calibration_source_label}:
        msg = (
            f"{label}: calibration_source_label does not match calibration CSV "
            "source_label values"
        )
        raise ValueError(msg)

    _require_signal_count_match(
        signal_sample_counts,
        signal_name=_LONGSHOT_SIGNAL_NAME,
        provenance_count=warehouse_longshot_count,
        min_sample_count=min_sample_count,
        label=label,
    )
    _require_signal_count_match(
        signal_sample_counts,
        signal_name=_FAVORITE_SIGNAL_NAME,
        provenance_count=warehouse_favorite_count,
        min_sample_count=min_sample_count,
        label=label,
    )
    if warehouse_market_count < warehouse_longshot_count + warehouse_favorite_count:
        msg = (
            f"{label}: warehouse_market_count must cover longshot and favorite "
            "sample counts"
        )
        raise ValueError(msg)

    return FlbCalibrationProvenance(
        generated_at=generated_at,
        warehouse_csv_sha256=warehouse_csv_sha256,
        warehouse_market_count=warehouse_market_count,
        warehouse_longshot_count=warehouse_longshot_count,
        warehouse_favorite_count=warehouse_favorite_count,
        calibration_csv_sha256=observed_calibration_sha256,
        calibration_source_label=calibration_source_label,
    )


def _require_signal_count_match(
    signal_sample_counts: Mapping[str, int],
    *,
    signal_name: str,
    provenance_count: int,
    min_sample_count: int,
    label: str,
) -> None:
    if provenance_count < min_sample_count:
        msg = (
            f"{label}: {signal_name} provenance sample count "
            f"{provenance_count} < {min_sample_count}"
        )
        raise ValueError(msg)
    observed = signal_sample_counts.get(signal_name)
    if observed is None:
        msg = f"{label}: missing calibration CSV sample count for {signal_name}"
        raise ValueError(msg)
    if observed != provenance_count:
        msg = (
            f"{label}: {signal_name} provenance sample count "
            "does not match calibration CSV"
        )
        raise ValueError(msg)


def _load_json_object(text: str, *, label: str) -> Mapping[str, object]:
    def reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                msg = f"{label}: duplicate JSON key: {key}"
                raise ValueError(msg)
            result[key] = value
        return result

    try:
        payload = json.loads(text, object_pairs_hook=reject_duplicate_keys)
    except json.JSONDecodeError as exc:
        msg = f"{label}: malformed JSON"
        raise ValueError(msg) from exc
    if not isinstance(payload, dict):
        msg = f"{label}: expected JSON object"
        raise ValueError(msg)
    return payload


def _required_text(payload: Mapping[str, object], field_name: str, *, label: str) -> str:
    raw_value = payload.get(field_name)
    if not isinstance(raw_value, str) or raw_value.strip() == "":
        msg = f"{label}: missing {field_name}"
        raise ValueError(msg)
    return raw_value.strip()


def _required_datetime(
    payload: Mapping[str, object],
    field_name: str,
    *,
    label: str,
) -> datetime:
    value = _required_text(payload, field_name, label=label)
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        msg = f"{label}: invalid {field_name}"
        raise ValueError(msg) from exc


def _required_sha256(
    payload: Mapping[str, object],
    field_name: str,
    *,
    label: str,
) -> str:
    value = _required_text(payload, field_name, label=label)
    if len(value) != 64 or not all(character in _HEX_DIGITS for character in value):
        msg = f"{label}: {field_name} must be lowercase sha256 hex"
        raise ValueError(msg)
    if len(set(value)) == 1:
        msg = f"{label}: {field_name} must not be a placeholder hash"
        raise ValueError(msg)
    return value


def _required_positive_int(
    payload: Mapping[str, object],
    field_name: str,
    *,
    label: str,
) -> int:
    raw_value = payload.get(field_name)
    if isinstance(raw_value, bool) or not isinstance(raw_value, int):
        msg = f"{label}: {field_name} must be a positive integer"
        raise ValueError(msg)
    if raw_value <= 0:
        msg = f"{label}: {field_name} must be a positive integer"
        raise ValueError(msg)
    return raw_value


def _read_bytes_no_follow(path: Path, *, label: str) -> bytes:
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    fd = -1
    try:
        fd = os.open(path, flags, 0o777)
        path_stat = os.fstat(fd)
        if not stat.S_ISREG(path_stat.st_mode):
            raise OSError(f"{label} cannot be read safely: {path}")
        if path_stat.st_nlink != 1:
            raise OSError(f"{label} cannot be read safely: {path}")
        with os.fdopen(fd, "rb") as file:
            fd = -1
            return file.read()
    except OSError as exc:
        msg = f"{label} cannot be read safely: {path}"
        raise ValueError(msg) from exc
    finally:
        if fd >= 0:
            os.close(fd)
