from __future__ import annotations

import csv
import math
import json
import os
import stat
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any

from pms.config import PMSSettings, normalize_webhook_url
from pms.core.models import LiveTradingDisabledError
from pms.research.spec_codec import deserialize_execution_model


_LIVE_PAPER_BACKTEST_DIFF_GENERATED_BY = "scripts/paper_backtest_execution_diff.py"
_LIVE_PAPER_BACKTEST_DIFF_ARTIFACT_MODE = "paper_backtest_execution_diff"
_LIVE_EXECUTION_MODEL_GENERATED_BY = "scripts/execution_model_from_telemetry.py"
_LIVE_EXECUTION_MODEL_ARTIFACT_MODE = "telemetry_execution_model"
_MIN_LIVE_EXECUTION_MODEL_TELEMETRY_SAMPLES = 10
_REQUIRED_LIVE_PAPER_BACKTEST_DIFF_METRICS: tuple[str, ...] = (
    "paper_decision_count",
    "backtest_decision_count",
    "matched_decision_count",
    "fill_rate_delta_abs",
    "rejection_rate_delta_abs",
    "avg_slippage_bps_delta_abs",
    "total_pnl_delta_abs",
)
_LIVE_PAPER_BACKTEST_DIFF_COUNT_METRICS: tuple[str, ...] = (
    "paper_decision_count",
    "backtest_decision_count",
    "matched_decision_count",
)
_MIN_LIVE_PAPER_BACKTEST_DIFF_MATCHED_DECISIONS = 10
_LIVE_PAPER_BACKTEST_DIFF_RATE_DELTA_METRICS: tuple[str, ...] = (
    "fill_rate_delta_abs",
    "rejection_rate_delta_abs",
)
_LIVE_PAPER_BACKTEST_DIFF_THRESHOLD_METRICS: tuple[tuple[str, str], ...] = (
    ("fill_rate_delta_abs", "max_fill_rate_delta"),
    ("rejection_rate_delta_abs", "max_rejection_rate_delta"),
    ("avg_slippage_bps_delta_abs", "max_avg_slippage_bps_delta"),
    ("total_pnl_delta_abs", "max_total_pnl_delta"),
)
_LIVE_PAPER_BACKTEST_DIFF_MIN_MATCHED_DECISIONS_THRESHOLD = "min_matched_decisions"
_LIVE_PAPER_BACKTEST_DIFF_EMPTY_LIST_FIELDS: tuple[tuple[str, str], ...] = (
    ("paper_only_decision_ids", "paper-only decisions"),
    ("backtest_only_decision_ids", "backtest-only decisions"),
    ("status_mismatches", "status mismatches"),
)
_REQUIRED_CATEGORY_PRIOR_COLUMNS = frozenset(
    {"market_id", "category", "yes_payout", "no_payout", "resolved_at"}
)
_REQUIRED_FLB_CALIBRATION_COLUMNS = frozenset(
    {"signal_name", "probability_estimate", "sample_count", "source_label"}
)
_REQUIRED_FLB_SIGNALS = frozenset(
    {
        "longshot_yes_overpriced_buy_no",
        "favorite_yes_underpriced_buy_yes",
    }
)
_FLB_CALIBRATION_SOURCE_LABEL_MAX_LENGTH = 80
_FLB_CALIBRATION_SOURCE_LABEL_FORBIDDEN_PARTS = frozenset(
    {
        "dummy",
        "example",
        "fixture",
        "gamma",
        "placeholder",
        "sample",
        "test",
        "todo",
    }
)


def live_preflight_settings_fingerprint(settings: PMSSettings) -> str:
    payload: dict[str, Any] = settings.model_dump(mode="json")
    payload["live_preflight_artifact_path"] = (
        "<validated separately by artifact output_path>"
    )
    payload["database"]["dsn"] = _optional_sha256(settings.database.dsn)
    payload["polymarket"]["private_key"] = _optional_sha256(
        settings.polymarket.private_key
    )
    payload["polymarket"]["api_key"] = _optional_sha256(settings.polymarket.api_key)
    payload["polymarket"]["api_secret"] = _optional_sha256(
        settings.polymarket.api_secret
    )
    payload["polymarket"]["api_passphrase"] = _optional_sha256(
        settings.polymarket.api_passphrase
    )
    payload["llm"]["api_key"] = _optional_sha256(settings.llm.api_key)
    payload["discord"]["webhook_url"] = _optional_sha256(
        normalize_webhook_url(settings.discord.webhook_url)
    )
    return canonical_sha256(payload)


def loads_json_rejecting_duplicate_keys(text: str, *, label: str) -> object:
    def reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
        seen: set[str] = set()
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in seen:
                msg = f"{label} duplicate JSON key: {key}"
                raise LiveTradingDisabledError(msg)
            seen.add(key)
            result[key] = value
        return result

    return json.loads(text, object_pairs_hook=reject_duplicate_keys)


def live_preflight_readiness_reports_fingerprint(settings: PMSSettings) -> str:
    payload: dict[str, object] = {
        "paper_soak_report": _readiness_report_fingerprint_payload(
            settings.live_paper_soak_report_path,
            label="LIVE paper soak GO report",
        ),
        "operator_rehearsal_report": _readiness_report_fingerprint_payload(
            settings.live_operator_rehearsal_report_path,
            label="LIVE operator rehearsal report",
        ),
        "execution_model": _readiness_report_fingerprint_payload(
            settings.live_execution_model_path,
            label="LIVE execution-model artifact",
        ),
        "paper_backtest_execution_diff": _readiness_report_fingerprint_payload(
            settings.live_paper_backtest_diff_path,
            label="LIVE paper-vs-backtest execution diff artifact",
        ),
        "category_prior": _readiness_report_fingerprint_payload(
            settings.controller.category_prior_observations_path,
            label="LIVE category-prior artifact",
        ),
        "flb_calibration": _readiness_report_fingerprint_payload(
            settings.strategies.flb_calibration_path,
            label="LIVE FLB calibration artifact",
        ),
    }
    return canonical_sha256(payload)


def validate_live_strategy_artifacts_for_submission(settings: PMSSettings) -> None:
    _validate_live_execution_model_artifact(settings)
    _validate_live_paper_backtest_diff_artifact(settings)
    _validate_live_category_prior_artifact(settings)
    _validate_live_flb_calibration_artifact(settings)


def live_preflight_readiness_report_generated_at_values(
    settings: PMSSettings,
) -> tuple[tuple[str, datetime], ...]:
    return (
        (
            "live_paper_soak_report_path",
            _readiness_report_generated_at(
                settings.live_paper_soak_report_path,
                label="LIVE paper soak GO report",
            ),
        ),
        (
            "live_operator_rehearsal_report_path",
            _readiness_report_generated_at(
                settings.live_operator_rehearsal_report_path,
                label="LIVE operator rehearsal report",
            ),
        ),
        (
            "live_execution_model_path",
            _json_artifact_generated_at(
                settings.live_execution_model_path,
                label="LIVE execution-model artifact",
            ),
        ),
        (
            "live_paper_backtest_diff_path",
            _json_artifact_generated_at(
                settings.live_paper_backtest_diff_path,
                label="LIVE paper-vs-backtest execution diff artifact",
            ),
        ),
    )


def _validate_live_execution_model_artifact(settings: PMSSettings) -> None:
    raw_path = settings.live_execution_model_path
    if raw_path is None or raw_path.strip() == "":
        msg = (
            "LIVE execution-model artifact path is required: "
            "live_execution_model_path"
        )
        raise LiveTradingDisabledError(msg)
    if _looks_like_artifact_placeholder(raw_path):
        msg = "LIVE execution-model artifact path contains placeholder"
        raise LiveTradingDisabledError(msg)
    path = _require_live_strategy_artifact_path(
        raw_path,
        label="LIVE execution-model artifact",
    )
    try:
        payload = loads_json_rejecting_duplicate_keys(
            _read_bytes_no_follow(path).decode("utf-8"),
            label="LIVE execution-model artifact",
        )
    except json.JSONDecodeError as exc:
        msg = "LIVE execution-model artifact must be valid JSON"
        raise LiveTradingDisabledError(msg) from exc
    except OSError as exc:
        msg = f"LIVE execution-model artifact is unreadable: {path}"
        raise LiveTradingDisabledError(msg) from exc
    if not isinstance(payload, dict):
        msg = "LIVE execution-model artifact must be a JSON object"
        raise LiveTradingDisabledError(msg)
    if payload.get("generated_by") != _LIVE_EXECUTION_MODEL_GENERATED_BY:
        msg = "LIVE execution-model artifact generated_by is invalid"
        raise LiveTradingDisabledError(msg)
    if payload.get("artifact_mode") != _LIVE_EXECUTION_MODEL_ARTIFACT_MODE:
        msg = "LIVE execution-model artifact artifact_mode is invalid"
        raise LiveTradingDisabledError(msg)
    _require_json_artifact_generated_at(
        payload,
        label="LIVE execution-model artifact",
        max_age_s=settings.live_readiness_report_max_age_s,
    )
    try:
        execution_model = deserialize_execution_model(payload)
    except (KeyError, TypeError, ValueError) as exc:
        msg = f"LIVE execution-model artifact invalid: {exc}"
        raise LiveTradingDisabledError(msg) from exc
    if execution_model.calibration_source != "telemetry_calibrated":
        msg = "LIVE execution-model artifact must be telemetry_calibrated"
        raise LiveTradingDisabledError(msg)
    if not math.isfinite(execution_model.staleness_ms):
        msg = "LIVE execution-model artifact staleness_ms must be finite"
        raise LiveTradingDisabledError(msg)
    if execution_model.adverse_selection_bps <= 0.0:
        msg = (
            "LIVE execution-model artifact must include positive "
            "adverse_selection_bps"
        )
        raise LiveTradingDisabledError(msg)
    _require_live_execution_model_telemetry_sample_contract(payload)


def _require_live_execution_model_telemetry_sample_contract(
    payload: Mapping[str, object],
) -> None:
    required_fields = (
        "min_samples",
        "telemetry_sample_count",
        "adverse_selection_sample_count",
        "require_adverse_selection",
    )
    missing = [field for field in required_fields if field not in payload]
    if missing:
        msg = (
            "LIVE execution-model artifact missing telemetry sample contract: "
            f"{', '.join(missing)}"
        )
        raise LiveTradingDisabledError(msg)
    if payload["require_adverse_selection"] is not True:
        msg = "LIVE execution-model artifact require_adverse_selection must be true"
        raise LiveTradingDisabledError(msg)
    min_samples = _require_live_execution_model_positive_integer(
        payload,
        field_name="min_samples",
    )
    if min_samples < _MIN_LIVE_EXECUTION_MODEL_TELEMETRY_SAMPLES:
        msg = (
            "LIVE execution-model artifact min_samples must be at least "
            f"{_MIN_LIVE_EXECUTION_MODEL_TELEMETRY_SAMPLES}"
        )
        raise LiveTradingDisabledError(msg)
    telemetry_sample_count = _require_live_execution_model_positive_integer(
        payload,
        field_name="telemetry_sample_count",
    )
    if telemetry_sample_count < min_samples:
        msg = (
            "LIVE execution-model artifact telemetry_sample_count must be at least "
            "min_samples"
        )
        raise LiveTradingDisabledError(msg)
    adverse_selection_sample_count = _require_live_execution_model_positive_integer(
        payload,
        field_name="adverse_selection_sample_count",
    )
    if adverse_selection_sample_count < min_samples:
        msg = (
            "LIVE execution-model artifact adverse_selection_sample_count must be "
            "at least min_samples"
        )
        raise LiveTradingDisabledError(msg)


def _require_live_execution_model_positive_integer(
    payload: Mapping[str, object],
    *,
    field_name: str,
) -> int:
    raw_value = payload[field_name]
    if isinstance(raw_value, bool) or not isinstance(raw_value, int | float):
        msg = (
            "LIVE execution-model artifact telemetry sample contract field "
            f"{field_name} must be a finite positive integer"
        )
        raise LiveTradingDisabledError(msg)
    value = float(raw_value)
    if not math.isfinite(value) or value <= 0.0 or not value.is_integer():
        msg = (
            "LIVE execution-model artifact telemetry sample contract field "
            f"{field_name} must be a finite positive integer"
        )
        raise LiveTradingDisabledError(msg)
    return int(value)


def _validate_live_paper_backtest_diff_artifact(settings: PMSSettings) -> None:
    raw_path = settings.live_paper_backtest_diff_path
    if raw_path is None or raw_path.strip() == "":
        msg = (
            "LIVE paper-vs-backtest execution diff artifact path is required: "
            "live_paper_backtest_diff_path"
        )
        raise LiveTradingDisabledError(msg)
    if _looks_like_artifact_placeholder(raw_path):
        msg = "LIVE paper-vs-backtest execution diff artifact path contains placeholder"
        raise LiveTradingDisabledError(msg)
    path = _require_live_strategy_artifact_path(
        raw_path,
        label="LIVE paper-vs-backtest execution diff artifact",
    )
    try:
        payload = loads_json_rejecting_duplicate_keys(
            _read_bytes_no_follow(path).decode("utf-8"),
            label="LIVE paper-vs-backtest execution diff artifact",
        )
    except json.JSONDecodeError as exc:
        msg = "LIVE paper-vs-backtest execution diff artifact must be valid JSON"
        raise LiveTradingDisabledError(msg) from exc
    except OSError as exc:
        msg = (
            "LIVE paper-vs-backtest execution diff artifact is unreadable: "
            f"{path}"
        )
        raise LiveTradingDisabledError(msg) from exc
    if not isinstance(payload, dict):
        msg = "LIVE paper-vs-backtest execution diff artifact must be a JSON object"
        raise LiveTradingDisabledError(msg)
    if payload.get("generated_by") != _LIVE_PAPER_BACKTEST_DIFF_GENERATED_BY:
        msg = (
            "LIVE paper-vs-backtest execution diff artifact generated_by "
            "is invalid"
        )
        raise LiveTradingDisabledError(msg)
    if payload.get("artifact_mode") != _LIVE_PAPER_BACKTEST_DIFF_ARTIFACT_MODE:
        msg = (
            "LIVE paper-vs-backtest execution diff artifact artifact_mode "
            "is invalid"
        )
        raise LiveTradingDisabledError(msg)
    _require_json_artifact_generated_at(
        payload,
        label="LIVE paper-vs-backtest execution diff artifact",
        max_age_s=settings.live_readiness_report_max_age_s,
    )
    if payload.get("final_go_no_go_valid") is not True:
        msg = "LIVE paper-vs-backtest execution diff artifact must be final GO"
        raise LiveTradingDisabledError(msg)
    failures = payload.get("failures")
    if not isinstance(failures, list):
        msg = "LIVE paper-vs-backtest execution diff artifact failures must be a list"
        raise LiveTradingDisabledError(msg)
    if failures:
        msg = "LIVE paper-vs-backtest execution diff artifact must not contain failures"
        raise LiveTradingDisabledError(msg)
    _require_paper_backtest_diff_empty_lists(payload)
    metrics = payload.get("metrics")
    if not isinstance(metrics, dict):
        msg = "LIVE paper-vs-backtest execution diff artifact must include metrics"
        raise LiveTradingDisabledError(msg)
    thresholds = payload.get("thresholds")
    if not isinstance(thresholds, dict):
        msg = "LIVE paper-vs-backtest execution diff artifact must include thresholds"
        raise LiveTradingDisabledError(msg)
    missing_metrics = [
        metric
        for metric in _REQUIRED_LIVE_PAPER_BACKTEST_DIFF_METRICS
        if metric not in metrics
    ]
    if missing_metrics:
        msg = (
            "LIVE paper-vs-backtest execution diff artifact missing metrics: "
            f"{', '.join(missing_metrics)}"
        )
        raise LiveTradingDisabledError(msg)

    metric_values = {
        metric: _require_paper_backtest_diff_metric_number(metrics, metric_name=metric)
        for metric in _REQUIRED_LIVE_PAPER_BACKTEST_DIFF_METRICS
    }
    for metric_name in _LIVE_PAPER_BACKTEST_DIFF_COUNT_METRICS:
        metric_value = metric_values[metric_name]
        if metric_value <= 0.0 or not metric_value.is_integer():
            msg = (
                "LIVE paper-vs-backtest execution diff artifact metric "
                f"{metric_name} must be a positive integer count"
            )
            raise LiveTradingDisabledError(msg)
    for metric_name, metric_value in metric_values.items():
        if metric_name not in _LIVE_PAPER_BACKTEST_DIFF_COUNT_METRICS:
            if metric_value < 0.0:
                msg = (
                    "LIVE paper-vs-backtest execution diff artifact metric "
                    f"{metric_name} must be nonnegative"
                )
                raise LiveTradingDisabledError(msg)
    min_matched_decisions = (
        _require_paper_backtest_diff_min_matched_decisions_threshold(thresholds)
    )
    _require_paper_backtest_diff_metric_consistency(
        metric_values,
        min_matched_decisions=min_matched_decisions,
    )
    _require_paper_backtest_diff_thresholds(metric_values, thresholds=thresholds)


def _validate_live_category_prior_artifact(settings: PMSSettings) -> None:
    raw_path = settings.controller.category_prior_observations_path
    if raw_path is None or raw_path.strip() == "":
        msg = (
            "LIVE category-prior artifact path is required: "
            "controller.category_prior_observations_path"
        )
        raise LiveTradingDisabledError(msg)
    if _looks_like_artifact_placeholder(raw_path):
        msg = (
            "LIVE category-prior artifact path contains placeholder: "
            "controller.category_prior_observations_path"
        )
        raise LiveTradingDisabledError(msg)
    path = _require_live_strategy_artifact_path(
        raw_path,
        label="LIVE category-prior artifact",
    )
    text = _read_strategy_artifact_text(path, label="LIVE category-prior artifact")
    observation_count = _count_category_prior_observations(text)
    minimum = settings.controller.category_prior_min_global_samples
    if observation_count < minimum:
        msg = (
            "LIVE category-prior artifact has too few observations: "
            f"{observation_count} < "
            "controller.category_prior_min_global_samples="
            f"{minimum}"
        )
        raise LiveTradingDisabledError(msg)


def _validate_live_flb_calibration_artifact(settings: PMSSettings) -> None:
    raw_path = settings.strategies.flb_calibration_path
    if raw_path is None or raw_path.strip() == "":
        msg = (
            "LIVE FLB calibration artifact path is required: "
            "strategies.flb_calibration_path"
        )
        raise LiveTradingDisabledError(msg)
    if _looks_like_artifact_placeholder(raw_path):
        msg = (
            "LIVE FLB calibration artifact path contains placeholder: "
            "strategies.flb_calibration_path"
        )
        raise LiveTradingDisabledError(msg)
    path = _require_live_strategy_artifact_path(
        raw_path,
        label="LIVE FLB calibration artifact",
    )
    text = _read_strategy_artifact_text(path, label="LIVE FLB calibration artifact")
    _validate_flb_calibration_rows(
        text,
        min_sample_count=settings.strategies.flb_min_calibration_samples,
    )


def _require_live_strategy_artifact_path(raw_path: str, *, label: str) -> Path:
    path = Path(raw_path).expanduser()
    _require_readiness_report_outside_working_tree(path, label=label)
    _require_readiness_report_parent_owner_writable(path, label=label)
    _require_readiness_report_regular_file_for_fingerprint(path, label=label)
    return path


def _read_strategy_artifact_text(path: Path, *, label: str) -> str:
    try:
        return _read_bytes_no_follow(path).decode("utf-8")
    except UnicodeDecodeError as exc:
        msg = f"{label} must be UTF-8 text"
        raise LiveTradingDisabledError(msg) from exc
    except OSError as exc:
        msg = f"{label} is unreadable: {path}"
        raise LiveTradingDisabledError(msg) from exc


def _require_json_artifact_generated_at(
    payload: Mapping[str, object],
    *,
    label: str,
    max_age_s: float,
) -> datetime:
    raw_generated_at = payload.get("generated_at")
    if not isinstance(raw_generated_at, str) or raw_generated_at.strip() == "":
        msg = f"{label} missing generated_at"
        raise LiveTradingDisabledError(msg)
    try:
        generated_at = datetime.fromisoformat(
            raw_generated_at.strip().replace("Z", "+00:00")
        )
    except ValueError as exc:
        msg = f"{label} generated_at is invalid"
        raise LiveTradingDisabledError(msg) from exc
    generated_at = _coerce_datetime(generated_at)
    now = datetime.now(tz=UTC)
    if generated_at > now:
        msg = f"{label} generated_at is in the future"
        raise LiveTradingDisabledError(msg)
    age_s = (now - generated_at).total_seconds()
    if age_s > max_age_s:
        msg = f"{label} is stale: age {age_s:.1f}s exceeds {max_age_s:.1f}s"
        raise LiveTradingDisabledError(msg)
    return generated_at


def _require_paper_backtest_diff_empty_lists(payload: Mapping[str, object]) -> None:
    for field_name, label in _LIVE_PAPER_BACKTEST_DIFF_EMPTY_LIST_FIELDS:
        value = payload.get(field_name)
        if not isinstance(value, list):
            msg = (
                "LIVE paper-vs-backtest execution diff artifact field "
                f"{field_name} must be a list"
            )
            raise LiveTradingDisabledError(msg)
        if value:
            msg = (
                "LIVE paper-vs-backtest execution diff artifact contains "
                f"{label}"
            )
            raise LiveTradingDisabledError(msg)


def _require_paper_backtest_diff_metric_number(
    metrics: Mapping[str, object],
    *,
    metric_name: str,
) -> float:
    raw_value = metrics[metric_name]
    if isinstance(raw_value, bool) or not isinstance(raw_value, int | float):
        msg = (
            "LIVE paper-vs-backtest execution diff artifact metric "
            f"{metric_name} must be a finite number"
        )
        raise LiveTradingDisabledError(msg)
    value = float(raw_value)
    if not math.isfinite(value):
        msg = (
            "LIVE paper-vs-backtest execution diff artifact metric "
            f"{metric_name} must be a finite number"
        )
        raise LiveTradingDisabledError(msg)
    return value


def _require_paper_backtest_diff_metric_consistency(
    metrics: Mapping[str, float],
    *,
    min_matched_decisions: int,
) -> None:
    matched_count = metrics["matched_decision_count"]
    paper_count = metrics["paper_decision_count"]
    backtest_count = metrics["backtest_decision_count"]
    if matched_count > paper_count:
        msg = (
            "LIVE paper-vs-backtest execution diff artifact metric "
            "matched_decision_count cannot exceed paper_decision_count"
        )
        raise LiveTradingDisabledError(msg)
    if matched_count > backtest_count:
        msg = (
            "LIVE paper-vs-backtest execution diff artifact metric "
            "matched_decision_count cannot exceed backtest_decision_count"
        )
        raise LiveTradingDisabledError(msg)
    if matched_count < _MIN_LIVE_PAPER_BACKTEST_DIFF_MATCHED_DECISIONS:
        msg = (
            "LIVE paper-vs-backtest execution diff artifact metric "
            "matched_decision_count must be at least "
            f"{_MIN_LIVE_PAPER_BACKTEST_DIFF_MATCHED_DECISIONS}"
        )
        raise LiveTradingDisabledError(msg)
    if matched_count < min_matched_decisions:
        msg = (
            "LIVE paper-vs-backtest execution diff artifact metric "
            "matched_decision_count must be at least min_matched_decisions"
        )
        raise LiveTradingDisabledError(msg)
    if matched_count != paper_count:
        msg = (
            "LIVE paper-vs-backtest execution diff artifact metric "
            "matched_decision_count must equal paper_decision_count when "
            "paper_only_decision_ids is empty"
        )
        raise LiveTradingDisabledError(msg)
    if matched_count != backtest_count:
        msg = (
            "LIVE paper-vs-backtest execution diff artifact metric "
            "matched_decision_count must equal backtest_decision_count when "
            "backtest_only_decision_ids is empty"
        )
        raise LiveTradingDisabledError(msg)
    for metric_name in _LIVE_PAPER_BACKTEST_DIFF_RATE_DELTA_METRICS:
        if metrics[metric_name] > 1.0:
            msg = (
                "LIVE paper-vs-backtest execution diff artifact metric "
                f"{metric_name} must be between 0 and 1"
            )
            raise LiveTradingDisabledError(msg)


def _require_paper_backtest_diff_thresholds(
    metrics: Mapping[str, float],
    *,
    thresholds: Mapping[str, object],
) -> None:
    for metric_name, threshold_name in _LIVE_PAPER_BACKTEST_DIFF_THRESHOLD_METRICS:
        if threshold_name not in thresholds:
            msg = (
                "LIVE paper-vs-backtest execution diff artifact missing "
                f"threshold: {threshold_name}"
            )
            raise LiveTradingDisabledError(msg)
        threshold = _require_paper_backtest_diff_threshold_number(
            thresholds,
            threshold_name=threshold_name,
        )
        if metrics[metric_name] > threshold:
            msg = (
                "LIVE paper-vs-backtest execution diff artifact metric "
                f"{metric_name} exceeds {threshold_name}"
            )
            raise LiveTradingDisabledError(msg)


def _require_paper_backtest_diff_min_matched_decisions_threshold(
    thresholds: Mapping[str, object],
) -> int:
    threshold_name = _LIVE_PAPER_BACKTEST_DIFF_MIN_MATCHED_DECISIONS_THRESHOLD
    if threshold_name not in thresholds:
        msg = (
            "LIVE paper-vs-backtest execution diff artifact missing "
            f"threshold: {threshold_name}"
        )
        raise LiveTradingDisabledError(msg)
    raw_value = thresholds[threshold_name]
    if isinstance(raw_value, bool) or not isinstance(raw_value, int | float):
        msg = (
            "LIVE paper-vs-backtest execution diff artifact threshold "
            f"{threshold_name} must be a finite positive integer count"
        )
        raise LiveTradingDisabledError(msg)
    value = float(raw_value)
    if not math.isfinite(value) or value <= 0.0 or not value.is_integer():
        msg = (
            "LIVE paper-vs-backtest execution diff artifact threshold "
            f"{threshold_name} must be a finite positive integer count"
        )
        raise LiveTradingDisabledError(msg)
    threshold = int(value)
    if threshold < _MIN_LIVE_PAPER_BACKTEST_DIFF_MATCHED_DECISIONS:
        msg = (
            "LIVE paper-vs-backtest execution diff artifact threshold "
            f"{threshold_name} must be at least "
            f"{_MIN_LIVE_PAPER_BACKTEST_DIFF_MATCHED_DECISIONS}"
        )
        raise LiveTradingDisabledError(msg)
    return threshold


def _require_paper_backtest_diff_threshold_number(
    thresholds: Mapping[str, object],
    *,
    threshold_name: str,
) -> float:
    raw_value = thresholds[threshold_name]
    if isinstance(raw_value, bool) or not isinstance(raw_value, int | float):
        msg = (
            "LIVE paper-vs-backtest execution diff artifact threshold "
            f"{threshold_name} must be a finite nonnegative number"
        )
        raise LiveTradingDisabledError(msg)
    value = float(raw_value)
    if not math.isfinite(value) or value < 0.0:
        msg = (
            "LIVE paper-vs-backtest execution diff artifact threshold "
            f"{threshold_name} must be a finite nonnegative number"
        )
        raise LiveTradingDisabledError(msg)
    return value


def _count_category_prior_observations(text: str) -> int:
    reader = csv.DictReader(text.splitlines())
    _require_unique_csv_fieldnames(
        reader.fieldnames,
        label="LIVE category-prior artifact invalid",
    )
    fieldnames = set(reader.fieldnames or ())
    missing_columns = sorted(_REQUIRED_CATEGORY_PRIOR_COLUMNS - fieldnames)
    if missing_columns:
        msg = (
            "LIVE category-prior artifact invalid: missing columns: "
            f"{', '.join(missing_columns)}"
        )
        raise LiveTradingDisabledError(msg)
    seen_market_ids: set[str] = set()
    observation_count = 0
    for row_number, row in enumerate(reader, start=2):
        market_id = (row.get("market_id") or "").strip()
        if market_id == "":
            msg = f"LIVE category-prior artifact invalid: row {row_number} missing market_id"
            raise LiveTradingDisabledError(msg)
        if market_id in seen_market_ids:
            msg = f"LIVE category-prior artifact invalid: duplicate market_id {market_id}"
            raise LiveTradingDisabledError(msg)
        seen_market_ids.add(market_id)
        if (row.get("category") or "").strip() == "":
            msg = f"LIVE category-prior artifact invalid: row {row_number} missing category"
            raise LiveTradingDisabledError(msg)
        _parse_category_prior_resolved_at(row, row_number=row_number)
        yes_payout = (row.get("yes_payout") or "").strip()
        no_payout = (row.get("no_payout") or "").strip()
        if (yes_payout, no_payout) == ("0.5", "0.5"):
            continue
        if (yes_payout, no_payout) not in {("1", "0"), ("0", "1")}:
            msg = (
                "LIVE category-prior artifact invalid: "
                f"row {row_number} has non-settled payout vector"
            )
            raise LiveTradingDisabledError(msg)
        observation_count += 1
    return observation_count


def _parse_category_prior_resolved_at(
    row: Mapping[str, str | None],
    *,
    row_number: int,
) -> None:
    raw_value = (row.get("resolved_at") or "").strip()
    if raw_value == "":
        msg = f"LIVE category-prior artifact invalid: row {row_number} missing resolved_at"
        raise LiveTradingDisabledError(msg)
    try:
        datetime.fromisoformat(raw_value.replace("Z", "+00:00"))
    except ValueError as exc:
        msg = f"LIVE category-prior artifact invalid: row {row_number} invalid resolved_at"
        raise LiveTradingDisabledError(msg) from exc


def _validate_flb_calibration_rows(text: str, *, min_sample_count: int) -> None:
    reader = csv.DictReader(text.splitlines())
    _require_unique_csv_fieldnames(
        reader.fieldnames,
        label="LIVE FLB calibration artifact invalid",
    )
    fieldnames = set(reader.fieldnames or ())
    missing_columns = sorted(_REQUIRED_FLB_CALIBRATION_COLUMNS - fieldnames)
    if missing_columns:
        msg = (
            "LIVE FLB calibration artifact invalid: missing columns: "
            f"{', '.join(missing_columns)}"
        )
        raise LiveTradingDisabledError(msg)
    observed_signals: set[str] = set()
    for row_number, row in enumerate(reader, start=2):
        signal_name = (row.get("signal_name") or "").strip()
        if signal_name == "":
            msg = f"LIVE FLB calibration artifact invalid: row {row_number} missing signal_name"
            raise LiveTradingDisabledError(msg)
        if signal_name not in _REQUIRED_FLB_SIGNALS:
            msg = (
                "LIVE FLB calibration artifact invalid: "
                f"row {row_number} unsupported FLB signal_name {signal_name!r}"
            )
            raise LiveTradingDisabledError(msg)
        if signal_name in observed_signals:
            msg = (
                "LIVE FLB calibration artifact invalid: "
                f"duplicate FLB calibration for {signal_name!r}"
            )
            raise LiveTradingDisabledError(msg)
        observed_signals.add(signal_name)
        _require_unit_probability(
            row.get("probability_estimate"),
            row_number=row_number,
        )
        sample_count = _require_sample_count(row.get("sample_count"), row_number=row_number)
        if sample_count < min_sample_count:
            msg = (
                "LIVE FLB calibration artifact invalid: "
                f"{signal_name} sample_count {sample_count} < {min_sample_count}"
            )
            raise LiveTradingDisabledError(msg)
        _require_flb_calibration_source_label(
            row.get("source_label"),
            row_number=row_number,
        )
    missing_signals = sorted(_REQUIRED_FLB_SIGNALS - observed_signals)
    if missing_signals:
        msg = (
            "LIVE FLB calibration artifact invalid: missing calibrated FLB "
            f"signals: {', '.join(missing_signals)}"
        )
        raise LiveTradingDisabledError(msg)


def _require_flb_calibration_source_label(
    raw_value: str | None,
    *,
    row_number: int,
) -> None:
    value = (raw_value or "").strip()
    if value == "":
        msg = f"LIVE FLB calibration artifact invalid: row {row_number} missing source_label"
        raise LiveTradingDisabledError(msg)
    if len(value) > _FLB_CALIBRATION_SOURCE_LABEL_MAX_LENGTH:
        msg = (
            "LIVE FLB calibration artifact invalid: "
            f"row {row_number} source_label too long"
        )
        raise LiveTradingDisabledError(msg)
    if not _is_flb_calibration_source_label_slug(value):
        msg = (
            "LIVE FLB calibration artifact invalid: "
            f"row {row_number} source_label must be a lowercase audit slug"
        )
        raise LiveTradingDisabledError(msg)
    parts = {
        part
        for separator in ("-", "_")
        for part in value.split(separator)
        if part
    }
    forbidden = sorted(parts & _FLB_CALIBRATION_SOURCE_LABEL_FORBIDDEN_PARTS)
    if forbidden:
        msg = (
            "LIVE FLB calibration artifact invalid: "
            f"row {row_number} source_label contains forbidden source marker "
            f"{forbidden[0]}"
        )
        raise LiveTradingDisabledError(msg)


def _is_flb_calibration_source_label_slug(value: str) -> bool:
    if value == "":
        return False
    if value[0] < "a" or value[0] > "z":
        return False
    if not ("0" <= value[-1] <= "9" or "a" <= value[-1] <= "z"):
        return False
    return all(
        character in {"-", "_"}
        or "a" <= character <= "z"
        or "0" <= character <= "9"
        for character in value
    )


def _require_unique_csv_fieldnames(
    fieldnames: Sequence[str] | None,
    *,
    label: str,
) -> None:
    if fieldnames is None:
        return
    seen: set[str] = set()
    for fieldname in fieldnames:
        if fieldname in seen:
            msg = f"{label}: duplicate CSV column: {fieldname}"
            raise LiveTradingDisabledError(msg)
        seen.add(fieldname)


def _require_unit_probability(raw_value: str | None, *, row_number: int) -> float:
    try:
        value = float((raw_value or "").strip())
    except ValueError as exc:
        msg = f"LIVE FLB calibration artifact invalid: row {row_number} invalid probability"
        raise LiveTradingDisabledError(msg) from exc
    if not math.isfinite(value) or not 0.0 < value < 1.0:
        msg = f"LIVE FLB calibration artifact invalid: row {row_number} invalid probability"
        raise LiveTradingDisabledError(msg)
    return value


def _require_sample_count(raw_value: str | None, *, row_number: int) -> int:
    try:
        value = int((raw_value or "").strip())
    except ValueError as exc:
        msg = f"LIVE FLB calibration artifact invalid: row {row_number} invalid sample_count"
        raise LiveTradingDisabledError(msg) from exc
    if value <= 0:
        msg = f"LIVE FLB calibration artifact invalid: row {row_number} invalid sample_count"
        raise LiveTradingDisabledError(msg)
    return value


def _looks_like_artifact_placeholder(value: str) -> bool:
    normalized = value.strip().lower()
    if normalized == "":
        return False
    return any(
        marker in normalized
        for marker in ("fill_in", "__fill", "todo", "replace", "placeholder")
    )


def _json_artifact_generated_at(raw_path: str | None, *, label: str) -> datetime:
    if raw_path is None or raw_path.strip() == "":
        msg = f"{label} path missing for credentialed preflight chronology"
        raise LiveTradingDisabledError(msg)
    path = Path(raw_path).expanduser()
    try:
        raw_payload = loads_json_rejecting_duplicate_keys(
            _read_bytes_no_follow(path).decode("utf-8"),
            label=label,
        )
    except json.JSONDecodeError as exc:
        msg = f"{label} is invalid JSON for credentialed preflight chronology"
        raise LiveTradingDisabledError(msg) from exc
    except OSError as exc:
        msg = f"{label} is unreadable for credentialed preflight chronology: {path}"
        raise LiveTradingDisabledError(msg) from exc
    if not isinstance(raw_payload, dict):
        msg = f"{label} must be a JSON object for credentialed preflight chronology"
        raise LiveTradingDisabledError(msg)
    generated_at = raw_payload.get("generated_at")
    if not isinstance(generated_at, str) or generated_at.strip() == "":
        msg = f"{label} missing generated_at for credentialed preflight chronology"
        raise LiveTradingDisabledError(msg)
    try:
        parsed = datetime.fromisoformat(generated_at.strip().replace("Z", "+00:00"))
    except ValueError as exc:
        msg = f"{label} generated_at is invalid for credentialed preflight chronology"
        raise LiveTradingDisabledError(msg) from exc
    return _coerce_datetime(parsed)


def canonical_sha256(payload: Mapping[str, object]) -> str:
    canonical = json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return sha256(canonical.encode("utf-8")).hexdigest()


def is_sha256_hexdigest(value: str) -> bool:
    return len(value) == 64 and all(
        character in "0123456789abcdef" for character in value
    )


def _optional_sha256(value: str | None) -> str | None:
    if value is None:
        return None
    return sha256(value.encode("utf-8")).hexdigest()


def latest_live_emergency_audit_timestamp(raw_path: str | None) -> datetime | None:
    if raw_path is None or raw_path.strip() == "":
        return None
    path = Path(raw_path).expanduser()
    try:
        path_stat = path.lstat()
    except FileNotFoundError:
        return None
    if not stat.S_ISREG(path_stat.st_mode):
        msg = f"LIVE emergency audit path is not a regular file: {path}"
        raise LiveTradingDisabledError(msg)
    if path_stat.st_nlink != 1:
        msg = f"LIVE emergency audit path is not a single-link file: {path}"
        raise LiveTradingDisabledError(msg)
    try:
        audit_text = _read_bytes_no_follow(path).decode("utf-8")
    except OSError as exc:
        msg = (
            "LIVE emergency audit is unreadable for credentialed "
            f"preflight chronology: {path}"
        )
        raise LiveTradingDisabledError(msg) from exc

    latest: datetime | None = None
    for line_number, raw_line in enumerate(audit_text.splitlines(), start=1):
        line = raw_line.strip()
        if line == "":
            continue
        record = _emergency_audit_record(line, path=path, line_number=line_number)
        timestamp = _emergency_audit_record_timestamp(
            record,
            path=path,
            line_number=line_number,
        )
        if latest is None or timestamp > latest:
            latest = timestamp
    return latest


def _emergency_audit_record(
    line: str,
    *,
    path: Path,
    line_number: int,
) -> Mapping[str, object]:
    try:
        record = loads_json_rejecting_duplicate_keys(
            line,
            label=(
                "LIVE emergency audit record invalid for credentialed "
                f"preflight chronology: {path}:{line_number}"
            ),
        )
    except json.JSONDecodeError as exc:
        msg = (
            "LIVE emergency audit record invalid for credentialed preflight "
            f"chronology: {path}:{line_number}"
        )
        raise LiveTradingDisabledError(msg) from exc
    if not isinstance(record, dict):
        msg = (
            "LIVE emergency audit record invalid for credentialed preflight "
            f"chronology: {path}:{line_number}"
        )
        raise LiveTradingDisabledError(msg)
    return record


def _emergency_audit_record_timestamp(
    record: Mapping[str, object],
    *,
    path: Path,
    line_number: int,
) -> datetime:
    raw_timestamp = record.get("timestamp")
    if not isinstance(raw_timestamp, str) or raw_timestamp.strip() == "":
        msg = (
            "LIVE emergency audit record invalid for credentialed preflight "
            f"chronology: {path}:{line_number} missing timestamp"
        )
        raise LiveTradingDisabledError(msg)
    try:
        parsed = datetime.fromisoformat(raw_timestamp.strip().replace("Z", "+00:00"))
    except ValueError as exc:
        msg = (
            "LIVE emergency audit record invalid for credentialed preflight "
            f"chronology: {path}:{line_number} invalid timestamp"
        )
        raise LiveTradingDisabledError(msg) from exc
    return _coerce_datetime(parsed)


def _coerce_datetime(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _readiness_report_generated_at(raw_path: str | None, *, label: str) -> datetime:
    if raw_path is None or raw_path.strip() == "":
        msg = f"{label} path missing for credentialed preflight chronology"
        raise LiveTradingDisabledError(msg)
    path = Path(raw_path).expanduser()
    try:
        report_text = _read_bytes_no_follow(path).decode("utf-8")
    except OSError as exc:
        msg = f"{label} is unreadable for credentialed preflight chronology: {path}"
        raise LiveTradingDisabledError(msg) from exc
    raw_generated_at = _markdown_report_provenance_value(
        report_text,
        field_name="generated_at",
        label=label,
    )
    try:
        generated_at = datetime.fromisoformat(raw_generated_at.replace("Z", "+00:00"))
    except ValueError as exc:
        msg = f"{label} persisted provenance generated_at is invalid"
        raise LiveTradingDisabledError(msg) from exc
    return _coerce_datetime(generated_at)


def _markdown_report_provenance_value(
    report_text: str,
    *,
    field_name: str,
    label: str,
) -> str:
    in_provenance = False
    for raw_line in report_text.splitlines():
        line = raw_line.strip()
        if line.startswith("## "):
            in_provenance = line == "## Report Provenance"
            continue
        if not in_provenance or not line.startswith("|"):
            continue
        cells = _markdown_table_cells(line)
        if len(cells) < 2 or cells[0] != field_name:
            continue
        if len(cells) != 2:
            msg = f"{label} persisted provenance malformed {field_name} row"
            raise LiveTradingDisabledError(msg)
        value = cells[1].strip()
        if value == "":
            break
        return value
    msg = f"{label} persisted provenance missing {field_name}"
    raise LiveTradingDisabledError(msg)


def _markdown_table_cells(line: str) -> list[str]:
    cells: list[str] = []
    current: list[str] = []
    consecutive_backslashes = 0
    for character in line.strip():
        if character == "|" and consecutive_backslashes % 2 == 0:
            cells.append("".join(current))
            current = []
        else:
            current.append(character)

        if character == "\\":
            consecutive_backslashes += 1
        else:
            consecutive_backslashes = 0
    cells.append("".join(current))

    if cells and cells[0] == "":
        cells = cells[1:]
    if cells and cells[-1] == "":
        cells = cells[:-1]
    return [_unescape_markdown_table_cell(cell.strip()) for cell in cells]


def _unescape_markdown_table_cell(value: str) -> str:
    chars: list[str] = []
    index = 0
    while index < len(value):
        character = value[index]
        if (
            character == "\\"
            and index + 1 < len(value)
            and value[index + 1] == "|"
        ):
            chars.append("|")
            index += 2
            continue
        chars.append(character)
        index += 1
    return "".join(chars)


def _readiness_report_fingerprint_payload(
    raw_path: str | None,
    *,
    label: str,
) -> dict[str, str]:
    if raw_path is None or raw_path.strip() == "":
        msg = f"{label} path missing for credentialed preflight fingerprint"
        raise LiveTradingDisabledError(msg)
    path = Path(raw_path).expanduser()
    _require_readiness_report_outside_working_tree(path, label=label)
    _require_readiness_report_parent_owner_writable(path, label=label)
    _require_readiness_report_regular_file_for_fingerprint(path, label=label)
    try:
        content = _read_bytes_no_follow(path)
    except OSError as exc:
        msg = f"{label} is unreadable for credentialed preflight fingerprint: {path}"
        raise LiveTradingDisabledError(msg) from exc
    return {
        "path": str(path.resolve(strict=False)),
        "sha256": sha256(content).hexdigest(),
    }


def _require_readiness_report_outside_working_tree(
    path: Path,
    *,
    label: str,
) -> None:
    configured_path = _absolute_path_without_symlink_resolution(path)
    resolved_path = path.expanduser().resolve(strict=False)
    working_tree = _working_tree_root(Path.cwd().resolve(strict=False))
    working_trees = [working_tree]
    for candidate in (configured_path, resolved_path):
        candidate_working_tree = _containing_working_tree_root(candidate)
        if candidate_working_tree is not None:
            working_trees.append(candidate_working_tree)

    for working_tree_candidate in dict.fromkeys(working_trees):
        if working_tree_candidate.parent == working_tree_candidate:
            continue
        for candidate in (configured_path, resolved_path):
            try:
                candidate.relative_to(working_tree_candidate)
            except ValueError:
                continue
            msg = (
                f"{label} must live outside the working tree for "
                f"credentialed preflight fingerprint: {candidate}"
            )
            raise LiveTradingDisabledError(msg)


def _absolute_path_without_symlink_resolution(path: Path) -> Path:
    expanded = path.expanduser()
    if not expanded.is_absolute():
        expanded = Path.cwd() / expanded
    return Path(os.path.abspath(expanded))


def _working_tree_root(start: Path) -> Path:
    for candidate in (start, *start.parents):
        if (candidate / ".git").exists():
            return candidate
    return start


def _containing_working_tree_root(path: Path) -> Path | None:
    start = path if path.is_dir() else path.parent
    for candidate in (start, *start.parents):
        if (candidate / ".git").exists():
            return candidate
    return None


def _require_readiness_report_parent_owner_writable(
    path: Path,
    *,
    label: str,
) -> None:
    parent = path.parent
    try:
        parent_stat = parent.lstat()
    except FileNotFoundError as exc:
        msg = f"{label} parent directory does not exist: {parent}"
        raise LiveTradingDisabledError(msg) from exc
    if not stat.S_ISDIR(parent_stat.st_mode):
        msg = f"{label} parent path is not a directory: {parent}"
        raise LiveTradingDisabledError(msg)
    mode = stat.S_IMODE(parent_stat.st_mode)
    if mode & 0o077:
        msg = (
            f"{label} parent directory {parent} is too permissive; "
            "run chmod 700"
        )
        raise LiveTradingDisabledError(msg)
    if not mode & stat.S_IWUSR:
        msg = (
            f"{label} parent directory {parent} is not owner-writable; "
            "run chmod 700"
        )
        raise LiveTradingDisabledError(msg)


def _require_readiness_report_regular_file_for_fingerprint(
    path: Path,
    *,
    label: str,
) -> None:
    try:
        path_stat = path.lstat()
    except FileNotFoundError as exc:
        msg = f"{label} does not exist for credentialed preflight fingerprint: {path}"
        raise LiveTradingDisabledError(msg) from exc
    if not stat.S_ISREG(path_stat.st_mode):
        msg = (
            f"{label} is not a regular file for credentialed preflight "
            f"fingerprint: {path}"
        )
        raise LiveTradingDisabledError(msg)
    if path_stat.st_nlink != 1:
        msg = (
            f"{label} is not a single-link file for credentialed preflight "
            f"fingerprint: {path}"
        )
        raise LiveTradingDisabledError(msg)


def _read_bytes_no_follow(path: Path) -> bytes:
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(path, flags, 0o777)
    try:
        path_stat = os.fstat(fd)
        if not stat.S_ISREG(path_stat.st_mode):
            raise OSError(f"path is not a regular file: {path}")
        if path_stat.st_nlink != 1:
            raise OSError(f"path is not a single-link file: {path}")
        with os.fdopen(fd, "rb") as file:
            fd = -1
            return file.read()
    finally:
        if fd >= 0:
            os.close(fd)
