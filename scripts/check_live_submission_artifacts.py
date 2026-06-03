"""Validate non-credential LIVE submission artifacts.

This checker is intentionally credential-free. It validates the launch artifacts
that must be ready before a credentialed preflight can become final GO, without
checking Polymarket secrets, venue connectivity, or account state.
"""

from __future__ import annotations

import argparse
import json
import shlex
from collections.abc import Callable, Sequence
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path

from pms.config import (
    PMSSettings,
    _require_live_operator_rehearsal_report,
    _require_live_paper_soak_go_report,
)
from pms.core.models import LiveTradingDisabledError
from pms.live_preflight_artifact import (
    _validate_live_category_prior_artifact,
    _validate_live_execution_model_artifact,
    _validate_live_flb_calibration_artifact,
    _validate_live_paper_backtest_diff_artifact,
)


@dataclass(frozen=True, slots=True)
class LiveSubmissionArtifactCheck:
    name: str
    passed: bool
    detail: str
    remediation: str | None = None


def check_live_submission_artifacts(
    settings: PMSSettings,
    *,
    config_path: str,
) -> tuple[LiveSubmissionArtifactCheck, ...]:
    """Return machine-checkable non-credential LIVE artifact readiness checks."""
    return (
        _check_readiness_report(
            settings,
            name="paper_soak_go_report",
            raw_path=settings.live_paper_soak_report_path,
            validator=_require_live_paper_soak_go_report,
            remediation=_paper_soak_report_remediation(config_path),
        ),
        _check_readiness_report(
            settings,
            name="operator_rehearsal_report",
            raw_path=settings.live_operator_rehearsal_report_path,
            validator=_require_live_operator_rehearsal_report,
            remediation=_operator_rehearsal_remediation(),
        ),
        _check_strategy_artifact(
            settings,
            name="execution_model",
            raw_path=settings.live_execution_model_path,
            validator=_validate_live_execution_model_artifact,
            remediation=_execution_model_remediation(),
        ),
        _check_strategy_artifact(
            settings,
            name="paper_backtest_diff",
            raw_path=settings.live_paper_backtest_diff_path,
            validator=_validate_live_paper_backtest_diff_artifact,
            remediation=_paper_backtest_diff_remediation(),
        ),
        _check_strategy_artifact(
            settings,
            name="category_prior",
            raw_path=settings.controller.category_prior_observations_path,
            validator=_validate_live_category_prior_artifact,
            remediation=_category_prior_remediation(),
        ),
        _check_strategy_artifact(
            settings,
            name="flb_calibration",
            raw_path=settings.strategies.flb_calibration_path,
            validator=_validate_live_flb_calibration_artifact,
            remediation=_flb_calibration_remediation(),
        ),
    )


def _check_readiness_report(
    settings: PMSSettings,
    *,
    name: str,
    raw_path: str | None,
    validator: Callable[[PMSSettings], datetime],
    remediation: str,
) -> LiveSubmissionArtifactCheck:
    try:
        generated_at = validator(settings)
    except (LiveTradingDisabledError, OSError, ValueError) as exc:
        return LiveSubmissionArtifactCheck(name, False, str(exc), remediation)
    return LiveSubmissionArtifactCheck(
        name,
        True,
        f"validated {cast_path_detail(raw_path)} (generated_at={generated_at.isoformat()})",
    )


def _check_strategy_artifact(
    settings: PMSSettings,
    *,
    name: str,
    raw_path: str | None,
    validator: Callable[[PMSSettings], None],
    remediation: str,
) -> LiveSubmissionArtifactCheck:
    try:
        validator(settings)
    except (LiveTradingDisabledError, OSError, ValueError) as exc:
        return LiveSubmissionArtifactCheck(name, False, str(exc), remediation)
    return LiveSubmissionArtifactCheck(
        name,
        True,
        f"validated {cast_path_detail(raw_path)}",
    )


def cast_path_detail(raw_path: str | None) -> str:
    if raw_path is None or raw_path.strip() == "":
        return "<missing path>"
    return str(Path(raw_path).expanduser())


def _format_plain(checks: Sequence[LiveSubmissionArtifactCheck]) -> str:
    lines: list[str] = []
    for check in checks:
        status = "PASS" if check.passed else "FAIL"
        lines.append(f"[{status}] {check.name}: {check.detail}")
        if check.remediation is not None:
            lines.append(f"  next: {check.remediation}")
    return "\n".join(lines)


def _format_json(checks: Sequence[LiveSubmissionArtifactCheck]) -> str:
    payload = {
        "ok": all(check.passed for check in checks),
        "checks": [asdict(check) for check in checks],
    }
    return json.dumps(payload, allow_nan=False, indent=2, sort_keys=True)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Validate non-credential LIVE submission artifacts before "
            "credentialed preflight."
        )
    )
    parser.add_argument(
        "--config",
        default="config.live-soak.yaml",
        help="Config path containing LIVE submission artifact paths.",
    )
    parser.add_argument("--json", action="store_true", help="Emit JSON output.")
    args = parser.parse_args(argv)

    config_path = cast_str(args.config)
    try:
        settings = PMSSettings.load(config_path)
    except Exception as exc:  # noqa: BLE001
        check = LiveSubmissionArtifactCheck("config_load", False, str(exc))
        output = _format_json((check,)) if args.json else _format_plain((check,))
        print(output)
        return 1

    checks = check_live_submission_artifacts(settings, config_path=config_path)
    output = _format_json(checks) if args.json else _format_plain(checks)
    print(output)
    return 0 if all(check.passed for check in checks) else 1


def cast_str(value: object) -> str:
    if isinstance(value, str):
        return value
    return str(value)


def _paper_soak_report_remediation(config_path: str) -> str:
    return (
        "complete the 30-day H1 PAPER soak, then run: "
        "uv run python scripts/paper_report.py --require-go "
        f"--config {shlex.quote(config_path)} "
        '--output "$PMS_SECURE_DIR/paper-soak-go-report.md"; '
        "configure live_paper_soak_report_path to that private path"
    )


def _operator_rehearsal_remediation() -> str:
    return (
        "run: uv run python scripts/rehearse_first_order.py "
        '--workdir "$PMS_SECURE_DIR/operator-rehearsal" '
        "--approver-id <operator-id>; configure "
        "live_operator_rehearsal_report_path to the generated PASS report"
    )


def _execution_model_remediation() -> str:
    return (
        "run: uv run python scripts/export_paper_execution_from_api.py "
        '--execution-output "$PMS_SECURE_DIR/paper-execution-export.csv" '
        '--telemetry-output "$PMS_SECURE_DIR/paper-execution-telemetry.csv" '
        "--require-adverse-selection; then run: "
        "uv run python scripts/execution_model_from_telemetry.py "
        '--input "$PMS_SECURE_DIR/paper-execution-telemetry.csv" '
        '--output "$PMS_SECURE_DIR/execution-model.json" --fee-rate 0.07 '
        "--staleness-ms 120000 --displayed-depth-fill-ratio 0.75 "
        "--require-adverse-selection --min-samples 30; configure "
        "live_execution_model_path to that private path"
    )


def _paper_backtest_diff_remediation() -> str:
    return (
        "run: uv run python scripts/export_paper_execution_from_api.py "
        '--execution-output "$PMS_SECURE_DIR/paper-execution-export.csv" '
        '--telemetry-output "$PMS_SECURE_DIR/paper-execution-telemetry.csv" '
        "--require-adverse-selection; then run the matching research "
        "backtest, then run: "
        "uv run python scripts/export_backtest_execution_from_db.py "
        "--run-id <backtest-run-id> "
        '--output "$PMS_SECURE_DIR/backtest-execution-export.csv"; then run: '
        "uv run python scripts/paper_backtest_execution_diff.py "
        '--paper "$PMS_SECURE_DIR/paper-execution-export.csv" '
        '--backtest "$PMS_SECURE_DIR/backtest-execution-export.csv" '
        '--output "$PMS_SECURE_DIR/paper-backtest-execution-diff.json" '
        "--max-fill-rate-delta 0.05 --max-rejection-rate-delta 0.05 "
        "--max-avg-slippage-bps-delta 5 --max-total-pnl-delta 1 "
        "--min-matched-decisions 10 --require-pass; configure "
        "live_paper_backtest_diff_path to that private path"
    )


def _category_prior_remediation() -> str:
    return (
        "run: uv run python scripts/export_category_prior_observations.py "
        '--output "$PMS_SECURE_DIR/category-prior-observations.csv" '
        "--min-observations 100; configure "
        "controller.category_prior_observations_path to that private path"
    )


def _flb_calibration_remediation() -> str:
    return (
        "load DUNE_API_KEY from the operator secret store, then run: "
        "uv run python scripts/export_flb_warehouse_from_dune.py "
        '--output "$PMS_SECURE_DIR/polymarket_resolved_binary.csv" '
        "--performance large; uv run python scripts/flb_data_feasibility.py "
        "--source warehouse-csv "
        '--input "$PMS_SECURE_DIR/polymarket_resolved_binary.csv" '
        '--output "$PMS_SECURE_DIR/flb-feasibility.md" '
        '--csv "$PMS_SECURE_DIR/flb-deciles.csv" '
        '--calibration-csv "$PMS_SECURE_DIR/flb-calibration.csv" '
        "--calibration-source-label warehouse-flb-v1 "
        "--calibration-provenance-json "
        '"$PMS_SECURE_DIR/flb-calibration.csv.provenance.json"; configure '
        "strategies.flb_calibration_path to the calibration CSV"
    )


if __name__ == "__main__":
    raise SystemExit(main())
