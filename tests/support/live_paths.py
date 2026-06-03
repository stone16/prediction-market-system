from __future__ import annotations

import json
import tempfile
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from pathlib import Path

from pms.config import PMSSettings
from pms.live_preflight import (
    live_preflight_readiness_reports_fingerprint,
    live_preflight_settings_fingerprint,
)


ROOT = Path(__file__).resolve().parents[2]


def make_private_live_paths(*, prefix: str = "pms-live-test-") -> tuple[str, str]:
    root = Path(tempfile.mkdtemp(prefix=prefix))
    root.chmod(0o700)
    return (
        str(root / "first-order.json"),
        str(root / "first-order-audit.jsonl"),
    )


def make_live_report_paths(*, prefix: str = "pms-live-reports-") -> tuple[str, str]:
    root = Path(tempfile.mkdtemp(prefix=prefix))
    root.chmod(0o700)
    paper_report_path = root / "paper-soak-go-report.md"
    rehearsal_report_path = root / "operator-rehearsal-pass-report.md"
    generated_at = datetime.now(tz=UTC) - timedelta(seconds=60)
    generated_at_line = f"| generated_at | {generated_at.isoformat()} |"
    paper_report_text = (
        ROOT / "tests" / "fixtures" / "paper_soak_go_report.md"
    ).read_text(encoding="utf-8")
    paper_report_path.write_text(
        paper_report_text.replace(
            "| generated_at | 2026-05-25T00:00:00+00:00 |",
            generated_at_line,
        ).replace(
            "| output_path | docs/paper-reports/2026-05-25.md |",
            f"| output_path | {paper_report_path} |",
        ),
        encoding="utf-8",
    )
    rehearsal_report_text = (
        ROOT / "tests" / "fixtures" / "operator_rehearsal_pass_report.md"
    ).read_text(encoding="utf-8")
    rehearsal_report_path.write_text(
        rehearsal_report_text.replace(
            "| generated_at | 2026-05-25T00:00:00+00:00 |",
            generated_at_line,
        ).replace(
            "| output_path | docs/live/operator-rehearsal-report.md |",
            f"| output_path | {rehearsal_report_path} |",
        ),
        encoding="utf-8",
    )
    return str(paper_report_path), str(rehearsal_report_path)


def make_live_execution_model_path(
    *,
    prefix: str = "pms-live-execution-model-",
) -> str:
    root = Path(tempfile.mkdtemp(prefix=prefix))
    root.chmod(0o700)
    path = root / "execution-model.json"
    path.write_text(
        json.dumps(
            {
                "generated_by": "scripts/execution_model_from_telemetry.py",
                "artifact_mode": "telemetry_execution_model",
                "generated_at": datetime.now(tz=UTC).isoformat(),
                "fee_rate": 0.04,
                "slippage_bps": 6.0,
                "latency_ms": 500.0,
                "staleness_ms": 120_000.0,
                "fill_policy": "immediate_or_cancel",
                "displayed_depth_fill_ratio": 0.75,
                "adverse_selection_bps": 9.0,
                "order_ttl_ms": 60_000,
                "price_invalidation_streak": 10,
                "replay_window_ms": 86_400_000,
                "calibration_source": "telemetry_calibrated",
                "min_samples": 10,
                "telemetry_sample_count": 10,
                "adverse_selection_sample_count": 10,
                "require_adverse_selection": True,
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    return str(path)


def make_live_paper_backtest_diff_path(
    *,
    prefix: str = "pms-live-paper-backtest-diff-",
) -> str:
    root = Path(tempfile.mkdtemp(prefix=prefix))
    root.chmod(0o700)
    path = root / "paper-backtest-execution-diff.json"
    path.write_text(
        json.dumps(
            {
                "generated_by": "scripts/paper_backtest_execution_diff.py",
                "artifact_mode": "paper_backtest_execution_diff",
                "generated_at": datetime.now(tz=UTC).isoformat(),
                "strategy_evidence": (
                    "default@"
                    "4d326514fa853b9278502ad43750b9648ac8f4f6ad8685ba522b2a4aa5f47d25"
                ),
                "final_go_no_go_valid": True,
                "thresholds": {
                    "min_matched_decisions": 10,
                    "max_fill_rate_delta": 0.05,
                    "max_rejection_rate_delta": 0.05,
                    "max_avg_slippage_bps_delta": 5.0,
                    "max_total_pnl_delta": 1.0,
                },
                "metrics": {
                    "paper_decision_count": 10,
                    "backtest_decision_count": 10,
                    "matched_decision_count": 10,
                    "paper_fill_rate": 0.5,
                    "backtest_fill_rate": 0.5,
                    "fill_rate_delta_abs": 0.0,
                    "paper_rejection_rate": 0.5,
                    "backtest_rejection_rate": 0.5,
                    "rejection_rate_delta_abs": 0.0,
                    "paper_avg_slippage_bps": 3.0,
                    "backtest_avg_slippage_bps": 3.0,
                    "avg_slippage_bps_delta_abs": 0.0,
                    "paper_total_pnl": 1.2,
                    "backtest_total_pnl": 1.2,
                    "total_pnl_delta_abs": 0.0,
                },
                "paper_only_decision_ids": [],
                "backtest_only_decision_ids": [],
                "status_mismatches": [],
                "failures": [],
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    return str(path)


def make_live_category_prior_path(
    *,
    prefix: str = "pms-live-category-prior-",
) -> str:
    root = Path(tempfile.mkdtemp(prefix=prefix))
    root.chmod(0o700)
    path = root / "category-prior-observations.csv"
    rows = ["market_id,category,yes_payout,no_payout,resolved_at"]
    for index in range(1, 121):
        category = "politics" if index % 2 == 0 else "sports"
        yes_payout, no_payout = ("1", "0") if index % 3 == 0 else ("0", "1")
        rows.append(
            f"m-{index},{category},{yes_payout},{no_payout},"
            f"2026-05-{(index % 20) + 1:02d}T12:00:00Z"
        )
    path.write_text("\n".join(rows) + "\n", encoding="utf-8")
    return str(path)


def make_live_flb_calibration_path(
    *,
    prefix: str = "pms-live-flb-calibration-",
) -> str:
    root = Path(tempfile.mkdtemp(prefix=prefix))
    root.chmod(0o700)
    path = root / "flb-calibration.csv"
    path.write_text(
        "\n".join(
            (
                "signal_name,probability_estimate,sample_count,source_label",
                "longshot_yes_overpriced_buy_no,0.99,150,warehouse-flb-v1",
                "favorite_yes_underpriced_buy_yes,0.97,151,warehouse-flb-v1",
            )
        )
        + "\n",
        encoding="utf-8",
    )
    provenance_path = Path(f"{path}.provenance.json")
    provenance_path.write_text(
        json.dumps(
            {
                "artifact_type": "flb_calibration_provenance",
                "generated_by": "scripts/flb_data_feasibility.py",
                "source": "warehouse-csv",
                "generated_at": "2026-06-01T00:00:00+00:00",
                "warehouse_csv_sha256": sha256(
                    b"unit warehouse provenance fixture"
                ).hexdigest(),
                "warehouse_market_count": 301,
                "warehouse_longshot_count": 150,
                "warehouse_favorite_count": 151,
                "calibration_csv_sha256": sha256(path.read_bytes()).hexdigest(),
                "calibration_source_label": "warehouse-flb-v1",
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    return str(path)


def make_live_preflight_artifact_path(
    *,
    prefix: str = "pms-live-preflight-",
    settings: PMSSettings,
    active_strategies_fingerprint: str = "a" * 64,
) -> str:
    root = Path(tempfile.mkdtemp(prefix=prefix))
    root.chmod(0o700)
    if settings.live_execution_model_path is None:
        settings.live_execution_model_path = make_live_execution_model_path(
            prefix=f"{prefix}execution-model-"
        )
    if settings.live_paper_backtest_diff_path is None:
        settings.live_paper_backtest_diff_path = make_live_paper_backtest_diff_path(
            prefix=f"{prefix}paper-backtest-diff-"
        )
    if settings.controller.category_prior_observations_path is None:
        settings.controller.category_prior_observations_path = (
            make_live_category_prior_path(prefix=f"{prefix}category-prior-")
        )
    if settings.strategies.flb_calibration_path is None:
        settings.strategies.flb_calibration_path = make_live_flb_calibration_path(
            prefix=f"{prefix}flb-calibration-"
        )
    artifact_path = root / "credentialed-preflight.json"
    check_names = (
        "live_config",
        "runtime_dependencies",
        "operator_approval",
        "emergency_audit",
        "first_order_audit",
        "database_connection",
        "schema_current",
        "market_data_freshness",
        "submission_unknown",
        "live_open_orders",
        "active_strategies",
        "venue_reconciliation",
    )
    artifact_path.write_text(
        json.dumps(
            {
                "generated_by": "pms-live preflight",
                "generated_at": datetime.now(tz=UTC).isoformat(),
                "artifact_mode": "credentialed_preflight",
                "final_go_no_go_valid": True,
                "skip_venue": False,
                "skip_credentials": False,
                "config_path": "config.live.yaml",
                "database_url_override_used": False,
                "settings_fingerprint": live_preflight_settings_fingerprint(settings),
                "readiness_reports_fingerprint": (
                    live_preflight_readiness_reports_fingerprint(settings)
                ),
                "active_strategies_fingerprint": active_strategies_fingerprint,
                "output_path": str(artifact_path),
                "result": {
                    "ok": True,
                    "checks": [
                        {
                            "name": name,
                            "ok": True,
                            "detail": "test fixture credentialed preflight",
                        }
                        for name in check_names
                    ],
                },
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    return str(artifact_path)
