from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts import check_live_submission_artifacts
from tests.support.live_paths import (
    make_live_category_prior_path,
    make_live_execution_model_path,
    make_live_flb_calibration_path,
    make_live_paper_backtest_diff_path,
    make_live_report_paths,
)


def test_check_live_submission_artifacts_reports_all_missing_paths(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config_path = tmp_path / "live-submission.yaml"
    config_path.write_text("mode: paper\n", encoding="utf-8")

    exit_code = check_live_submission_artifacts.main(
        ["--config", str(config_path), "--json"]
    )

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    checks_by_name = {check["name"]: check for check in payload["checks"]}
    assert exit_code == 1
    assert payload["ok"] is False
    assert set(checks_by_name) == {
        "paper_soak_go_report",
        "operator_rehearsal_report",
        "execution_model",
        "paper_backtest_diff",
        "category_prior",
        "flb_calibration",
    }
    for name, check in checks_by_name.items():
        assert check["passed"] is False, name
        assert check["remediation"]
    assert "live_paper_soak_report_path" in checks_by_name[
        "paper_soak_go_report"
    ]["detail"]
    assert "live_execution_model_path" in checks_by_name["execution_model"]["detail"]
    assert "controller.category_prior_observations_path" in checks_by_name[
        "category_prior"
    ]["detail"]
    assert "scripts/export_paper_execution_from_api.py" in checks_by_name[
        "execution_model"
    ]["remediation"]
    assert "scripts/export_paper_execution_from_api.py" in checks_by_name[
        "paper_backtest_diff"
    ]["remediation"]
    assert "scripts/export_backtest_execution_from_db.py" in checks_by_name[
        "paper_backtest_diff"
    ]["remediation"]
    assert "completed matching research backtest" in checks_by_name[
        "paper_backtest_diff"
    ]["remediation"]


def test_check_live_submission_artifacts_passes_with_private_valid_artifacts(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    paper_report_path, rehearsal_report_path = make_live_report_paths(
        prefix="pms-live-submission-reports-"
    )
    execution_model_path = make_live_execution_model_path(
        prefix="pms-live-submission-execution-model-"
    )
    paper_backtest_diff_path = make_live_paper_backtest_diff_path(
        prefix="pms-live-submission-paper-backtest-diff-"
    )
    category_prior_path = make_live_category_prior_path(
        prefix="pms-live-submission-category-prior-"
    )
    flb_calibration_path = make_live_flb_calibration_path(
        prefix="pms-live-submission-flb-calibration-"
    )
    config_path = tmp_path / "live-submission.yaml"
    config_path.write_text(
        "\n".join(
            [
                "mode: paper",
                f"live_paper_soak_report_path: {paper_report_path}",
                f"live_operator_rehearsal_report_path: {rehearsal_report_path}",
                f"live_execution_model_path: {execution_model_path}",
                f"live_paper_backtest_diff_path: {paper_backtest_diff_path}",
                "controller:",
                f"  category_prior_observations_path: {category_prior_path}",
                "  category_prior_min_global_samples: 100",
                "strategies:",
                f"  flb_calibration_path: {flb_calibration_path}",
                "  flb_min_calibration_samples: 100",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    exit_code = check_live_submission_artifacts.main(
        ["--config", str(config_path), "--json"]
    )

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert exit_code == 0
    assert payload["ok"] is True
    assert {check["name"] for check in payload["checks"]} == {
        "paper_soak_go_report",
        "operator_rehearsal_report",
        "execution_model",
        "paper_backtest_diff",
        "category_prior",
        "flb_calibration",
    }
    assert all(check["passed"] for check in payload["checks"])
    assert all(check["remediation"] is None for check in payload["checks"])
