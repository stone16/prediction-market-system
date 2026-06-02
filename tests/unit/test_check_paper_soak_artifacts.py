from __future__ import annotations

from pathlib import Path

import pytest

from scripts import check_paper_soak_artifacts


def _write_flb_calibration(path: Path) -> None:
    path.write_text(
        "\n".join(
            [
                "signal_name,probability_estimate,sample_count,source_label",
                "longshot_yes_overpriced_buy_no,0.99,150,warehouse-flb-v1",
                "favorite_yes_underpriced_buy_yes,0.97,151,warehouse-flb-v1",
            ]
        )
        + "\n",
        encoding="utf-8",
    )


def _write_category_prior(path: Path, *, rows: int) -> None:
    lines = ["market_id,category,yes_payout,no_payout,resolved_at"]
    for index in range(rows):
        lines.append(
            f"m-{index},politics,{1 if index % 2 == 0 else 0},"
            f"{0 if index % 2 == 0 else 1},2026-01-01T00:00:00Z"
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def test_check_paper_soak_artifacts_fails_when_flb_calibration_missing(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config_path = tmp_path / "live-soak.yaml"
    missing_path = tmp_path / "missing-flb-calibration.csv"
    config_path.write_text(
        "\n".join(
            [
                "mode: paper",
                "paper_soak_strategy_id: h1_flb",
                "strategies:",
                f"  flb_calibration_path: {missing_path}",
                "  flb_min_calibration_samples: 100",
            ]
        ),
        encoding="utf-8",
    )

    exit_code = check_paper_soak_artifacts.main(["--config", str(config_path)])

    captured = capsys.readouterr()
    assert exit_code == 1
    assert "[FAIL] flb_calibration:" in captured.out
    assert f"FLB calibration CSV does not exist: {missing_path}" in captured.out


def test_check_paper_soak_artifacts_passes_with_staged_flb_calibration(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    calibration_path = tmp_path / "flb-calibration.csv"
    _write_flb_calibration(calibration_path)
    config_path = tmp_path / "live-soak.yaml"
    config_path.write_text(
        "\n".join(
            [
                "mode: paper",
                "paper_soak_strategy_id: h1_flb",
                "strategies:",
                f"  flb_calibration_path: {calibration_path}",
                "  flb_min_calibration_samples: 100",
            ]
        ),
        encoding="utf-8",
    )

    exit_code = check_paper_soak_artifacts.main(["--config", str(config_path)])

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "[PASS] paper_mode:" in captured.out
    assert "[PASS] h1_flb_strategy:" in captured.out
    assert "[PASS] flb_calibration:" in captured.out
    assert "[PASS] category_prior:" in captured.out
    assert "not configured" in captured.out


def test_check_paper_soak_artifacts_fails_thin_configured_category_prior(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    calibration_path = tmp_path / "flb-calibration.csv"
    category_prior_path = tmp_path / "category-prior.csv"
    _write_flb_calibration(calibration_path)
    _write_category_prior(category_prior_path, rows=1)
    config_path = tmp_path / "live-soak.yaml"
    config_path.write_text(
        "\n".join(
            [
                "mode: paper",
                "paper_soak_strategy_id: h1_flb",
                "controller:",
                f"  category_prior_observations_path: {category_prior_path}",
                "  category_prior_min_global_samples: 2",
                "strategies:",
                f"  flb_calibration_path: {calibration_path}",
                "  flb_min_calibration_samples: 100",
            ]
        ),
        encoding="utf-8",
    )

    exit_code = check_paper_soak_artifacts.main(["--config", str(config_path)])

    captured = capsys.readouterr()
    assert exit_code == 1
    assert "[FAIL] category_prior:" in captured.out
    assert "loaded 1 resolved rows" in captured.out
    assert "category_prior_min_global_samples=2" in captured.out
