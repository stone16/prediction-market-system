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
                "paper_soak_archive_default: true",
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
    category_prior_path = tmp_path / "category-prior.csv"
    _write_flb_calibration(calibration_path)
    _write_category_prior(category_prior_path, rows=2)
    config_path = tmp_path / "live-soak.yaml"
    config_path.write_text(
        "\n".join(
            [
                "mode: paper",
                "paper_soak_strategy_id: h1_flb",
                "paper_soak_archive_default: true",
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
    assert exit_code == 0
    assert "[PASS] paper_mode:" in captured.out
    assert "[PASS] h1_flb_strategy:" in captured.out
    assert "[PASS] flb_calibration:" in captured.out
    assert "[PASS] category_prior:" in captured.out
    assert "loaded 2 resolved rows" in captured.out


def test_check_paper_soak_artifacts_fails_when_category_prior_missing(
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
                "paper_soak_archive_default: true",
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
    assert "controller.category_prior_observations_path is required" in captured.out


def test_check_paper_soak_artifacts_fails_when_default_strategy_not_archived(
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
    assert exit_code == 1
    assert "[FAIL] h1_flb_strategy:" in captured.out
    assert "paper_soak_archive_default=true" in captured.out


def test_check_paper_soak_artifacts_fails_when_flb_parent_is_permissive(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    artifact_dir = tmp_path / "permissive"
    artifact_dir.mkdir()
    artifact_dir.chmod(0o755)
    calibration_path = artifact_dir / "flb-calibration.csv"
    _write_flb_calibration(calibration_path)
    config_path = tmp_path / "live-soak.yaml"
    config_path.write_text(
        "\n".join(
            [
                "mode: paper",
                "paper_soak_strategy_id: h1_flb",
                "paper_soak_archive_default: true",
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
    assert "[FAIL] flb_calibration:" in captured.out
    assert "parent directory" in captured.out
    assert "too permissive" in captured.out


def test_check_paper_soak_artifacts_fails_when_flb_artifact_is_inside_working_tree(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    repo_dir = tmp_path / "repo"
    repo_dir.mkdir()
    (repo_dir / ".git").mkdir()
    private_dir = repo_dir / "private"
    private_dir.mkdir(mode=0o700)
    calibration_path = private_dir / "flb-calibration.csv"
    _write_flb_calibration(calibration_path)
    outside_dir = tmp_path / "outside"
    outside_dir.mkdir(mode=0o700)
    category_prior_path = outside_dir / "category-prior.csv"
    _write_category_prior(category_prior_path, rows=2)
    config_path = repo_dir / "live-soak.yaml"
    config_path.write_text(
        "\n".join(
            [
                "mode: paper",
                "paper_soak_strategy_id: h1_flb",
                "paper_soak_archive_default: true",
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
    monkeypatch.chdir(repo_dir)

    exit_code = check_paper_soak_artifacts.main(["--config", str(config_path)])

    captured = capsys.readouterr()
    assert exit_code == 1
    assert "[FAIL] flb_calibration:" in captured.out
    assert "outside the working tree" in captured.out


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
                "paper_soak_archive_default: true",
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


def test_check_paper_soak_artifacts_fails_when_category_prior_parent_is_permissive(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    calibration_dir = tmp_path / "private"
    calibration_dir.mkdir(mode=0o700)
    calibration_path = calibration_dir / "flb-calibration.csv"
    _write_flb_calibration(calibration_path)
    category_dir = tmp_path / "permissive"
    category_dir.mkdir()
    category_dir.chmod(0o755)
    category_prior_path = category_dir / "category-prior.csv"
    _write_category_prior(category_prior_path, rows=2)
    config_path = tmp_path / "live-soak.yaml"
    config_path.write_text(
        "\n".join(
            [
                "mode: paper",
                "paper_soak_strategy_id: h1_flb",
                "paper_soak_archive_default: true",
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
    assert "parent directory" in captured.out
    assert "too permissive" in captured.out


def test_check_paper_soak_artifacts_fails_when_category_prior_is_inside_working_tree(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    repo_dir = tmp_path / "repo"
    repo_dir.mkdir()
    (repo_dir / ".git").mkdir()
    private_dir = repo_dir / "private"
    private_dir.mkdir(mode=0o700)
    category_prior_path = private_dir / "category-prior.csv"
    _write_category_prior(category_prior_path, rows=2)
    outside_dir = tmp_path / "outside"
    outside_dir.mkdir(mode=0o700)
    calibration_path = outside_dir / "flb-calibration.csv"
    _write_flb_calibration(calibration_path)
    config_path = repo_dir / "live-soak.yaml"
    config_path.write_text(
        "\n".join(
            [
                "mode: paper",
                "paper_soak_strategy_id: h1_flb",
                "paper_soak_archive_default: true",
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
    monkeypatch.chdir(repo_dir)

    exit_code = check_paper_soak_artifacts.main(["--config", str(config_path)])

    captured = capsys.readouterr()
    assert exit_code == 1
    assert "[FAIL] category_prior:" in captured.out
    assert "outside the working tree" in captured.out
