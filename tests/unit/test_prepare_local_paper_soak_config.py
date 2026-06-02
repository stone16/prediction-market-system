from __future__ import annotations

import stat
from pathlib import Path

import pytest

from scripts import prepare_local_paper_soak_config


def test_prepare_local_paper_soak_config_rewrites_artifact_paths_and_secures_dir(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    source = tmp_path / "config.live-soak.yaml"
    output = tmp_path / "config.local.live-soak.yaml"
    secure_dir = tmp_path / "pms-secure"
    source.write_text(
        "\n".join(
            [
                "mode: paper",
                "controller:",
                "  category_prior_observations_path: null",
                "strategies:",
                "  flb_calibration_path: /secure/pms/flb-calibration.csv",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    exit_code = prepare_local_paper_soak_config.main(
        [
            "--source",
            str(source),
            "--output",
            str(output),
            "--secure-dir",
            str(secure_dir),
        ]
    )

    captured = capsys.readouterr()
    rendered = output.read_text(encoding="utf-8")
    assert exit_code == 0
    assert stat.S_IMODE(secure_dir.stat().st_mode) == 0o700
    assert (
        f'category_prior_observations_path: "{secure_dir}/'
        'category-prior-observations.csv"'
    ) in rendered
    assert (
        f'flb_calibration_path: "{secure_dir}/flb-calibration.csv"'
    ) in rendered
    assert "/secure/pms" not in rendered
    assert f"local paper-soak config written: {output}" in captured.out


def test_prepare_local_paper_soak_config_refuses_to_overwrite_without_flag(
    tmp_path: Path,
) -> None:
    source = tmp_path / "config.live-soak.yaml"
    output = tmp_path / "config.local.live-soak.yaml"
    source.write_text(
        "\n".join(
            [
                "controller:",
                "  category_prior_observations_path: null",
                "strategies:",
                "  flb_calibration_path: /secure/pms/flb-calibration.csv",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    output.write_text("operator local edits\n", encoding="utf-8")

    exit_code = prepare_local_paper_soak_config.main(
        ["--source", str(source), "--output", str(output)]
    )

    assert exit_code == 1
    assert output.read_text(encoding="utf-8") == "operator local edits\n"


def test_prepare_local_paper_soak_config_refuses_symlink_output_with_overwrite(
    tmp_path: Path,
) -> None:
    source = tmp_path / "config.live-soak.yaml"
    target = tmp_path / "target.yaml"
    output = tmp_path / "config.local.live-soak.yaml"
    source.write_text(
        "\n".join(
            [
                "controller:",
                "  category_prior_observations_path: null",
                "strategies:",
                "  flb_calibration_path: /secure/pms/flb-calibration.csv",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    target.write_text("do not mutate\n", encoding="utf-8")
    output.symlink_to(target)

    exit_code = prepare_local_paper_soak_config.main(
        [
            "--source",
            str(source),
            "--output",
            str(output),
            "--secure-dir",
            str(tmp_path / "secure"),
            "--overwrite",
        ]
    )

    assert exit_code == 1
    assert target.read_text(encoding="utf-8") == "do not mutate\n"
    assert output.is_symlink()


def test_prepare_local_paper_soak_config_requires_expected_source_paths(
    tmp_path: Path,
) -> None:
    source = tmp_path / "config.live-soak.yaml"
    output = tmp_path / "config.local.live-soak.yaml"
    source.write_text(
        "\n".join(
            [
                "controller:",
                "  category_prior_observations_path: null",
                "strategies:",
                "  flb_calibration_path: null",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    exit_code = prepare_local_paper_soak_config.main(
        ["--source", str(source), "--output", str(output)]
    )

    assert exit_code == 1
    assert not output.exists()


def test_prepare_local_paper_soak_config_can_write_paper_canary_plumbing_config(
    tmp_path: Path,
) -> None:
    source = tmp_path / "config.live-soak.yaml"
    output = tmp_path / "config.local.paper-canary.yaml"
    secure_dir = tmp_path / "pms-secure"
    source.write_text(
        "\n".join(
            [
                "mode: paper",
                "paper_soak_strategy_id: h1_flb",
                "paper_soak_archive_default: true",
                "controller:",
                "  category_prior_observations_path: null",
                "strategies:",
                "  flb_calibration_path: /secure/pms/flb-calibration.csv",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    exit_code = prepare_local_paper_soak_config.main(
        [
            "--source",
            str(source),
            "--output",
            str(output),
            "--secure-dir",
            str(secure_dir),
            "--paper-canary",
        ]
    )

    rendered = output.read_text(encoding="utf-8")
    assert exit_code == 0
    assert "paper_soak_strategy_id: null" in rendered
    assert "paper_soak_archive_default: false" in rendered
    assert "category_prior_observations_path: null" in rendered
    assert "flb_calibration_path: null" in rendered
    assert "/secure/pms" not in rendered
