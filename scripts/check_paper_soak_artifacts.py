"""Validate PAPER-soak launch artifacts before starting ``pms-api``.

This helper does not generate artifacts and does not relax runtime gates. It
loads the same config and artifact parsers used by the runner so operators can
fail fast before the API process exits during ``Runner(...)`` construction.
"""

from __future__ import annotations

import argparse
import json
import shlex
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Sequence

from scripts.artifact_path_safety import (
    require_path_outside_working_tree,
    require_private_parent,
)
from pms.config import PMSSettings
from pms.controller.baselines import load_category_prior_observations_csv
from pms.core.enums import RunMode
from pms.strategies.flb.artifacts import (
    file_sha256_no_follow,
    flb_calibration_provenance_path,
    load_flb_calibration_provenance_json,
)
from pms.strategies.flb.source import load_flb_calibration_csv


@dataclass(frozen=True, slots=True)
class PaperSoakArtifactCheck:
    name: str
    passed: bool
    detail: str
    remediation: str | None = None


def check_paper_soak_artifacts(settings: PMSSettings) -> tuple[PaperSoakArtifactCheck, ...]:
    """Return machine-checkable PAPER-soak artifact readiness checks."""
    checks: list[PaperSoakArtifactCheck] = [
        _check_paper_mode(settings),
        _check_h1_flb_strategy(settings),
        _check_flb_calibration(settings),
        _check_category_prior(settings),
    ]
    return tuple(checks)


def _check_paper_mode(settings: PMSSettings) -> PaperSoakArtifactCheck:
    if settings.mode is RunMode.PAPER:
        return PaperSoakArtifactCheck("paper_mode", True, "mode=paper")
    return PaperSoakArtifactCheck(
        "paper_mode",
        False,
        f"mode must be paper for launch soak artifact validation: {settings.mode.value}",
    )


def _check_h1_flb_strategy(settings: PMSSettings) -> PaperSoakArtifactCheck:
    if settings.paper_soak_strategy_id != "h1_flb":
        return PaperSoakArtifactCheck(
            "h1_flb_strategy",
            False,
            "paper_soak_strategy_id must be h1_flb for the launch soak: "
            f"{settings.paper_soak_strategy_id}",
        )
    if not settings.paper_soak_archive_default:
        return PaperSoakArtifactCheck(
            "h1_flb_strategy",
            False,
            (
                "paper_soak_archive_default=true is required so the launch "
                "soak does not run the legacy default strategy beside h1_flb"
            ),
        )
    return PaperSoakArtifactCheck(
        "h1_flb_strategy",
        True,
        "paper_soak_strategy_id=h1_flb; paper_soak_archive_default=true",
    )


def _check_flb_calibration(settings: PMSSettings) -> PaperSoakArtifactCheck:
    raw_path = settings.strategies.flb_calibration_path
    if raw_path is None or raw_path.strip() == "":
        return PaperSoakArtifactCheck(
            "flb_calibration",
            False,
            "strategies.flb_calibration_path is required for h1_flb paper soak",
            _flb_calibration_remediation(None),
        )
    path = Path(raw_path).expanduser()
    try:
        require_path_outside_working_tree(path, label="FLB calibration artifact")
        require_private_parent(path, label="FLB calibration artifact")
        model = load_flb_calibration_csv(
            path,
            min_sample_count=settings.strategies.flb_min_calibration_samples,
        )
        calibration_sha256 = file_sha256_no_follow(
            path,
            label="FLB calibration artifact",
        )
        provenance_path = flb_calibration_provenance_path(path)
        require_path_outside_working_tree(
            provenance_path,
            label="FLB calibration provenance JSON",
        )
        require_private_parent(
            provenance_path,
            label="FLB calibration provenance JSON",
        )
        load_flb_calibration_provenance_json(
            provenance_path,
            calibration_csv_sha256=calibration_sha256,
            source_labels=tuple(row.source_label for row in model.calibrations),
            signal_sample_counts={
                row.signal_name: row.sample_count for row in model.calibrations
            },
            min_sample_count=model.min_sample_count,
        )
    except (OSError, ValueError) as exc:
        return PaperSoakArtifactCheck(
            "flb_calibration",
            False,
            str(exc),
            _flb_calibration_remediation(path),
        )
    source_labels = sorted({row.source_label for row in model.calibrations})
    return PaperSoakArtifactCheck(
        "flb_calibration",
        True,
        (
            f"loaded {len(model.calibrations)} signals from {path} "
            f"(min_sample_count={model.min_sample_count}, "
            f"source_labels={','.join(source_labels)})"
        ),
    )


def _check_category_prior(settings: PMSSettings) -> PaperSoakArtifactCheck:
    raw_path = settings.controller.category_prior_observations_path
    if raw_path is None or raw_path.strip() == "":
        return PaperSoakArtifactCheck(
            "category_prior",
            False,
            (
                "controller.category_prior_observations_path is required "
                "for the h1_flb launch paper soak"
            ),
            _category_prior_remediation(None, minimum=None),
        )
    path = Path(raw_path).expanduser()
    try:
        require_path_outside_working_tree(path, label="category-prior artifact")
        require_private_parent(path, label="category-prior artifact")
        loaded = load_category_prior_observations_csv(path)
    except (OSError, ValueError) as exc:
        return PaperSoakArtifactCheck(
            "category_prior",
            False,
            str(exc),
            _category_prior_remediation(
                path,
                minimum=settings.controller.category_prior_min_global_samples,
            ),
        )
    observation_count = len(loaded.observations)
    minimum = settings.controller.category_prior_min_global_samples
    if observation_count < minimum:
        return PaperSoakArtifactCheck(
            "category_prior",
            False,
            (
                f"loaded {observation_count} resolved rows from "
                f"{path}; requires "
                f"category_prior_min_global_samples={minimum}"
            ),
            _category_prior_remediation(path, minimum=minimum),
        )
    return PaperSoakArtifactCheck(
        "category_prior",
        True,
        (
            f"loaded {observation_count} resolved rows from "
            f"{path} "
            f"(skipped_ambiguous_count={loaded.skipped_ambiguous_count})"
        ),
    )


def _format_plain(checks: Sequence[PaperSoakArtifactCheck]) -> str:
    lines: list[str] = []
    for check in checks:
        status = "PASS" if check.passed else "FAIL"
        lines.append(f"[{status}] {check.name}: {check.detail}")
        if check.remediation is not None:
            lines.append(f"  next: {check.remediation}")
    return "\n".join(lines)


def _format_json(checks: Sequence[PaperSoakArtifactCheck]) -> str:
    payload = {
        "ok": all(check.passed for check in checks),
        "checks": [asdict(check) for check in checks],
    }
    return json.dumps(payload, allow_nan=False, indent=2, sort_keys=True)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Validate PAPER-soak launch artifacts before pms-api startup."
    )
    parser.add_argument(
        "--config",
        default="config.live-soak.yaml",
        help="PAPER soak config path. Defaults to config.live-soak.yaml.",
    )
    parser.add_argument("--json", action="store_true", help="Emit JSON output.")
    args = parser.parse_args(argv)

    try:
        settings = PMSSettings.load(cast_str(args.config))
    except Exception as exc:  # noqa: BLE001
        check = PaperSoakArtifactCheck("config_load", False, str(exc))
        output = _format_json((check,)) if args.json else _format_plain((check,))
        print(output)
        return 1

    checks = check_paper_soak_artifacts(settings)
    output = _format_json(checks) if args.json else _format_plain(checks)
    print(output)
    return 0 if all(check.passed for check in checks) else 1


def cast_str(value: object) -> str:
    if isinstance(value, str):
        return value
    return str(value)


def _flb_calibration_remediation(path: Path | None) -> str:
    if path is None:
        return (
            "configure strategies.flb_calibration_path to a private artifact "
            "path, load DUNE_API_KEY from the operator secret store, then run "
            "scripts/export_flb_warehouse_from_dune.py followed by "
            "scripts/flb_data_feasibility.py --source warehouse-csv with "
            "--calibration-csv and --calibration-provenance-json"
        )
    warehouse_path = path.parent / "polymarket_resolved_binary.csv"
    report_path = path.parent / "flb-feasibility.md"
    decile_path = path.parent / "flb-deciles.csv"
    provenance_path = flb_calibration_provenance_path(path)
    return (
        "load DUNE_API_KEY from the operator secret store, then run: "
        "uv run python scripts/export_flb_warehouse_from_dune.py "
        f"--output {_shell_quote(warehouse_path)} --performance large; "
        "uv run python scripts/flb_data_feasibility.py --source warehouse-csv "
        f"--input {_shell_quote(warehouse_path)} "
        f"--output {_shell_quote(report_path)} "
        f"--csv {_shell_quote(decile_path)} "
        f"--calibration-csv {_shell_quote(path)} "
        "--calibration-source-label warehouse-flb-v1 "
        f"--calibration-provenance-json {_shell_quote(provenance_path)}"
    )


def _category_prior_remediation(path: Path | None, *, minimum: int | None) -> str:
    minimum_value = minimum if minimum is not None else 100
    if path is None:
        return (
            "configure controller.category_prior_observations_path to a private "
            "artifact path, then run scripts/export_category_prior_observations.py "
            f"--output <configured-path> --min-observations {minimum_value}"
        )
    return (
        "run: uv run python scripts/export_category_prior_observations.py "
        f"--output {_shell_quote(path)} --min-observations {minimum_value}"
    )


def _shell_quote(path: Path) -> str:
    return shlex.quote(str(path))


if __name__ == "__main__":
    sys.exit(main())
