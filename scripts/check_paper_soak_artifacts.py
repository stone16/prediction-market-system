"""Validate PAPER-soak launch artifacts before starting ``pms-api``.

This helper does not generate artifacts and does not relax runtime gates. It
loads the same config and artifact parsers used by the runner so operators can
fail fast before the API process exits during ``Runner(...)`` construction.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Sequence

from pms.config import PMSSettings
from pms.controller.baselines import load_category_prior_observations_csv
from pms.core.enums import RunMode
from pms.strategies.flb.source import load_flb_calibration_csv


@dataclass(frozen=True, slots=True)
class PaperSoakArtifactCheck:
    name: str
    passed: bool
    detail: str


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
    if settings.paper_soak_strategy_id == "h1_flb":
        return PaperSoakArtifactCheck(
            "h1_flb_strategy",
            True,
            "paper_soak_strategy_id=h1_flb",
        )
    return PaperSoakArtifactCheck(
        "h1_flb_strategy",
        False,
        "paper_soak_strategy_id must be h1_flb for the launch soak: "
        f"{settings.paper_soak_strategy_id}",
    )


def _check_flb_calibration(settings: PMSSettings) -> PaperSoakArtifactCheck:
    raw_path = settings.strategies.flb_calibration_path
    if raw_path is None or raw_path.strip() == "":
        return PaperSoakArtifactCheck(
            "flb_calibration",
            False,
            "strategies.flb_calibration_path is required for h1_flb paper soak",
        )
    try:
        model = load_flb_calibration_csv(
            raw_path,
            min_sample_count=settings.strategies.flb_min_calibration_samples,
        )
    except (OSError, ValueError) as exc:
        return PaperSoakArtifactCheck("flb_calibration", False, str(exc))
    source_labels = sorted({row.source_label for row in model.calibrations})
    return PaperSoakArtifactCheck(
        "flb_calibration",
        True,
        (
            f"loaded {len(model.calibrations)} signals from {Path(raw_path).expanduser()} "
            f"(min_sample_count={model.min_sample_count}, "
            f"source_labels={','.join(source_labels)})"
        ),
    )


def _check_category_prior(settings: PMSSettings) -> PaperSoakArtifactCheck:
    raw_path = settings.controller.category_prior_observations_path
    if raw_path is None or raw_path.strip() == "":
        return PaperSoakArtifactCheck(
            "category_prior",
            True,
            "not configured; skipped for PAPER soak startup",
        )
    try:
        loaded = load_category_prior_observations_csv(raw_path)
    except (OSError, ValueError) as exc:
        return PaperSoakArtifactCheck("category_prior", False, str(exc))
    observation_count = len(loaded.observations)
    minimum = settings.controller.category_prior_min_global_samples
    if observation_count < minimum:
        return PaperSoakArtifactCheck(
            "category_prior",
            False,
            (
                f"loaded {observation_count} resolved rows from "
                f"{Path(raw_path).expanduser()}; requires "
                f"category_prior_min_global_samples={minimum}"
            ),
        )
    return PaperSoakArtifactCheck(
        "category_prior",
        True,
        (
            f"loaded {observation_count} resolved rows from "
            f"{Path(raw_path).expanduser()} "
            f"(skipped_ambiguous_count={loaded.skipped_ambiguous_count})"
        ),
    )


def _format_plain(checks: Sequence[PaperSoakArtifactCheck]) -> str:
    lines: list[str] = []
    for check in checks:
        status = "PASS" if check.passed else "FAIL"
        lines.append(f"[{status}] {check.name}: {check.detail}")
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


if __name__ == "__main__":
    sys.exit(main())
