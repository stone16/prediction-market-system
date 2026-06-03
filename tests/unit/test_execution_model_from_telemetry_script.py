from __future__ import annotations

import csv
import json
import os
import stat
from collections.abc import Callable
from pathlib import Path
from types import TracebackType
from typing import IO, cast

import pytest

from scripts.execution_model_from_telemetry import (
    build_execution_model_from_telemetry_csv,
    main,
    save_execution_model_json,
)
from pms.research.specs import ExecutionModel


TELEMETRY_COLUMNS = [
    "slippage_bps",
    "latency_ms",
    "adverse_selection_bps",
]


class _FailingTextWriter:
    def __init__(self, wrapped: IO[str]) -> None:
        self._wrapped = wrapped

    def __enter__(self) -> "_FailingTextWriter":
        self._wrapped.__enter__()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool | None:
        return self._wrapped.__exit__(exc_type, exc, traceback)

    def write(self, content: str) -> int:
        self._wrapped.write(content)
        raise OSError("simulated write failure")


def _patch_text_artifact_writes_to_fail(monkeypatch: pytest.MonkeyPatch) -> None:
    real_fdopen = os.fdopen

    def failing_fdopen(
        fd: int,
        mode: str = "r",
        buffering: int = -1,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
        closefd: bool = True,
        opener: Callable[[str, int], int] | None = None,
    ) -> object:
        file = real_fdopen(
            fd,
            mode,
            buffering,
            encoding,
            errors,
            newline,
            closefd,
            opener,
        )
        if "w" in mode:
            return _FailingTextWriter(cast(IO[str], file))
        return file

    monkeypatch.setattr(os, "fdopen", failing_fdopen)


def _write_telemetry_csv(path: Path, rows: list[dict[str, str]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=TELEMETRY_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def _write_csv(
    path: Path,
    *,
    fieldnames: list[str],
    rows: list[dict[str, str]],
) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def test_build_execution_model_from_telemetry_csv_writes_profile(
    tmp_path: Path,
) -> None:
    telemetry_path = tmp_path / "paper-execution-telemetry.csv"
    _write_telemetry_csv(
        telemetry_path,
        [
            {
                "slippage_bps": "2",
                "latency_ms": "101",
                "adverse_selection_bps": "1",
            },
            {
                "slippage_bps": "12",
                "latency_ms": "160",
                "adverse_selection_bps": "4",
            },
            {
                "slippage_bps": "8",
                "latency_ms": "240",
                "adverse_selection_bps": "2",
            },
            {
                "slippage_bps": "4",
                "latency_ms": "360",
                "adverse_selection_bps": "9",
            },
            {
                "slippage_bps": "6",
                "latency_ms": "500",
                "adverse_selection_bps": "6",
            },
        ],
    )

    model = build_execution_model_from_telemetry_csv(
        telemetry_path,
        fee_rate=0.04,
        staleness_ms=120_000.0,
        displayed_depth_fill_ratio=0.75,
        require_adverse_selection=True,
    )
    output_path = tmp_path / "execution-model.json"
    save_execution_model_json(model, output_path)

    payload = json.loads(output_path.read_text(encoding="utf-8"))
    generated_at = payload.pop("generated_at")
    assert isinstance(generated_at, str)
    assert payload == {
        "generated_by": "scripts/execution_model_from_telemetry.py",
        "artifact_mode": "telemetry_execution_model",
        "fee_rate": pytest.approx(0.04),
        "slippage_bps": pytest.approx(6.0),
        "latency_ms": pytest.approx(500.0),
        "staleness_ms": pytest.approx(120_000.0),
        "fill_policy": "immediate_or_cancel",
        "displayed_depth_fill_ratio": pytest.approx(0.75),
        "adverse_selection_bps": pytest.approx(9.0),
        "order_ttl_ms": 60_000,
        "price_invalidation_streak": 10,
        "replay_window_ms": 86_400_000,
        "calibration_source": "telemetry_calibrated",
    }


def test_save_execution_model_json_rejects_non_strict_json_numbers(
    tmp_path: Path,
) -> None:
    output_path = tmp_path / "execution-model.json"

    with pytest.raises(ValueError, match="JSON"):
        save_execution_model_json(ExecutionModel.polymarket_paper(), output_path)

    assert not output_path.exists()


def test_build_execution_model_from_telemetry_csv_rejects_missing_columns(
    tmp_path: Path,
) -> None:
    telemetry_path = tmp_path / "missing-latency.csv"
    _write_csv(
        telemetry_path,
        fieldnames=["slippage_bps"],
        rows=[{"slippage_bps": "6"}],
    )

    with pytest.raises(ValueError, match="missing required columns: latency_ms"):
        build_execution_model_from_telemetry_csv(
            telemetry_path,
            fee_rate=0.04,
            staleness_ms=120_000.0,
        )


def test_build_execution_model_from_telemetry_csv_rejects_duplicate_header(
    tmp_path: Path,
) -> None:
    telemetry_path = tmp_path / "duplicate-header.csv"
    telemetry_path.write_text(
        "\n".join(
            (
                "slippage_bps,latency_ms,latency_ms,adverse_selection_bps",
                "2,101,999,1",
            )
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="duplicate CSV column: latency_ms"):
        build_execution_model_from_telemetry_csv(
            telemetry_path,
            fee_rate=0.04,
            staleness_ms=120_000.0,
        )


def test_build_execution_model_from_telemetry_csv_rejects_symlink_input(
    tmp_path: Path,
) -> None:
    target_path = tmp_path / "target-telemetry.csv"
    _write_telemetry_csv(
        target_path,
        [
            {
                "slippage_bps": "2",
                "latency_ms": "101",
                "adverse_selection_bps": "1",
            }
        ],
    )
    telemetry_path = tmp_path / "paper-execution-telemetry.csv"
    telemetry_path.symlink_to(target_path)

    with pytest.raises(ValueError, match="cannot be read safely"):
        build_execution_model_from_telemetry_csv(
            telemetry_path,
            fee_rate=0.04,
            staleness_ms=120_000.0,
        )


def test_build_execution_model_from_telemetry_csv_opens_input_with_no_follow(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    no_follow_flag = getattr(os, "O_NOFOLLOW", 0)
    if no_follow_flag == 0:
        pytest.skip("os.O_NOFOLLOW is unavailable on this platform")

    telemetry_path = tmp_path / "paper-execution-telemetry.csv"
    _write_telemetry_csv(
        telemetry_path,
        [
            {
                "slippage_bps": "2",
                "latency_ms": "101",
                "adverse_selection_bps": "1",
            }
        ],
    )
    observed: list[tuple[Path, int]] = []
    real_open = os.open

    def recording_open(
        path_arg: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        flags: int,
        mode: int = 0o777,
    ) -> int:
        observed.append((Path(os.fsdecode(os.fspath(path_arg))), flags))
        return real_open(path_arg, flags, mode)

    monkeypatch.setattr(os, "open", recording_open)

    model = build_execution_model_from_telemetry_csv(
        telemetry_path,
        fee_rate=0.04,
        staleness_ms=120_000.0,
    )

    observed_by_path = {path: flags for path, flags in observed}
    assert model.slippage_bps == pytest.approx(2.0)
    assert observed_by_path[telemetry_path] & no_follow_flag


def test_build_execution_model_from_telemetry_csv_rejects_hardlink_swap_during_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    telemetry_path = tmp_path / "paper-execution-telemetry.csv"
    _write_telemetry_csv(
        telemetry_path,
        [
            {
                "slippage_bps": "2",
                "latency_ms": "101",
                "adverse_selection_bps": "1",
            }
        ],
    )
    replacement_source = tmp_path / "replacement-telemetry.csv"
    _write_telemetry_csv(
        replacement_source,
        [
            {
                "slippage_bps": "4",
                "latency_ms": "202",
                "adverse_selection_bps": "2",
            }
        ],
    )
    real_open = os.open
    swapped = False

    def swapping_open(
        path_arg: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        flags: int,
        mode: int = 0o777,
    ) -> int:
        nonlocal swapped
        observed_path = Path(os.fsdecode(os.fspath(path_arg)))
        if observed_path == telemetry_path and not swapped:
            swapped = True
            telemetry_path.unlink()
            os.link(replacement_source, telemetry_path)
        return real_open(path_arg, flags, mode)

    monkeypatch.setattr(os, "open", swapping_open)

    with pytest.raises(ValueError, match="cannot be read safely"):
        build_execution_model_from_telemetry_csv(
            telemetry_path,
            fee_rate=0.04,
            staleness_ms=120_000.0,
        )

    assert swapped is True


def test_build_execution_model_from_telemetry_csv_requires_adverse_selection(
    tmp_path: Path,
) -> None:
    telemetry_path = tmp_path / "without-adverse-selection.csv"
    _write_csv(
        telemetry_path,
        fieldnames=["slippage_bps", "latency_ms"],
        rows=[{"slippage_bps": "6", "latency_ms": "120"}],
    )

    with pytest.raises(ValueError, match="adverse_selection_bps samples"):
        build_execution_model_from_telemetry_csv(
            telemetry_path,
            fee_rate=0.04,
            staleness_ms=120_000.0,
            require_adverse_selection=True,
        )


def test_build_execution_model_from_telemetry_csv_rejects_too_few_samples(
    tmp_path: Path,
) -> None:
    telemetry_path = tmp_path / "thin-sample.csv"
    _write_telemetry_csv(
        telemetry_path,
        [
            {
                "slippage_bps": "2",
                "latency_ms": "101",
                "adverse_selection_bps": "1",
            },
            {
                "slippage_bps": "12",
                "latency_ms": "500",
                "adverse_selection_bps": "9",
            },
        ],
    )

    with pytest.raises(ValueError, match="at least 3 telemetry samples"):
        build_execution_model_from_telemetry_csv(
            telemetry_path,
            fee_rate=0.04,
            staleness_ms=120_000.0,
            min_samples=3,
        )


def test_build_execution_model_from_telemetry_csv_requires_enough_adverse_samples(
    tmp_path: Path,
) -> None:
    telemetry_path = tmp_path / "thin-adverse-sample.csv"
    _write_telemetry_csv(
        telemetry_path,
        [
            {
                "slippage_bps": "2",
                "latency_ms": "101",
                "adverse_selection_bps": "1",
            },
            {
                "slippage_bps": "12",
                "latency_ms": "500",
                "adverse_selection_bps": "",
            },
            {
                "slippage_bps": "8",
                "latency_ms": "240",
                "adverse_selection_bps": "",
            },
        ],
    )

    with pytest.raises(ValueError, match="at least 3 adverse_selection_bps samples"):
        build_execution_model_from_telemetry_csv(
            telemetry_path,
            fee_rate=0.04,
            staleness_ms=120_000.0,
            require_adverse_selection=True,
            min_samples=3,
        )


def test_cli_writes_execution_model_artifact(tmp_path: Path) -> None:
    telemetry_path = tmp_path / "paper-execution-telemetry.csv"
    _write_telemetry_csv(
        telemetry_path,
        [
            {
                "slippage_bps": "2",
                "latency_ms": "101",
                "adverse_selection_bps": "1",
            },
            {
                "slippage_bps": "12",
                "latency_ms": "500",
                "adverse_selection_bps": "9",
            },
        ],
    )
    output_path = tmp_path / "execution-model.json"

    exit_code = main(
        [
            "--input",
            str(telemetry_path),
            "--output",
            str(output_path),
            "--fee-rate",
            "0.04",
            "--staleness-ms",
            "120000",
            "--displayed-depth-fill-ratio",
            "0.75",
            "--require-adverse-selection",
            "--min-samples",
            "2",
        ]
    )

    assert exit_code == 0
    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["generated_by"] == "scripts/execution_model_from_telemetry.py"
    assert payload["artifact_mode"] == "telemetry_execution_model"
    assert isinstance(payload["generated_at"], str)
    assert payload["calibration_source"] == "telemetry_calibrated"
    assert payload["adverse_selection_bps"] == pytest.approx(9.0)
    assert payload["min_samples"] == 2
    assert payload["telemetry_sample_count"] == 2
    assert payload["adverse_selection_sample_count"] == 2
    assert payload["require_adverse_selection"] is True


def test_cli_writes_execution_model_strategy_evidence(tmp_path: Path) -> None:
    telemetry_path = tmp_path / "paper-execution-telemetry.csv"
    _write_telemetry_csv(
        telemetry_path,
        [
            {
                "slippage_bps": "2",
                "latency_ms": "101",
                "adverse_selection_bps": "1",
            },
            {
                "slippage_bps": "12",
                "latency_ms": "500",
                "adverse_selection_bps": "9",
            },
        ],
    )
    output_path = tmp_path / "execution-model.json"

    exit_code = main(
        [
            "--input",
            str(telemetry_path),
            "--output",
            str(output_path),
            "--fee-rate",
            "0.04",
            "--staleness-ms",
            "120000",
            "--require-adverse-selection",
            "--min-samples",
            "2",
            "--strategy-id",
            "h1_flb",
            "--strategy-version-id",
            "h1-flb-v1",
        ]
    )

    assert exit_code == 0
    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["strategy_evidence"] == "h1_flb@h1-flb-v1"


def test_save_execution_model_json_creates_output_parent_private(
    tmp_path: Path,
) -> None:
    telemetry_path = tmp_path / "paper-execution-telemetry.csv"
    _write_telemetry_csv(
        telemetry_path,
        [
            {
                "slippage_bps": "2",
                "latency_ms": "101",
                "adverse_selection_bps": "1",
            }
        ],
    )
    model = build_execution_model_from_telemetry_csv(
        telemetry_path,
        fee_rate=0.04,
        staleness_ms=120_000.0,
    )
    output_dir = tmp_path / "artifacts"

    save_execution_model_json(model, output_dir / "execution-model.json")

    assert stat.S_IMODE(output_dir.stat().st_mode) == 0o700


def test_save_execution_model_json_refuses_permissive_output_parent(
    tmp_path: Path,
) -> None:
    telemetry_path = tmp_path / "paper-execution-telemetry.csv"
    _write_telemetry_csv(
        telemetry_path,
        [
            {
                "slippage_bps": "2",
                "latency_ms": "101",
                "adverse_selection_bps": "1",
            }
        ],
    )
    model = build_execution_model_from_telemetry_csv(
        telemetry_path,
        fee_rate=0.04,
        staleness_ms=120_000.0,
    )
    output_dir = tmp_path / "shared-artifacts"
    output_dir.mkdir(mode=0o700)
    output_dir.chmod(0o755)

    try:
        with pytest.raises(OSError, match="execution model output parent"):
            save_execution_model_json(model, output_dir / "execution-model.json")
    finally:
        output_dir.chmod(0o700)

    assert not (output_dir / "execution-model.json").exists()


def test_cli_returns_operator_error_for_permissive_output_parent(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    telemetry_path = tmp_path / "paper-execution-telemetry.csv"
    _write_telemetry_csv(
        telemetry_path,
        [
            {
                "slippage_bps": "2",
                "latency_ms": "101",
                "adverse_selection_bps": "1",
            }
        ],
    )
    output_dir = tmp_path / "shared-artifacts"
    output_dir.mkdir(mode=0o700)
    output_dir.chmod(0o755)
    output_path = output_dir / "execution-model.json"

    try:
        exit_code = main(
            [
                "--input",
                str(telemetry_path),
                "--output",
                str(output_path),
                "--fee-rate",
                "0.04",
                "--staleness-ms",
                "120000",
            ]
        )
        captured = capsys.readouterr()
    finally:
        output_dir.chmod(0o700)

    assert exit_code == 2
    assert "execution model output parent" in captured.err
    assert "too permissive" in captured.err
    assert not output_path.exists()


def test_cli_rejects_output_reusing_input_path(tmp_path: Path) -> None:
    telemetry_path = tmp_path / "paper-execution-telemetry.csv"
    _write_telemetry_csv(
        telemetry_path,
        [
            {
                "slippage_bps": "2",
                "latency_ms": "101",
                "adverse_selection_bps": "1",
            }
        ],
    )
    original_csv = telemetry_path.read_text(encoding="utf-8")

    exit_code = main(
        [
            "--input",
            str(telemetry_path),
            "--output",
            str(telemetry_path),
            "--fee-rate",
            "0.04",
            "--staleness-ms",
            "120000",
        ]
    )

    assert exit_code == 2
    assert telemetry_path.read_text(encoding="utf-8") == original_csv


def test_save_execution_model_json_preserves_existing_output_when_write_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    telemetry_path = tmp_path / "paper-execution-telemetry.csv"
    _write_telemetry_csv(
        telemetry_path,
        [
            {
                "slippage_bps": "2",
                "latency_ms": "101",
                "adverse_selection_bps": "1",
            }
        ],
    )
    model = build_execution_model_from_telemetry_csv(
        telemetry_path,
        fee_rate=0.04,
        staleness_ms=120_000.0,
    )
    output_path = tmp_path / "execution-model.json"
    output_path.write_text("old execution model\n", encoding="utf-8")
    _patch_text_artifact_writes_to_fail(monkeypatch)

    with pytest.raises(OSError, match="simulated write failure"):
        save_execution_model_json(model, output_path)

    assert output_path.read_text(encoding="utf-8") == "old execution model\n"


def test_save_execution_model_json_does_not_publish_new_output_when_write_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    telemetry_path = tmp_path / "paper-execution-telemetry.csv"
    _write_telemetry_csv(
        telemetry_path,
        [
            {
                "slippage_bps": "2",
                "latency_ms": "101",
                "adverse_selection_bps": "1",
            }
        ],
    )
    model = build_execution_model_from_telemetry_csv(
        telemetry_path,
        fee_rate=0.04,
        staleness_ms=120_000.0,
    )
    output_path = tmp_path / "execution-model.json"
    _patch_text_artifact_writes_to_fail(monkeypatch)

    with pytest.raises(OSError, match="simulated write failure"):
        save_execution_model_json(model, output_path)

    assert not output_path.exists()


def test_save_execution_model_json_atomic_publish_does_not_mutate_hardlink_swap_target(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    telemetry_path = tmp_path / "paper-execution-telemetry.csv"
    _write_telemetry_csv(
        telemetry_path,
        [
            {
                "slippage_bps": "2",
                "latency_ms": "101",
                "adverse_selection_bps": "1",
            }
        ],
    )
    model = build_execution_model_from_telemetry_csv(
        telemetry_path,
        fee_rate=0.04,
        staleness_ms=120_000.0,
    )
    target_path = tmp_path / "target-execution-model.json"
    target_path.write_text("target must not be overwritten\n", encoding="utf-8")
    output_path = tmp_path / "execution-model.json"
    output_path.write_text("old execution model\n", encoding="utf-8")
    real_replace = os.replace
    swapped = False

    def swapping_replace(
        src: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        dst: str | bytes | os.PathLike[str] | os.PathLike[bytes],
    ) -> None:
        nonlocal swapped
        observed_dst = Path(os.fsdecode(os.fspath(dst)))
        if observed_dst == output_path and not swapped:
            swapped = True
            output_path.unlink()
            os.link(target_path, output_path)
        real_replace(src, dst)

    monkeypatch.setattr(os, "replace", swapping_replace)

    save_execution_model_json(model, output_path)

    assert swapped is True
    assert target_path.read_text(encoding="utf-8") == (
        "target must not be overwritten\n"
    )
    assert output_path.stat().st_nlink == 1
    assert json.loads(output_path.read_text(encoding="utf-8"))["artifact_mode"] == (
        "telemetry_execution_model"
    )
