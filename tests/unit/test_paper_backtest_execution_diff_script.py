from __future__ import annotations

import csv
import json
import os
import stat
from collections.abc import Callable
from dataclasses import replace
from pathlib import Path
from types import TracebackType
from typing import IO, cast

import pytest

from scripts.paper_backtest_execution_diff import (
    build_execution_diff,
    main,
    save_execution_diff_json,
)


EXECUTION_COLUMNS = [
    "decision_id",
    "strategy_id",
    "strategy_version_id",
    "market_id",
    "status",
    "slippage_bps",
    "pnl",
    "rejection_reason",
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


def _write_execution_csv(path: Path, rows: list[dict[str, str]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=EXECUTION_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def _row(
    decision_id: str,
    *,
    strategy_id: str = "h1_flb",
    strategy_version_id: str = "h1-flb-v1",
    market_id: str = "market-1",
    status: str = "filled",
    slippage_bps: str = "6",
    pnl: str = "1.2",
    rejection_reason: str = "",
) -> dict[str, str]:
    return {
        "decision_id": decision_id,
        "strategy_id": strategy_id,
        "strategy_version_id": strategy_version_id,
        "market_id": market_id,
        "status": status,
        "slippage_bps": slippage_bps,
        "pnl": pnl,
        "rejection_reason": rejection_reason,
    }


def test_build_execution_diff_writes_pass_artifact(tmp_path: Path) -> None:
    paper_path = tmp_path / "paper.csv"
    backtest_path = tmp_path / "backtest.csv"
    _write_execution_csv(
        paper_path,
        [
            _row("decision-1", slippage_bps="6", pnl="1.2"),
            _row("decision-2", slippage_bps="10", pnl="-0.2"),
            _row(
                "decision-3",
                status="rejected",
                slippage_bps="",
                pnl="0",
                rejection_reason="insufficient_liquidity",
            ),
            *[
                _row(f"decision-{index}", slippage_bps="6", pnl="0")
                for index in range(4, 10)
            ],
            _row(
                "decision-10",
                status="rejected",
                slippage_bps="",
                pnl="0",
                rejection_reason="insufficient_liquidity",
            ),
        ],
    )
    _write_execution_csv(
        backtest_path,
        [
            _row("decision-1", slippage_bps="7", pnl="1.1"),
            _row("decision-2", slippage_bps="9", pnl="-0.3"),
            _row(
                "decision-3",
                status="rejected",
                slippage_bps="",
                pnl="0",
                rejection_reason="insufficient_liquidity",
            ),
            *[
                _row(f"decision-{index}", slippage_bps="6", pnl="0")
                for index in range(4, 10)
            ],
            _row(
                "decision-10",
                status="rejected",
                slippage_bps="",
                pnl="0",
                rejection_reason="insufficient_liquidity",
            ),
        ],
    )

    diff = build_execution_diff(
        paper_path=paper_path,
        backtest_path=backtest_path,
        max_fill_rate_delta=0.05,
        max_rejection_rate_delta=0.05,
        max_avg_slippage_bps_delta=2.0,
        max_total_pnl_delta=0.25,
    )
    output_path = tmp_path / "paper-backtest-execution-diff.json"
    save_execution_diff_json(diff, output_path)

    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["generated_by"] == "scripts/paper_backtest_execution_diff.py"
    assert payload["artifact_mode"] == "paper_backtest_execution_diff"
    assert payload["strategy_evidence"] == "h1_flb@h1-flb-v1"
    assert payload["final_go_no_go_valid"] is True
    assert payload["metrics"]["paper_fill_rate"] == pytest.approx(0.8)
    assert payload["metrics"]["backtest_fill_rate"] == pytest.approx(0.8)
    assert payload["metrics"]["avg_slippage_bps_delta_abs"] == pytest.approx(0.0)
    assert payload["metrics"]["total_pnl_delta_abs"] == pytest.approx(0.2)
    assert payload["thresholds"]["min_matched_decisions"] == pytest.approx(10.0)
    assert payload["failures"] == []


def test_save_execution_diff_json_rejects_non_strict_json_numbers(
    tmp_path: Path,
) -> None:
    paper_path = tmp_path / "paper.csv"
    backtest_path = tmp_path / "backtest.csv"
    _write_execution_csv(paper_path, [_row("decision-1")])
    _write_execution_csv(backtest_path, [_row("decision-1")])
    diff = build_execution_diff(
        paper_path=paper_path,
        backtest_path=backtest_path,
        min_matched_decisions=1,
    )
    poisoned_diff = replace(
        diff,
        metrics={**diff.metrics, "paper_total_pnl": float("nan")},
    )
    output_path = tmp_path / "paper-backtest-execution-diff.json"

    with pytest.raises(ValueError, match="JSON"):
        save_execution_diff_json(poisoned_diff, output_path)

    assert not output_path.exists()


def test_build_execution_diff_rejects_symlink_input(tmp_path: Path) -> None:
    target_path = tmp_path / "target-paper.csv"
    _write_execution_csv(target_path, [_row("decision-1")])
    paper_path = tmp_path / "paper.csv"
    paper_path.symlink_to(target_path)
    backtest_path = tmp_path / "backtest.csv"
    _write_execution_csv(backtest_path, [_row("decision-1")])

    with pytest.raises(ValueError, match="cannot be read safely"):
        build_execution_diff(paper_path=paper_path, backtest_path=backtest_path)


def test_build_execution_diff_opens_inputs_with_no_follow_when_available(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    no_follow_flag = getattr(os, "O_NOFOLLOW", 0)
    if no_follow_flag == 0:
        pytest.skip("os.O_NOFOLLOW is unavailable on this platform")

    paper_path = tmp_path / "paper.csv"
    backtest_path = tmp_path / "backtest.csv"
    _write_execution_csv(paper_path, [_row("decision-1")])
    _write_execution_csv(backtest_path, [_row("decision-1")])
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

    diff = build_execution_diff(
        paper_path=paper_path,
        backtest_path=backtest_path,
        min_matched_decisions=1,
    )

    observed_by_path = {path: flags for path, flags in observed}
    assert diff.final_go_no_go_valid is True
    assert observed_by_path[paper_path] & no_follow_flag
    assert observed_by_path[backtest_path] & no_follow_flag


def test_build_execution_diff_rejects_hardlink_swap_during_input_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paper_path = tmp_path / "paper.csv"
    backtest_path = tmp_path / "backtest.csv"
    replacement_source = tmp_path / "replacement-paper.csv"
    _write_execution_csv(paper_path, [_row("decision-1")])
    _write_execution_csv(backtest_path, [_row("decision-1")])
    _write_execution_csv(replacement_source, [_row("decision-1")])
    real_open = os.open
    swapped = False

    def swapping_open(
        path_arg: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        flags: int,
        mode: int = 0o777,
    ) -> int:
        nonlocal swapped
        observed_path = Path(os.fsdecode(os.fspath(path_arg)))
        if observed_path == paper_path and not swapped:
            swapped = True
            paper_path.unlink()
            os.link(replacement_source, paper_path)
        return real_open(path_arg, flags, mode)

    monkeypatch.setattr(os, "open", swapping_open)

    with pytest.raises(ValueError, match="cannot be read safely"):
        build_execution_diff(paper_path=paper_path, backtest_path=backtest_path)

    assert swapped is True


def test_build_execution_diff_fails_on_status_mismatch(tmp_path: Path) -> None:
    paper_path = tmp_path / "paper.csv"
    backtest_path = tmp_path / "backtest.csv"
    _write_execution_csv(
        paper_path,
        [_row("decision-1", status="filled", slippage_bps="6", pnl="1.2")],
    )
    _write_execution_csv(
        backtest_path,
        [
            _row(
                "decision-1",
                status="rejected",
                slippage_bps="",
                pnl="0",
                rejection_reason="limit_not_touchable",
            )
        ],
    )

    diff = build_execution_diff(
        paper_path=paper_path,
        backtest_path=backtest_path,
        max_fill_rate_delta=0.05,
        max_rejection_rate_delta=0.05,
        max_avg_slippage_bps_delta=2.0,
        max_total_pnl_delta=0.25,
    )

    assert diff.final_go_no_go_valid is False
    assert "status mismatch decision-1: paper=filled backtest=rejected" in (
        diff.failures
    )


def test_build_execution_diff_rejects_strategy_mismatch_between_exports(
    tmp_path: Path,
) -> None:
    paper_path = tmp_path / "paper.csv"
    backtest_path = tmp_path / "backtest.csv"
    _write_execution_csv(paper_path, [_row("decision-1")])
    _write_execution_csv(
        backtest_path,
        [_row("decision-1", strategy_id="paper_canary_v1")],
    )

    with pytest.raises(ValueError, match="strategy evidence mismatch"):
        build_execution_diff(
            paper_path=paper_path,
            backtest_path=backtest_path,
            min_matched_decisions=1,
        )


def test_build_execution_diff_requires_minimum_matched_decisions(
    tmp_path: Path,
) -> None:
    paper_path = tmp_path / "paper.csv"
    backtest_path = tmp_path / "backtest.csv"
    _write_execution_csv(paper_path, [_row("decision-1")])
    _write_execution_csv(backtest_path, [_row("decision-1")])

    diff = build_execution_diff(paper_path=paper_path, backtest_path=backtest_path)

    assert diff.final_go_no_go_valid is False
    assert "matched_decision_count 1 < min_matched_decisions 10" in diff.failures


def test_cli_require_pass_rejects_thin_matched_sample(tmp_path: Path) -> None:
    paper_path = tmp_path / "paper.csv"
    backtest_path = tmp_path / "backtest.csv"
    output_path = tmp_path / "diff.json"
    _write_execution_csv(paper_path, [_row("decision-1")])
    _write_execution_csv(backtest_path, [_row("decision-1")])

    exit_code = main(
        [
            "--paper",
            str(paper_path),
            "--backtest",
            str(backtest_path),
            "--output",
            str(output_path),
            "--require-pass",
        ]
    )

    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert exit_code == 1
    assert payload["final_go_no_go_valid"] is False
    assert any("min_matched_decisions" in failure for failure in payload["failures"])


def test_build_execution_diff_rejects_missing_required_columns(
    tmp_path: Path,
) -> None:
    paper_path = tmp_path / "paper.csv"
    backtest_path = tmp_path / "backtest.csv"
    with paper_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["decision_id", "status"])
        writer.writeheader()
        writer.writerow({"decision_id": "decision-1", "status": "filled"})
    _write_execution_csv(backtest_path, [_row("decision-1")])

    with pytest.raises(ValueError, match="missing required columns"):
        build_execution_diff(
            paper_path=paper_path,
            backtest_path=backtest_path,
            max_fill_rate_delta=0.05,
            max_rejection_rate_delta=0.05,
            max_avg_slippage_bps_delta=2.0,
            max_total_pnl_delta=0.25,
        )


def test_build_execution_diff_rejects_duplicate_header(tmp_path: Path) -> None:
    paper_path = tmp_path / "paper.csv"
    backtest_path = tmp_path / "backtest.csv"
    paper_path.write_text(
        "\n".join(
            (
                "decision_id,market_id,status,slippage_bps,pnl,pnl,rejection_reason",
                "decision-1,market-1,filled,6,1.2,999,",
            )
        ),
        encoding="utf-8",
    )
    _write_execution_csv(backtest_path, [_row("decision-1")])

    with pytest.raises(ValueError, match="duplicate CSV column: pnl"):
        build_execution_diff(
            paper_path=paper_path,
            backtest_path=backtest_path,
        )


def test_cli_returns_nonzero_when_required_diff_fails(tmp_path: Path) -> None:
    paper_path = tmp_path / "paper.csv"
    backtest_path = tmp_path / "backtest.csv"
    output_path = tmp_path / "diff.json"
    _write_execution_csv(paper_path, [_row("decision-1", pnl="10")])
    _write_execution_csv(backtest_path, [_row("decision-1", pnl="-10")])

    exit_code = main(
        [
            "--paper",
            str(paper_path),
            "--backtest",
            str(backtest_path),
            "--output",
            str(output_path),
            "--max-total-pnl-delta",
            "1",
            "--require-pass",
        ]
    )

    assert exit_code == 1
    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["final_go_no_go_valid"] is False
    assert any("total_pnl_delta_abs" in failure for failure in payload["failures"])


def test_save_execution_diff_json_creates_output_parent_private(
    tmp_path: Path,
) -> None:
    paper_path = tmp_path / "paper.csv"
    backtest_path = tmp_path / "backtest.csv"
    _write_execution_csv(paper_path, [_row("decision-1")])
    _write_execution_csv(backtest_path, [_row("decision-1")])
    diff = build_execution_diff(paper_path=paper_path, backtest_path=backtest_path)
    output_dir = tmp_path / "artifacts"

    save_execution_diff_json(diff, output_dir / "diff.json")

    assert stat.S_IMODE(output_dir.stat().st_mode) == 0o700


def test_save_execution_diff_json_refuses_permissive_output_parent(
    tmp_path: Path,
) -> None:
    paper_path = tmp_path / "paper.csv"
    backtest_path = tmp_path / "backtest.csv"
    _write_execution_csv(paper_path, [_row("decision-1")])
    _write_execution_csv(backtest_path, [_row("decision-1")])
    diff = build_execution_diff(paper_path=paper_path, backtest_path=backtest_path)
    output_dir = tmp_path / "shared-artifacts"
    output_dir.mkdir(mode=0o700)
    output_dir.chmod(0o755)

    try:
        with pytest.raises(OSError, match="execution diff output parent"):
            save_execution_diff_json(diff, output_dir / "diff.json")
    finally:
        output_dir.chmod(0o700)

    assert not (output_dir / "diff.json").exists()


def test_cli_returns_operator_error_for_permissive_output_parent(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    paper_path = tmp_path / "paper.csv"
    backtest_path = tmp_path / "backtest.csv"
    _write_execution_csv(paper_path, [_row("decision-1")])
    _write_execution_csv(backtest_path, [_row("decision-1")])
    output_dir = tmp_path / "shared-artifacts"
    output_dir.mkdir(mode=0o700)
    output_dir.chmod(0o755)
    output_path = output_dir / "diff.json"

    try:
        exit_code = main(
            [
                "--paper",
                str(paper_path),
                "--backtest",
                str(backtest_path),
                "--output",
                str(output_path),
            ]
        )
        captured = capsys.readouterr()
    finally:
        output_dir.chmod(0o700)

    assert exit_code == 2
    assert "paper/backtest execution diff output parent" in captured.err
    assert "too permissive" in captured.err
    assert not output_path.exists()


def test_cli_rejects_output_reusing_input_path(tmp_path: Path) -> None:
    paper_path = tmp_path / "paper.csv"
    backtest_path = tmp_path / "backtest.csv"
    _write_execution_csv(paper_path, [_row("decision-1")])
    _write_execution_csv(backtest_path, [_row("decision-1")])
    original_paper_csv = paper_path.read_text(encoding="utf-8")

    exit_code = main(
        [
            "--paper",
            str(paper_path),
            "--backtest",
            str(backtest_path),
            "--output",
            str(paper_path),
        ]
    )

    assert exit_code == 2
    assert paper_path.read_text(encoding="utf-8") == original_paper_csv


def test_save_execution_diff_json_preserves_existing_output_when_write_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paper_path = tmp_path / "paper.csv"
    backtest_path = tmp_path / "backtest.csv"
    _write_execution_csv(paper_path, [_row("decision-1")])
    _write_execution_csv(backtest_path, [_row("decision-1")])
    diff = build_execution_diff(
        paper_path=paper_path,
        backtest_path=backtest_path,
    )
    output_path = tmp_path / "paper-backtest-execution-diff.json"
    output_path.write_text("old diff\n", encoding="utf-8")
    _patch_text_artifact_writes_to_fail(monkeypatch)

    with pytest.raises(OSError, match="simulated write failure"):
        save_execution_diff_json(diff, output_path)

    assert output_path.read_text(encoding="utf-8") == "old diff\n"


def test_save_execution_diff_json_does_not_publish_new_output_when_write_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paper_path = tmp_path / "paper.csv"
    backtest_path = tmp_path / "backtest.csv"
    _write_execution_csv(paper_path, [_row("decision-1")])
    _write_execution_csv(backtest_path, [_row("decision-1")])
    diff = build_execution_diff(
        paper_path=paper_path,
        backtest_path=backtest_path,
    )
    output_path = tmp_path / "paper-backtest-execution-diff.json"
    _patch_text_artifact_writes_to_fail(monkeypatch)

    with pytest.raises(OSError, match="simulated write failure"):
        save_execution_diff_json(diff, output_path)

    assert not output_path.exists()


def test_save_execution_diff_json_atomic_publish_does_not_mutate_hardlink_swap_target(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paper_path = tmp_path / "paper.csv"
    backtest_path = tmp_path / "backtest.csv"
    _write_execution_csv(paper_path, [_row("decision-1")])
    _write_execution_csv(backtest_path, [_row("decision-1")])
    diff = build_execution_diff(
        paper_path=paper_path,
        backtest_path=backtest_path,
    )
    target_path = tmp_path / "target-paper-backtest-diff.json"
    target_path.write_text("target must not be overwritten\n", encoding="utf-8")
    output_path = tmp_path / "paper-backtest-execution-diff.json"
    output_path.write_text("old diff\n", encoding="utf-8")
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

    save_execution_diff_json(diff, output_path)

    assert swapped is True
    assert target_path.read_text(encoding="utf-8") == (
        "target must not be overwritten\n"
    )
    assert output_path.stat().st_nlink == 1
    assert json.loads(output_path.read_text(encoding="utf-8"))["artifact_mode"] == (
        "paper_backtest_execution_diff"
    )
