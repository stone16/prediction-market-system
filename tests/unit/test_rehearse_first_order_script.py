"""Tests for the cp-03 first-live-order rehearsal driver at
scripts/rehearse_first_order.py.

The rehearsal exercises the real `PolymarketActuator` slow path with a
real `FileFirstLiveOrderGate` + real `JsonlFirstOrderAuditWriter`, so
it doubles as a deployment smoke test: a successful run on the Fly
machine proves the volume is mounted, the env var resolves to a
writable path, and the audit pipeline emits the expected sequence.

Test split:

* Async tests exercise `run_rehearsal()` end to end through the real
  actuator. These are the load-bearing assertions.
* Sync tests exercise `report_result()` — the pure print-and-exit-code
  helper that `main()` delegates to. Splitting `main()` this way lets
  us test the operator-facing behaviour without invoking
  `asyncio.run()` from a sync test (which tangles with pytest's
  session loop and the project's `filterwarnings = ["error"]`).
"""

from __future__ import annotations

from collections.abc import Coroutine
from dataclasses import asdict
from datetime import datetime
import json
import os
from pathlib import Path
import stat
from typing import NoReturn

import pytest

from scripts.approve_first_order import ApprovalPreview
import scripts.rehearse_first_order as rehearsal
from scripts.rehearse_first_order import RehearsalResult, report_result, run_rehearsal


@pytest.mark.asyncio
async def test_run_rehearsal_passes_with_correct_audit_sequence(
    tmp_path: Path,
) -> None:
    """The full happy path: gate denies once, operator files approval,
    gate matches, submit succeeds, consume runs, audit log records
    `approval_denied → approval_matched → approval_consumed → approval_denied`."""
    result = await run_rehearsal(workdir=tmp_path, approver_id="rehearsal-op")

    assert result.passed, f"rehearsal failed: {result.failure_reason}"
    assert result.events == [
        "approval_denied",
        "approval_matched",
        "approval_consumed",
        "approval_denied",
    ]
    assert result.failure_reason is None


@pytest.mark.asyncio
async def test_run_rehearsal_creates_missing_workdir_private(
    tmp_path: Path,
) -> None:
    workdir = tmp_path / "rehearsal-workdir"

    result = await run_rehearsal(workdir=workdir, approver_id="rehearsal-op")

    assert result.passed
    assert stat.S_IMODE(workdir.stat().st_mode) == 0o700


@pytest.mark.asyncio
async def test_run_rehearsal_rejects_permissive_workdir(
    tmp_path: Path,
) -> None:
    workdir = tmp_path / "shared-rehearsal"
    workdir.mkdir(mode=0o700)
    workdir.chmod(0o755)

    try:
        with pytest.raises(OSError, match="rehearsal workdir"):
            await run_rehearsal(workdir=workdir, approver_id="rehearsal-op")
    finally:
        workdir.chmod(0o700)

    assert not (workdir / "first-order.json").exists()
    assert not (workdir / "audit.jsonl").exists()
    assert not (workdir / "operator-rehearsal-report.md").exists()


@pytest.mark.asyncio
async def test_run_rehearsal_rejects_workdir_inside_working_tree(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    (repo_root / ".git").mkdir()
    workdir = repo_root / "secure-rehearsal"
    monkeypatch.chdir(repo_root)

    with pytest.raises(OSError, match="outside the working tree"):
        await run_rehearsal(workdir=workdir, approver_id="rehearsal-op")

    assert not (workdir / "first-order.json").exists()
    assert not (workdir / "audit.jsonl").exists()
    assert not (workdir / "operator-rehearsal-report.md").exists()


@pytest.mark.asyncio
async def test_run_rehearsal_writes_audit_jsonl_with_approver(
    tmp_path: Path,
) -> None:
    """STO-10 cp-02 + sidecar end-to-end: the audit JSONL the rehearsal
    leaves on disk must record `approver_id` for the matched and
    consumed events (denied has no approver yet — the file isn't filed
    until step 2)."""
    result = await run_rehearsal(workdir=tmp_path, approver_id="rehearsal-op")

    assert result.passed
    lines = result.audit_path.read_text(encoding="utf-8").splitlines()
    records = [json.loads(line) for line in lines]

    by_event = {record["event"]: record for record in records}
    assert by_event["approval_denied"]["approver_id"] is None
    assert by_event["approval_matched"]["approver_id"] == "rehearsal-op"
    assert by_event["approval_consumed"]["approver_id"] == "rehearsal-op"


@pytest.mark.asyncio
async def test_run_rehearsal_reports_normalized_approver_id(
    tmp_path: Path,
) -> None:
    result = await run_rehearsal(workdir=tmp_path, approver_id="  rehearsal-op  ")

    assert result.passed
    assert result.approver_id == "rehearsal-op"

    lines = result.audit_path.read_text(encoding="utf-8").splitlines()
    records = [json.loads(line) for line in lines]
    by_event = {record["event"]: record for record in records}
    assert by_event["approval_matched"]["approver_id"] == "rehearsal-op"
    assert by_event["approval_consumed"]["approver_id"] == "rehearsal-op"

    report_text = result.report_path.read_text(encoding="utf-8")
    assert "| operator_id | PASS | rehearsal-op |" in report_text
    assert "  rehearsal-op  " not in report_text


def test_rehearsal_reads_audit_jsonl_with_no_follow_when_available(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    no_follow_flag = getattr(os, "O_NOFOLLOW", 0)
    if no_follow_flag == 0:
        pytest.skip("os.O_NOFOLLOW is unavailable on this platform")

    audit_path = tmp_path / "audit.jsonl"
    audit_path.write_text(
        "\n".join(
            [
                json.dumps({"event": "approval_denied"}),
                json.dumps({"event": "approval_matched"}),
            ]
        )
        + "\n",
        encoding="utf-8",
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

    events = rehearsal._read_audit_events(audit_path)

    observed_by_path = {path: flags for path, flags in observed}
    assert events == ["approval_denied", "approval_matched"]
    assert observed_by_path[audit_path] & no_follow_flag


def test_rehearsal_rejects_malformed_audit_jsonl_row(tmp_path: Path) -> None:
    audit_path = tmp_path / "audit.jsonl"
    audit_path.write_text(
        "\n".join(
            [
                json.dumps({"event": "approval_denied"}),
                "{malformed-json",
                json.dumps({"event": "approval_matched"}),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="audit.jsonl:2: invalid JSON row"):
        rehearsal._read_audit_events(audit_path)


def test_rehearsal_rejects_duplicate_audit_json_key(tmp_path: Path) -> None:
    audit_path = tmp_path / "audit.jsonl"
    audit_path.write_text(
        (
            '{"event":"approval_denied","event":"approval_matched"}\n'
        ),
        encoding="utf-8",
    )

    with pytest.raises(
        ValueError,
        match="audit.jsonl:1: duplicate JSON key: event",
    ):
        rehearsal._read_audit_events(audit_path)


def test_rehearsal_rejects_hardlink_swap_during_audit_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    audit_path = tmp_path / "audit.jsonl"
    audit_path.write_text(
        json.dumps({"event": "approval_denied"}) + "\n",
        encoding="utf-8",
    )
    replacement_source = tmp_path / "replacement-audit.jsonl"
    replacement_source.write_text(
        json.dumps({"event": "approval_matched"}) + "\n",
        encoding="utf-8",
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
        if observed_path == audit_path and not swapped:
            swapped = True
            audit_path.unlink()
            os.link(replacement_source, audit_path)
        return real_open(path_arg, flags, mode)

    monkeypatch.setattr(os, "open", swapping_open)

    with pytest.raises(OSError, match="single-link"):
        rehearsal._read_audit_events(audit_path)

    assert swapped is True


@pytest.mark.asyncio
async def test_run_rehearsal_unlinks_both_approval_files_on_consume(
    tmp_path: Path,
) -> None:
    """After consume the approval JSON and the sidecar must both be
    gone — verifies the cp-02 sidecar-cleanup change end to end."""
    result = await run_rehearsal(workdir=tmp_path, approver_id="rehearsal-op")

    assert result.passed
    assert not result.approval_path.exists()
    assert not (Path(str(result.approval_path) + ".meta.json")).exists()


@pytest.mark.asyncio
async def test_run_rehearsal_fails_without_strict_sidecar_provenance(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """The rehearsal report gates LIVE startup, so it must exercise the
    same strict sidecar provenance path as the real LIVE runner. A helper
    that only writes the approval JSON must not produce a PASS report."""

    def _write_approval_without_sidecar(
        preview: ApprovalPreview,
        *,
        path: Path,
        approver_id: str,
        ts: datetime,
        force: bool = False,
    ) -> tuple[Path, Path]:
        del approver_id, ts, force
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps({"approved": True, **asdict(preview)}, sort_keys=True),
            encoding="utf-8",
        )
        return path, Path(str(path) + ".meta.json")

    monkeypatch.setattr(
        rehearsal,
        "write_approval",
        _write_approval_without_sidecar,
    )

    result = await run_rehearsal(workdir=tmp_path, approver_id="rehearsal-op")

    assert not result.passed
    assert result.failure_reason is not None
    assert "sidecar" in result.failure_reason.lower()


@pytest.mark.asyncio
async def test_run_rehearsal_reports_approval_helper_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    def _failing_write_approval(
        preview: ApprovalPreview,
        *,
        path: Path,
        approver_id: str,
        ts: datetime,
        force: bool = False,
    ) -> tuple[Path, Path]:
        del preview, path, approver_id, ts, force
        raise OSError("simulated approval helper failure")

    monkeypatch.setattr(rehearsal, "write_approval", _failing_write_approval)

    result = await run_rehearsal(workdir=tmp_path, approver_id="rehearsal-op")

    assert not result.passed
    assert result.failure_reason is not None
    assert "approval helper failed" in result.failure_reason
    assert "simulated approval helper failure" in result.failure_reason
    assert result.report_path.exists()
    report_text = result.report_path.read_text(encoding="utf-8")
    assert "**Decision:** FAIL" in report_text
    assert "| failure_reason | FAIL |" in report_text


@pytest.mark.asyncio
async def test_run_rehearsal_writes_machine_checkable_report(
    tmp_path: Path,
) -> None:
    result = await run_rehearsal(workdir=tmp_path, approver_id="rehearsal-op")

    assert result.passed
    assert result.report_path.exists()
    report_text = result.report_path.read_text(encoding="utf-8")
    assert "## Operator Approval Rehearsal" in report_text
    assert "## Report Provenance" in report_text
    assert "| generated_by | scripts/rehearse_first_order.py |" in report_text
    assert "| generated_at | " in report_text
    generated_at_line = next(
        line
        for line in report_text.splitlines()
        if line.startswith("| generated_at | ")
    )
    generated_at = datetime.fromisoformat(
        generated_at_line.strip("|").split("|")[1].strip()
    )
    assert report_text.splitlines()[0] == (
        f"# Operator Approval Rehearsal - {generated_at.date().isoformat()}"
    )
    assert "| artifact_mode | persisted |" in report_text
    assert f"| output_path | {result.report_path} |" in report_text
    assert "**Decision:** PASS" in report_text
    assert "| approval_denied | PASS |" in report_text
    assert "| approval_matched | PASS |" in report_text
    assert "| approval_consumed | PASS |" in report_text
    assert "| strict_sidecar_provenance | PASS |" in report_text
    assert "| fresh_approval_required | PASS |" in report_text
    assert "| operator_id | PASS | rehearsal-op |" in report_text


@pytest.mark.asyncio
async def test_run_rehearsal_records_absolute_report_output_path_for_relative_workdir(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    (repo_root / ".git").mkdir()
    monkeypatch.chdir(repo_root)

    result = await run_rehearsal(
        workdir=Path("../secure/rehearsal"),
        approver_id="rehearsal-op",
    )

    expected_report_path = (
        tmp_path / "secure" / "rehearsal" / "operator-rehearsal-report.md"
    )
    assert result.report_path == expected_report_path
    report_text = result.report_path.read_text(encoding="utf-8")
    assert f"| output_path | {expected_report_path} |" in report_text


def test_report_result_returns_zero_and_prints_pass_on_success(
    capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    """report_result is the operator-facing summary — PASS path prints
    the audit log location so the operator can `cat` it."""
    audit_path = tmp_path / "audit.jsonl"
    approval_path = tmp_path / "first-order.json"
    result = RehearsalResult(
        passed=True,
        events=["approval_denied", "approval_matched", "approval_consumed"],
        audit_path=audit_path,
        approval_path=approval_path,
        report_path=tmp_path / "rehearsal-report.md",
        approver_id="rehearsal-op",
        failure_reason=None,
    )

    exit_code = report_result(result)

    stdout = capsys.readouterr().out
    assert exit_code == 0
    assert "PASS" in stdout
    assert str(audit_path) in stdout


def test_main_returns_operator_error_for_permissive_workdir(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    workdir = tmp_path / "shared-rehearsal"
    workdir.mkdir(mode=0o700)
    workdir.chmod(0o755)

    def raise_workdir_error(
        coro: Coroutine[object, object, RehearsalResult],
    ) -> NoReturn:
        coro.close()
        raise OSError(
            f"rehearsal workdir {workdir} is too permissive; "
            f"run `chmod 700 {workdir}`."
        )

    monkeypatch.setattr(
        "scripts.rehearse_first_order.asyncio.run",
        raise_workdir_error,
    )

    exit_code = rehearsal.main(
        [
            "--workdir",
            str(workdir),
            "--approver-id",
            "rehearsal-op",
        ]
    )
    captured = capsys.readouterr()

    assert exit_code == 2
    assert "rehearsal workdir" in captured.err
    assert "too permissive" in captured.err
    assert not (workdir / "first-order.json").exists()
    assert not (workdir / "audit.jsonl").exists()
    assert not (workdir / "operator-rehearsal-report.md").exists()


def test_rehearsal_report_writer_refuses_symlink_report_path(tmp_path: Path) -> None:
    target_path = tmp_path / "target-rehearsal-report.md"
    target_path.write_text("target must not be overwritten\n", encoding="utf-8")
    report_path = tmp_path / "rehearsal-report.md"
    report_path.symlink_to(target_path)
    result = RehearsalResult(
        passed=True,
        events=["approval_denied", "approval_matched", "approval_consumed"],
        audit_path=tmp_path / "audit.jsonl",
        approval_path=tmp_path / "first-order.json",
        report_path=report_path,
        approver_id="rehearsal-op",
        failure_reason=None,
    )

    with pytest.raises(OSError, match="regular file"):
        rehearsal._write_rehearsal_report(result)

    assert target_path.read_text(encoding="utf-8") == "target must not be overwritten\n"


def test_rehearsal_report_writer_refuses_hardlinked_report_path(tmp_path: Path) -> None:
    target_path = tmp_path / "target-rehearsal-report.md"
    target_path.write_text("target must not be overwritten\n", encoding="utf-8")
    report_path = tmp_path / "rehearsal-report.md"
    os.link(target_path, report_path)
    result = RehearsalResult(
        passed=True,
        events=["approval_denied", "approval_matched", "approval_consumed"],
        audit_path=tmp_path / "audit.jsonl",
        approval_path=tmp_path / "first-order.json",
        report_path=report_path,
        approver_id="rehearsal-op",
        failure_reason=None,
    )

    with pytest.raises(OSError, match="single-link"):
        rehearsal._write_rehearsal_report(result)

    assert target_path.read_text(encoding="utf-8") == "target must not be overwritten\n"


def test_rehearsal_report_writer_hardlink_swap_during_atomic_publish_keeps_linked_target(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target_path = tmp_path / "target-rehearsal-report.md"
    target_path.write_text("target must not be overwritten\n", encoding="utf-8")
    report_path = tmp_path / "operator-rehearsal-report.md"
    report_path.write_text("old report\n", encoding="utf-8")
    real_replace = os.replace
    swapped = False
    result = RehearsalResult(
        passed=True,
        events=["approval_denied", "approval_matched", "approval_consumed"],
        audit_path=tmp_path / "audit.jsonl",
        approval_path=tmp_path / "first-order.json",
        report_path=report_path,
        approver_id="rehearsal-op",
        failure_reason=None,
    )

    def swapping_replace(
        src: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        dst: str | bytes | os.PathLike[str] | os.PathLike[bytes],
    ) -> None:
        nonlocal swapped
        observed_path = Path(os.fsdecode(os.fspath(dst)))
        if observed_path == report_path and not swapped:
            swapped = True
            report_path.unlink()
            os.link(target_path, report_path)
        real_replace(src, dst)

    monkeypatch.setattr(os, "replace", swapping_replace)

    rehearsal._write_rehearsal_report(result)

    assert swapped is True
    assert target_path.read_text(encoding="utf-8") == "target must not be overwritten\n"
    assert "**Decision:** PASS" in report_path.read_text(encoding="utf-8")


def test_rehearsal_report_writer_preserves_existing_report_when_truncate_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    report_path = tmp_path / "operator-rehearsal-report.md"
    original_report_text = "# Existing operator rehearsal report\n"
    report_path.write_text(original_report_text, encoding="utf-8")
    report_path.chmod(0o600)
    real_ftruncate = os.ftruncate
    result = RehearsalResult(
        passed=True,
        events=["approval_denied", "approval_matched", "approval_consumed"],
        audit_path=tmp_path / "audit.jsonl",
        approval_path=tmp_path / "first-order.json",
        report_path=report_path,
        approver_id="rehearsal-op",
        failure_reason=None,
    )

    def truncate_then_fail(fd: int, length: int) -> None:
        real_ftruncate(fd, length)
        raise OSError("simulated rehearsal report truncate failure")

    monkeypatch.setattr(os, "ftruncate", truncate_then_fail)

    with pytest.raises(OSError, match="simulated rehearsal report truncate failure"):
        rehearsal._write_rehearsal_report(result)

    assert report_path.read_text(encoding="utf-8") == original_report_text


def test_rehearsal_report_writer_does_not_publish_new_report_when_truncate_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    report_path = tmp_path / "operator-rehearsal-report.md"
    real_ftruncate = os.ftruncate
    result = RehearsalResult(
        passed=True,
        events=["approval_denied", "approval_matched", "approval_consumed"],
        audit_path=tmp_path / "audit.jsonl",
        approval_path=tmp_path / "first-order.json",
        report_path=report_path,
        approver_id="rehearsal-op",
        failure_reason=None,
    )

    def truncate_then_fail(fd: int, length: int) -> None:
        real_ftruncate(fd, length)
        raise OSError("simulated rehearsal report truncate failure")

    monkeypatch.setattr(os, "ftruncate", truncate_then_fail)

    with pytest.raises(OSError, match="simulated rehearsal report truncate failure"):
        rehearsal._write_rehearsal_report(result)

    assert not report_path.exists()


def test_rehearsal_report_writer_overwrite_clamps_output_permissions(
    tmp_path: Path,
) -> None:
    report_path = tmp_path / "rehearsal-report.md"
    report_path.write_text("pre-existing report\n", encoding="utf-8")
    report_path.chmod(0o644)
    result = RehearsalResult(
        passed=True,
        events=["approval_denied", "approval_matched", "approval_consumed"],
        audit_path=tmp_path / "audit.jsonl",
        approval_path=tmp_path / "first-order.json",
        report_path=report_path,
        approver_id="rehearsal-op",
        failure_reason=None,
    )

    rehearsal._write_rehearsal_report(result)

    assert stat.S_IMODE(report_path.stat().st_mode) == 0o600


def test_rehearsal_report_writer_escapes_freeform_table_values(
    tmp_path: Path,
) -> None:
    report_dir = tmp_path / "operator|reports"
    report_dir.mkdir()
    report_path = report_dir / "operator-rehearsal-report.md"
    result = RehearsalResult(
        passed=False,
        events=["approval_denied"],
        audit_path=tmp_path / "audit.jsonl",
        approval_path=tmp_path / "first-order.json",
        report_path=report_path,
        approver_id="operator|one\nlead",
        failure_reason="operator saw | FAIL | text\nretry approval",
    )

    rehearsal._write_rehearsal_report(result)

    report_text = report_path.read_text(encoding="utf-8")
    assert f"| output_path | {str(report_path).replace('|', '\\|')} |" in report_text
    assert "| operator_id | PASS | operator\\|one lead |" in report_text
    assert (
        "| failure_reason | FAIL | operator saw \\| FAIL \\| text retry approval |"
        in report_text
    )


def test_report_result_returns_one_and_surfaces_reason_on_failure(
    capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    """FAIL path returns nonzero and surfaces the failure reason so an
    operator (or a deploy-time gate) can act on it."""
    result = RehearsalResult(
        passed=False,
        events=["approval_denied"],
        audit_path=tmp_path / "audit.jsonl",
        approval_path=tmp_path / "first-order.json",
        report_path=tmp_path / "rehearsal-report.md",
        approver_id="rehearsal-op",
        failure_reason="audit events were ['approval_denied'], expected …",
    )

    exit_code = report_result(result)

    stdout = capsys.readouterr().out
    assert exit_code == 1
    assert "FAIL" in stdout
    assert "audit events were" in stdout


def test_rehearsal_result_dataclass_is_frozen() -> None:
    """STO-10: result-bearing value objects are frozen dataclasses
    (project convention; see CLAUDE.md). Prevents accidental post-hoc
    mutation of an audit-bearing record."""
    result = RehearsalResult(
        passed=True,
        events=["approval_matched"],
        audit_path=Path("/tmp/audit.jsonl"),
        approval_path=Path("/tmp/approval.json"),
        report_path=Path("/tmp/rehearsal-report.md"),
        approver_id="rehearsal-op",
        failure_reason=None,
    )

    with pytest.raises(Exception):
        # frozen dataclass blocks attribute assignment
        result.passed = False  # type: ignore[misc]
