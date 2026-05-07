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

import json
from pathlib import Path

import pytest

from scripts.rehearse_first_order import RehearsalResult, report_result, run_rehearsal


@pytest.mark.asyncio
async def test_run_rehearsal_passes_with_correct_audit_sequence(
    tmp_path: Path,
) -> None:
    """The full happy path: gate denies once, operator files approval,
    gate matches, submit succeeds, consume runs, audit log records
    `approval_denied → approval_matched → approval_consumed`."""
    result = await run_rehearsal(workdir=tmp_path, approver_id="rehearsal-op")

    assert result.passed, f"rehearsal failed: {result.failure_reason}"
    assert result.events == [
        "approval_denied",
        "approval_matched",
        "approval_consumed",
    ]
    assert result.failure_reason is None


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
async def test_run_rehearsal_unlinks_both_approval_files_on_consume(
    tmp_path: Path,
) -> None:
    """After consume the approval JSON and the sidecar must both be
    gone — verifies the cp-02 sidecar-cleanup change end to end."""
    result = await run_rehearsal(workdir=tmp_path, approver_id="rehearsal-op")

    assert result.passed
    assert not result.approval_path.exists()
    assert not (Path(str(result.approval_path) + ".meta.json")).exists()


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
        failure_reason=None,
    )

    exit_code = report_result(result)

    stdout = capsys.readouterr().out
    assert exit_code == 0
    assert "PASS" in stdout
    assert str(audit_path) in stdout


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
        failure_reason=None,
    )

    with pytest.raises(Exception):
        # frozen dataclass blocks attribute assignment
        result.passed = False  # type: ignore[misc]
