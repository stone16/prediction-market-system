"""STO-10 cp-03: end-to-end first-live-order rehearsal driver.

Drives the real `PolymarketActuator` slow path with a real
strict-sidecar `FileFirstLiveOrderGate` and a real
`JsonlFirstOrderAuditWriter`, backed by inline fakes for the venue
client and quote provider. No network, no DB, no real money.

The script exists so an operator can run a single-command smoke test
on the deployed Fly machine before the first live launch:

    uv run python scripts/rehearse_first_order.py --approver-id alice

PASS proves five things together: (1) the volume is mounted at a path
the runner UID can write; (2) the audit JSONL pipeline writes records;
(3) the gate matches an approval JSON the operator helper produces;
(4) strict sidecar provenance is present and bound to that approval;
(5) the consume() lifecycle unlinks both files. FAIL prints which step
broke and where the audit log is for inspection.

Tests in `tests/unit/test_rehearse_first_order_script.py` exercise the
same path through the asyncio entry point so CI catches regressions.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import stat
import sys
import tempfile
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from pms.actuator.adapters.polymarket import (
    FileFirstLiveOrderGate,
    LivePreSubmitQuote,
    OperatorApprovalRequiredError,
    PolymarketActuator,
    PolymarketOrderRequest,
    PolymarketOrderResult,
)
from pms.config import ControllerSettings, PMSSettings, PolymarketSettings
from pms.core.enums import OrderStatus, Side, TimeInForce
from pms.core.models import Portfolio, TradeDecision
from pms.storage.first_order_audit import JsonlFirstOrderAuditWriter
from scripts.approve_first_order import ApprovalPreview, write_approval


_EXPECTED_EVENT_SEQUENCE: tuple[str, ...] = (
    "approval_denied",
    "approval_matched",
    "approval_consumed",
    "approval_denied",
)
REHEARSAL_REPORT_GENERATOR_ID = "scripts/rehearse_first_order.py"
REHEARSAL_APPROVAL_MAX_AGE_S = 300.0


@dataclass(frozen=True, slots=True)
class RehearsalResult:
    passed: bool
    events: list[str]
    audit_path: Path
    approval_path: Path
    report_path: Path
    approver_id: str
    failure_reason: str | None


@dataclass(frozen=True, slots=True)
class _RehearsalClient:
    """Stand-in for `PolymarketSDKClient` that returns a fixed
    matched-fill result. Local-only — never reaches the venue."""

    requires_live_mode: bool = False

    async def submit_order(
        self,
        order: PolymarketOrderRequest,
        credentials: object,
    ) -> PolymarketOrderResult:
        del credentials
        filled_quantity = (
            order.notional_usdc / order.price if order.price > 0 else 0.0
        )
        return PolymarketOrderResult(
            order_id="rehearsal-order-1",
            status=OrderStatus.MATCHED.value,
            raw_status="matched",
            filled_notional_usdc=order.notional_usdc,
            remaining_notional_usdc=0.0,
            fill_price=order.price,
            filled_quantity=filled_quantity,
        )


@dataclass(frozen=True, slots=True)
class _RehearsalQuoteProvider:
    async def quote(
        self,
        order: PolymarketOrderRequest,
        credentials: object,
    ) -> LivePreSubmitQuote:
        del credentials
        return LivePreSubmitQuote(
            market_status="open",
            book_age_ms=25.0,
            executable_notional_usdc=order.notional_usdc,
            best_executable_price=order.price,
            spread_bps=10.0,
            quote_hash="rehearsal-quote",
            book_ts=datetime.now(tz=UTC),
        )


def _rehearsal_settings() -> PMSSettings:
    return PMSSettings(
        live_trading_enabled=True,
        controller=ControllerSettings(time_in_force="IOC"),
        polymarket=PolymarketSettings(
            private_key="rehearsal-private-key",
            api_key="rehearsal-api-key",
            api_secret="rehearsal-api-secret",
            api_passphrase="rehearsal-passphrase",
            signature_type=1,
            funder_address="0x3333333333333333333333333333333333333333",
            operator_approval_mode="every_order",
        ),
    )


def _rehearsal_decision() -> TradeDecision:
    return TradeDecision(
        decision_id="rehearsal-decision",
        market_id="m-rehearsal",
        token_id="t-rehearsal-yes",
        venue="polymarket",
        side=Side.BUY.value,
        notional_usdc=5.0,
        order_type="limit",
        max_slippage_bps=50,
        stop_conditions=["rehearsal"],
        prob_estimate=0.6,
        expected_edge=0.2,
        time_in_force=TimeInForce.IOC,
        opportunity_id="op-rehearsal",
        strategy_id="rehearsal",
        strategy_version_id="rehearsal-v1",
        action=Side.BUY.value,
        limit_price=0.4,
        outcome="YES",
    )


def _rehearsal_decision_for_fresh_approval_probe() -> TradeDecision:
    return replace(
        _rehearsal_decision(),
        decision_id="rehearsal-decision-fresh-approval-probe",
        market_id="m-rehearsal-fresh-approval",
        token_id="t-rehearsal-fresh-approval-yes",
        opportunity_id="op-rehearsal-fresh-approval-probe",
    )


def _rehearsal_portfolio() -> Portfolio:
    return Portfolio(
        total_usdc=1000.0,
        free_usdc=1000.0,
        locked_usdc=0.0,
        open_positions=[],
        max_drawdown_pct=None,
    )


def _read_audit_events(audit_path: Path) -> list[str]:
    if not audit_path.exists():
        return []
    events: list[str] = []
    for line_number, raw_line in enumerate(
        _read_text_no_follow(audit_path).splitlines(),
        start=1,
    ):
        line = raw_line.strip()
        if not line.strip():
            continue
        try:
            record = _loads_json_rejecting_duplicate_keys(line)
        except json.JSONDecodeError as exc:
            msg = f"{audit_path.name}:{line_number}: invalid JSON row"
            raise ValueError(msg) from exc
        except ValueError as exc:
            msg = f"{audit_path.name}:{line_number}: {exc}"
            raise ValueError(msg) from exc
        if not isinstance(record, dict):
            msg = f"{audit_path.name}:{line_number}: expected JSON object"
            raise ValueError(msg)
        event = record.get("event")
        if isinstance(event, str):
            events.append(event)
    return events


def _loads_json_rejecting_duplicate_keys(text: str) -> object:
    def reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
        parsed: dict[str, object] = {}
        for key, value in pairs:
            if key in parsed:
                msg = f"duplicate JSON key: {key}"
                raise ValueError(msg)
            parsed[key] = value
        return parsed

    return json.loads(text, object_pairs_hook=reject_duplicate_keys)


def _read_text_no_follow(path: Path) -> str:
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(path, flags, 0o777)
    try:
        path_stat = os.fstat(fd)
        if not stat.S_ISREG(path_stat.st_mode):
            raise OSError(f"operator rehearsal audit path is not a regular file: {path}")
        if path_stat.st_nlink != 1:
            raise OSError(f"operator rehearsal audit path is not a single-link file: {path}")
        with os.fdopen(fd, "r", encoding="utf-8") as file:
            fd = -1
            return file.read()
    finally:
        if fd >= 0:
            os.close(fd)


def _finish_rehearsal_result(
    *,
    passed: bool,
    events: list[str],
    audit_path: Path,
    approval_path: Path,
    report_path: Path,
    approver_id: str,
    failure_reason: str | None,
) -> RehearsalResult:
    result = RehearsalResult(
        passed=passed,
        events=events,
        audit_path=audit_path,
        approval_path=approval_path,
        report_path=report_path,
        approver_id=approver_id,
        failure_reason=failure_reason,
    )
    _write_rehearsal_report(result)
    return result


def _write_rehearsal_report(result: RehearsalResult) -> None:
    decision = "PASS" if result.passed else "FAIL"
    expected_events = set(_EXPECTED_EVENT_SEQUENCE)
    observed_events = set(result.events)
    generated_at = datetime.now(tz=UTC)

    rows = [
        _report_row(
            "approval_denied",
            "approval_denied" in observed_events,
            "gate denied before approval file existed",
        ),
        _report_row(
            "approval_matched",
            "approval_matched" in observed_events,
            "approval JSON matched preview",
        ),
        _report_row(
            "approval_consumed",
            "approval_consumed" in observed_events,
            "approval JSON and sidecar were unlinked",
        ),
        _report_row(
            "strict_sidecar_provenance",
            result.passed,
            "strict gate required sidecar approver_id, timestamp, and approval hash",
        ),
        _report_row(
            "fresh_approval_required",
            tuple(result.events) == _EXPECTED_EVENT_SEQUENCE,
            "every-order mode denied the next submit after approval consume",
        ),
        _report_row(
            "unexpected_events",
            observed_events <= expected_events,
            f"events={result.events}",
        ),
        _report_row("operator_id", result.approver_id.strip() != "", result.approver_id),
    ]
    if result.failure_reason is not None:
        rows.append(
            f"| failure_reason | FAIL | {_escape_table_value(result.failure_reason)} |"
        )

    _write_text_no_follow(
        result.report_path,
        "\n".join(
            [
                f"# Operator Approval Rehearsal - {generated_at.date().isoformat()}",
                "",
                "## Report Provenance",
                "",
                "| Field | Value |",
                "|---|---|",
                f"| generated_by | {REHEARSAL_REPORT_GENERATOR_ID} |",
                f"| generated_at | {generated_at.isoformat()} |",
                "| artifact_mode | persisted |",
                f"| output_path | {_escape_table_value(str(result.report_path))} |",
                "",
                "## Operator Approval Rehearsal",
                "",
                f"**Decision:** {decision}",
                "",
                "| Check | Status | Detail |",
                "|---|---|---|",
                *rows,
                "",
            ]
        ),
    )


def _report_row(check: str, passed: bool, detail: str) -> str:
    status = "PASS" if passed else "FAIL"
    return f"| {check} | {status} | {_escape_table_value(detail)} |"


def _escape_table_value(value: str) -> str:
    return " ".join(value.replace("|", "\\|").splitlines())


def _write_text_no_follow(path: Path, content: str) -> None:
    _require_regular_file_or_absent(path)
    fd, temp_path = _open_report_temp_file(path)
    published = False
    try:
        os.fchmod(fd, 0o600)
        os.ftruncate(fd, 0)
        os.lseek(fd, 0, os.SEEK_SET)
        with os.fdopen(fd, "w", encoding="utf-8") as file:
            fd = -1
            file.write(content)
            file.flush()
            os.fsync(file.fileno())
        _require_regular_file_or_absent(path)
        os.replace(temp_path, path)
        published = True
        _fsync_parent_directory(path)
    finally:
        if fd >= 0:
            os.close(fd)
        if not published:
            _unlink_regular_single_link_file_if_present(temp_path)


def _open_report_temp_file(path: Path) -> tuple[int, Path]:
    _require_regular_file_or_absent(path)
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    for _ in range(16):
        temp_path = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
        try:
            fd = os.open(temp_path, flags, 0o600)
        except FileExistsError:
            continue
        try:
            _require_open_regular_single_link_file(fd, temp_path)
            os.fchmod(fd, 0o600)
        except BaseException:
            os.close(fd)
            _unlink_regular_single_link_file_if_present(temp_path)
            raise
        return fd, temp_path
    raise FileExistsError(f"could not create temporary rehearsal report for {path}")


def _unlink_regular_single_link_file_if_present(path: Path) -> None:
    try:
        path_stat = path.lstat()
    except FileNotFoundError:
        return
    if not stat.S_ISREG(path_stat.st_mode) or path_stat.st_nlink != 1:
        return
    path.unlink()


def _fsync_parent_directory(path: Path) -> None:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    try:
        fd = os.open(path.parent, flags)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        return
    finally:
        os.close(fd)


def _require_open_regular_single_link_file(fd: int, path: Path) -> None:
    path_stat = os.fstat(fd)
    if not stat.S_ISREG(path_stat.st_mode):
        raise OSError(f"operator rehearsal report path is not a regular file: {path}")
    if path_stat.st_nlink != 1:
        raise OSError(
            f"operator rehearsal report path is not a single-link file: {path}"
        )


def _require_regular_file_or_absent(path: Path) -> None:
    try:
        mode = path.lstat().st_mode
    except FileNotFoundError:
        return
    if not stat.S_ISREG(mode):
        raise OSError(f"operator rehearsal report path is not a regular file: {path}")
    if path.lstat().st_nlink != 1:
        raise OSError(
            f"operator rehearsal report path is not a single-link file: {path}"
        )


def _require_workdir_outside_working_tree(workdir: Path) -> None:
    configured_path = _absolute_path_without_symlink_resolution(workdir)
    resolved_path = workdir.expanduser().resolve(strict=False)
    working_tree = _working_tree_root(Path.cwd().resolve(strict=False))
    working_trees = [working_tree]
    for candidate in (configured_path, resolved_path):
        candidate_working_tree = _containing_working_tree_root(candidate)
        if candidate_working_tree is not None:
            working_trees.append(candidate_working_tree)

    for working_tree_candidate in dict.fromkeys(working_trees):
        if working_tree_candidate.parent == working_tree_candidate:
            continue
        for candidate in (configured_path, resolved_path):
            try:
                candidate.relative_to(working_tree_candidate)
            except ValueError:
                continue
            raise OSError(
                "rehearsal workdir must live outside the working tree: "
                f"{candidate}"
            )


def _absolute_path_without_symlink_resolution(path: Path) -> Path:
    expanded = path.expanduser()
    if not expanded.is_absolute():
        expanded = Path.cwd() / expanded
    return Path(os.path.abspath(expanded))


def _working_tree_root(start: Path) -> Path:
    for candidate in (start, *start.parents):
        if (candidate / ".git").exists():
            return candidate
    return start


def _containing_working_tree_root(path: Path) -> Path | None:
    start = path if path.is_dir() else path.parent
    for candidate in (start, *start.parents):
        if (candidate / ".git").exists():
            return candidate
    return None


def _prepare_private_workdir(workdir: Path) -> None:
    _require_workdir_outside_working_tree(workdir)
    try:
        mode = workdir.lstat().st_mode
    except FileNotFoundError:
        workdir.mkdir(mode=0o700, parents=True, exist_ok=False)
        os.chmod(workdir, 0o700)
        return
    if not stat.S_ISDIR(mode):
        raise OSError(f"rehearsal workdir is not a directory: {workdir}")
    permissions = stat.S_IMODE(mode)
    if permissions & 0o077:
        raise OSError(
            f"rehearsal workdir {workdir} is too permissive; "
            f"run `chmod 700 {workdir}`."
        )
    if not permissions & stat.S_IWUSR:
        raise OSError(
            f"rehearsal workdir {workdir} is not owner-writable; "
            f"run `chmod 700 {workdir}`."
        )


async def run_rehearsal(
    *,
    workdir: Path,
    approver_id: str = "rehearsal-operator",
) -> RehearsalResult:
    """Execute the cp-03 procedure end to end and return a result.

    The function never raises for expected operational outcomes —
    every failure mode lands in `RehearsalResult.failure_reason` so a
    caller can branch on `passed`."""
    _prepare_private_workdir(workdir)
    workdir = workdir.expanduser().resolve(strict=False)
    approver_id = approver_id.strip()
    approval_path = workdir / "first-order.json"
    audit_path = workdir / "audit.jsonl"
    report_path = workdir / "operator-rehearsal-report.md"

    actuator = PolymarketActuator(
        settings=_rehearsal_settings(),
        client=_RehearsalClient(),
        operator_gate=FileFirstLiveOrderGate(
            approval_path,
            require_approver_sidecar=True,
            approval_max_age_s=REHEARSAL_APPROVAL_MAX_AGE_S,
        ),
        quote_provider=_RehearsalQuoteProvider(),
        audit_writer=JsonlFirstOrderAuditWriter(audit_path),
    )

    decision = _rehearsal_decision()
    portfolio = _rehearsal_portfolio()

    # Step 1: gate denies (no approval file yet) — emits approval_denied.
    try:
        await actuator.execute(decision, portfolio)
    except OperatorApprovalRequiredError:
        pass
    else:
        return _finish_rehearsal_result(
            passed=False,
            events=_read_audit_events(audit_path),
            audit_path=audit_path,
            approval_path=approval_path,
            report_path=report_path,
            approver_id=approver_id,
            failure_reason=(
                "first execute() succeeded with no approval file present; "
                "the gate must deny when the file is missing"
            ),
        )

    # Step 2: operator files the approval (use the same helper the
    # runbook step 6 procedure uses).
    try:
        write_approval(
            ApprovalPreview(
                venue=decision.venue,
                market_id=decision.market_id,
                token_id=decision.token_id,
                side=decision.side,
                outcome=decision.outcome,
                max_notional_usdc=decision.notional_usdc,
                limit_price=decision.limit_price,
                max_slippage_bps=decision.max_slippage_bps,
            ),
            path=approval_path,
            approver_id=approver_id,
            ts=datetime.now(tz=UTC),
        )
    except Exception as exc:  # noqa: BLE001
        return _finish_rehearsal_result(
            passed=False,
            events=_read_audit_events(audit_path),
            audit_path=audit_path,
            approval_path=approval_path,
            report_path=report_path,
            approver_id=approver_id,
            failure_reason=f"approval helper failed while filing approval: {exc}",
        )

    # Step 3: gate matches, submit succeeds, consume runs.
    try:
        order_state = await actuator.execute(decision, portfolio)
    except OperatorApprovalRequiredError as exc:
        return _finish_rehearsal_result(
            passed=False,
            events=_read_audit_events(audit_path),
            audit_path=audit_path,
            approval_path=approval_path,
            report_path=report_path,
            approver_id=approver_id,
            failure_reason=(
                "second execute() was denied after approval was filed; "
                "strict sidecar provenance or approval payload did not validate: "
                f"{exc}"
            ),
        )
    except Exception as exc:  # noqa: BLE001
        return _finish_rehearsal_result(
            passed=False,
            events=_read_audit_events(audit_path),
            audit_path=audit_path,
            approval_path=approval_path,
            report_path=report_path,
            approver_id=approver_id,
            failure_reason=f"second execute() raised after approval was filed: {exc!r}",
        )

    if order_state.status != OrderStatus.MATCHED.value:
        return _finish_rehearsal_result(
            passed=False,
            events=_read_audit_events(audit_path),
            audit_path=audit_path,
            approval_path=approval_path,
            report_path=report_path,
            approver_id=approver_id,
            failure_reason=(
                f"order status was {order_state.status!r}, expected "
                f"{OrderStatus.MATCHED.value!r}"
            ),
        )

    # Step 4: in every-order mode, the next submit must require a fresh approval.
    try:
        await actuator.execute(
            _rehearsal_decision_for_fresh_approval_probe(),
            portfolio,
        )
    except OperatorApprovalRequiredError:
        pass
    else:
        return _finish_rehearsal_result(
            passed=False,
            events=_read_audit_events(audit_path),
            audit_path=audit_path,
            approval_path=approval_path,
            report_path=report_path,
            approver_id=approver_id,
            failure_reason=(
                "third execute() succeeded without a new approval file; "
                "every-order mode must require fresh approval after consume"
            ),
        )

    # Step 5: validate the audit JSONL.
    events = _read_audit_events(audit_path)
    if tuple(events) != _EXPECTED_EVENT_SEQUENCE:
        return _finish_rehearsal_result(
            passed=False,
            events=events,
            audit_path=audit_path,
            approval_path=approval_path,
            report_path=report_path,
            approver_id=approver_id,
            failure_reason=(
                f"audit events were {events!r}, expected "
                f"{list(_EXPECTED_EVENT_SEQUENCE)!r}"
            ),
        )

    return _finish_rehearsal_result(
        passed=True,
        events=events,
        audit_path=audit_path,
        approval_path=approval_path,
        report_path=report_path,
        approver_id=approver_id,
        failure_reason=None,
    )


def report_result(result: RehearsalResult) -> int:
    """Print the rehearsal outcome and return the exit code. Pure
    sync, no asyncio: testable without harness concerns."""
    if result.passed:
        print(f"✓ PASS  events={result.events}")
        print(f"  audit log:    {result.audit_path}")
        print(f"  report:       {result.report_path}")
        print(f"  approval:     {result.approval_path} (unlinked after consume)")
        return 0

    print(f"✗ FAIL  reason: {result.failure_reason}")
    print(f"  events seen:  {result.events}")
    print(f"  audit log:    {result.audit_path}")
    print(f"  report:       {result.report_path}")
    print(f"  approval:     {result.approval_path}")
    return 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "STO-10 cp-03 first-live-order rehearsal: drives the actuator "
            "slow path end to end with fakes and verifies the audit log."
        ),
    )
    parser.add_argument(
        "--workdir",
        default=None,
        help=(
            "Where to write the approval JSON, sidecar, and audit JSONL. "
            "Defaults to a fresh temp directory."
        ),
    )
    parser.add_argument(
        "--approver-id",
        default="rehearsal-operator",
        help="Identity recorded in the audit log's approver_id field.",
    )
    args = parser.parse_args(argv)

    if args.workdir is None:
        workdir = Path(tempfile.mkdtemp(prefix="pms-rehearsal-"))
    else:
        workdir = Path(args.workdir)

    try:
        result = asyncio.run(
            run_rehearsal(workdir=workdir, approver_id=args.approver_id)
        )
    except OSError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    return report_result(result)


if __name__ == "__main__":
    sys.exit(main())
