"""STO-10 cp-03: end-to-end first-live-order rehearsal driver.

Drives the real `PolymarketActuator` slow path with a real
`FileFirstLiveOrderGate` and a real `JsonlFirstOrderAuditWriter`,
backed by inline fakes for the venue client and quote provider. No
network, no DB, no real money.

The script exists so an operator can run a single-command smoke test
on the deployed Fly machine before the first live launch:

    uv run python scripts/rehearse_first_order.py --approver-id alice

PASS proves four things together: (1) the volume is mounted at a path
the runner UID can write; (2) the audit JSONL pipeline writes records;
(3) the gate matches an approval JSON the operator helper produces;
(4) the consume() lifecycle unlinks both files. FAIL prints which step
broke and where the audit log is for inspection.

Tests in `tests/unit/test_rehearse_first_order_script.py` exercise the
same path through the asyncio entry point so CI catches regressions.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

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
)


@dataclass(frozen=True, slots=True)
class RehearsalResult:
    passed: bool
    events: list[str]
    audit_path: Path
    approval_path: Path
    failure_reason: str | None


@dataclass(frozen=True, slots=True)
class _RehearsalClient:
    """Stand-in for `PolymarketSDKClient` that returns a fixed
    matched-fill result. Local-only — never reaches the venue."""

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
            funder_address="0xrehearsal",
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
    for line in audit_path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue
        event = record.get("event")
        if isinstance(event, str):
            events.append(event)
    return events


async def run_rehearsal(
    *,
    workdir: Path,
    approver_id: str = "rehearsal-operator",
) -> RehearsalResult:
    """Execute the cp-03 procedure end to end and return a result.

    The function never raises for expected operational outcomes —
    every failure mode lands in `RehearsalResult.failure_reason` so a
    caller can branch on `passed`."""
    workdir.mkdir(parents=True, exist_ok=True)
    approval_path = workdir / "first-order.json"
    audit_path = workdir / "audit.jsonl"

    actuator = PolymarketActuator(
        settings=_rehearsal_settings(),
        client=_RehearsalClient(),
        operator_gate=FileFirstLiveOrderGate(approval_path),
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
        return RehearsalResult(
            passed=False,
            events=_read_audit_events(audit_path),
            audit_path=audit_path,
            approval_path=approval_path,
            failure_reason=(
                "first execute() succeeded with no approval file present; "
                "the gate must deny when the file is missing"
            ),
        )

    # Step 2: operator files the approval (use the same helper the
    # runbook step 6 procedure uses).
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

    # Step 3: gate matches, submit succeeds, consume runs.
    try:
        order_state = await actuator.execute(decision, portfolio)
    except Exception as exc:  # noqa: BLE001
        return RehearsalResult(
            passed=False,
            events=_read_audit_events(audit_path),
            audit_path=audit_path,
            approval_path=approval_path,
            failure_reason=f"second execute() raised after approval was filed: {exc!r}",
        )

    if order_state.status != OrderStatus.MATCHED.value:
        return RehearsalResult(
            passed=False,
            events=_read_audit_events(audit_path),
            audit_path=audit_path,
            approval_path=approval_path,
            failure_reason=(
                f"order status was {order_state.status!r}, expected "
                f"{OrderStatus.MATCHED.value!r}"
            ),
        )

    # Step 4: validate the audit JSONL.
    events = _read_audit_events(audit_path)
    if tuple(events) != _EXPECTED_EVENT_SEQUENCE:
        return RehearsalResult(
            passed=False,
            events=events,
            audit_path=audit_path,
            approval_path=approval_path,
            failure_reason=(
                f"audit events were {events!r}, expected "
                f"{list(_EXPECTED_EVENT_SEQUENCE)!r}"
            ),
        )

    return RehearsalResult(
        passed=True,
        events=events,
        audit_path=audit_path,
        approval_path=approval_path,
        failure_reason=None,
    )


def report_result(result: RehearsalResult) -> int:
    """Print the rehearsal outcome and return the exit code. Pure
    sync, no asyncio: testable without harness concerns."""
    if result.passed:
        print(f"✓ PASS  events={result.events}")
        print(f"  audit log:    {result.audit_path}")
        print(f"  approval:     {result.approval_path} (unlinked after consume)")
        return 0

    print(f"✗ FAIL  reason: {result.failure_reason}")
    print(f"  events seen:  {result.events}")
    print(f"  audit log:    {result.audit_path}")
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

    result = asyncio.run(
        run_rehearsal(workdir=workdir, approver_id=args.approver_id)
    )
    return report_result(result)


if __name__ == "__main__":
    sys.exit(main())
