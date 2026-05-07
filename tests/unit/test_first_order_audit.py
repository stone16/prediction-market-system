from __future__ import annotations

import json
from pathlib import Path

import pytest

from pms.actuator.adapters.polymarket import LiveOrderPreview
from pms.core.enums import Side
from pms.storage.first_order_audit import JsonlFirstOrderAuditWriter


def _preview() -> LiveOrderPreview:
    return LiveOrderPreview(
        max_notional_usdc=10.0,
        venue="polymarket",
        market_id="m-x",
        token_id="t-yes",
        side=Side.BUY.value,
        limit_price=0.4,
        max_slippage_bps=50,
        outcome="YES",
    )


@pytest.mark.asyncio
async def test_jsonl_first_order_audit_writer_appends_one_record_per_event(
    tmp_path: Path,
) -> None:
    """STO-10 cp-02: each call to record_event appends exactly one
    JSONL line, in call order, with the preview's match-keys captured
    so a forensic walker can compare an event back to the gate."""
    audit_path = tmp_path / "audit.jsonl"
    writer = JsonlFirstOrderAuditWriter(audit_path)

    await writer.record_event(
        event="approval_matched", preview=_preview(), approver_id="op-a"
    )
    await writer.record_event(
        event="approval_consumed", preview=_preview(), approver_id="op-a"
    )

    lines = audit_path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 2

    record_matched = json.loads(lines[0])
    record_consumed = json.loads(lines[1])

    assert record_matched["event"] == "approval_matched"
    assert record_consumed["event"] == "approval_consumed"

    # Match-keys (the same fields _approval_payload_matches checks
    # at polymarket.py:1122-1144).
    assert record_matched["venue"] == "polymarket"
    assert record_matched["market_id"] == "m-x"
    assert record_matched["token_id"] == "t-yes"
    assert record_matched["side"] == "BUY"
    assert record_matched["outcome"] == "YES"
    assert record_matched["max_notional_usdc"] == 10.0
    assert record_matched["limit_price"] == 0.4
    assert record_matched["max_slippage_bps"] == 50
    assert record_matched["approver_id"] == "op-a"
    assert "ts" in record_matched and record_matched["ts"].endswith("+00:00")


@pytest.mark.asyncio
async def test_jsonl_first_order_audit_writer_creates_parent_directory(
    tmp_path: Path,
) -> None:
    """STO-10 cp-02: the writer must create missing parent dirs (matching
    LiveEmergencyAuditWriter behaviour at live_emergency_audit.py:75)."""
    audit_path = tmp_path / "deep" / "nested" / "audit.jsonl"
    writer = JsonlFirstOrderAuditWriter(audit_path)

    await writer.record_event(event="approval_matched", preview=_preview())

    assert audit_path.exists()


@pytest.mark.asyncio
async def test_jsonl_first_order_audit_writer_handles_missing_approver_id(
    tmp_path: Path,
) -> None:
    """approver_id is optional — if not supplied, the JSONL record must
    include the field as null so downstream readers can rely on schema
    stability."""
    audit_path = tmp_path / "audit.jsonl"
    writer = JsonlFirstOrderAuditWriter(audit_path)

    await writer.record_event(event="approval_denied", preview=_preview())

    record = json.loads(audit_path.read_text(encoding="utf-8").splitlines()[0])
    assert record["approver_id"] is None
