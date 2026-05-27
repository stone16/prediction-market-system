from __future__ import annotations

import json
import os
import stat
from dataclasses import replace
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
    assert stat.S_IMODE(audit_path.parent.stat().st_mode) == 0o700


@pytest.mark.asyncio
async def test_jsonl_first_order_audit_writer_refuses_permissive_parent(
    tmp_path: Path,
) -> None:
    audit_dir = tmp_path / "permissive-audit"
    audit_dir.mkdir(mode=0o700)
    audit_dir.chmod(0o755)
    audit_path = audit_dir / "audit.jsonl"
    writer = JsonlFirstOrderAuditWriter(audit_path)

    try:
        with pytest.raises(OSError, match="too permissive"):
            await writer.record_event(event="approval_matched", preview=_preview())
    finally:
        audit_dir.chmod(0o700)

    assert not audit_path.exists()


@pytest.mark.asyncio
async def test_jsonl_first_order_audit_writer_refuses_symlink_parent(
    tmp_path: Path,
) -> None:
    audit_dir = tmp_path / "audit-target"
    audit_dir.mkdir(mode=0o700)
    symlink_parent = tmp_path / "audit-link"
    symlink_parent.symlink_to(audit_dir, target_is_directory=True)
    audit_path = symlink_parent / "audit.jsonl"
    writer = JsonlFirstOrderAuditWriter(audit_path)

    with pytest.raises(OSError, match="parent path is not a directory"):
        await writer.record_event(event="approval_matched", preview=_preview())

    assert not (audit_dir / "audit.jsonl").exists()


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


@pytest.mark.asyncio
async def test_jsonl_first_order_audit_writer_rejects_non_finite_preview_evidence(
    tmp_path: Path,
) -> None:
    audit_path = tmp_path / "audit.jsonl"
    writer = JsonlFirstOrderAuditWriter(audit_path)

    with pytest.raises(ValueError, match="max_notional_usdc"):
        await writer.record_event(
            event="approval_matched",
            preview=replace(_preview(), max_notional_usdc=float("nan")),
            approver_id="op-a",
        )

    assert not audit_path.exists()


@pytest.mark.asyncio
async def test_jsonl_first_order_audit_writer_refuses_symlink_path(
    tmp_path: Path,
) -> None:
    target_path = tmp_path / "target-audit.jsonl"
    target_path.write_text("target must not be appended\n", encoding="utf-8")
    audit_path = tmp_path / "audit.jsonl"
    audit_path.symlink_to(target_path)
    writer = JsonlFirstOrderAuditWriter(audit_path)

    with pytest.raises(OSError):
        await writer.record_event(event="approval_matched", preview=_preview())

    assert target_path.read_text(encoding="utf-8") == "target must not be appended\n"


@pytest.mark.asyncio
async def test_jsonl_first_order_audit_writer_refuses_hardlinked_path(
    tmp_path: Path,
) -> None:
    target_path = tmp_path / "target-audit.jsonl"
    target_path.write_text("target must not be appended\n", encoding="utf-8")
    audit_path = tmp_path / "audit.jsonl"
    os.link(target_path, audit_path)
    writer = JsonlFirstOrderAuditWriter(audit_path)

    with pytest.raises(OSError, match="single-link"):
        await writer.record_event(event="approval_matched", preview=_preview())

    assert target_path.read_text(encoding="utf-8") == "target must not be appended\n"


@pytest.mark.asyncio
async def test_jsonl_first_order_audit_writer_refuses_hardlink_swap_during_append(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target_path = tmp_path / "target-audit.jsonl"
    target_path.write_text("target must not be appended\n", encoding="utf-8")
    audit_path = tmp_path / "audit.jsonl"
    audit_path.write_text("old audit\n", encoding="utf-8")
    real_open = os.open
    swapped = False

    def swapping_open(
        path_arg: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        flags: int,
        mode: int = 0o777,
    ) -> int:
        nonlocal swapped
        observed_path = Path(os.fsdecode(os.fspath(path_arg)))
        if observed_path == audit_path and flags & os.O_WRONLY and not swapped:
            swapped = True
            audit_path.unlink()
            os.link(target_path, audit_path)
        return real_open(path_arg, flags, mode)

    monkeypatch.setattr(os, "open", swapping_open)
    writer = JsonlFirstOrderAuditWriter(audit_path)

    with pytest.raises(OSError, match="single-link"):
        await writer.record_event(event="approval_matched", preview=_preview())

    assert swapped is True
    assert target_path.read_text(encoding="utf-8") == "target must not be appended\n"


@pytest.mark.asyncio
async def test_jsonl_first_order_audit_writer_retries_short_os_write(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    audit_path = tmp_path / "audit.jsonl"
    writer = JsonlFirstOrderAuditWriter(audit_path)
    real_write = os.write
    write_sizes: list[int] = []

    def short_write(fd: int, data: bytes) -> int:
        if len(data) > 1:
            chunk_size = max(1, len(data) // 2)
            write_sizes.append(chunk_size)
            return real_write(fd, data[:chunk_size])
        write_sizes.append(len(data))
        return real_write(fd, data)

    monkeypatch.setattr(os, "write", short_write)

    await writer.record_event(event="approval_matched", preview=_preview())

    assert len(write_sizes) > 1
    line = audit_path.read_text(encoding="utf-8")
    assert line.endswith("\n")
    assert json.loads(line)["event"] == "approval_matched"


@pytest.mark.asyncio
async def test_jsonl_first_order_audit_writer_fsyncs_before_returning(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    audit_path = tmp_path / "audit.jsonl"
    writer = JsonlFirstOrderAuditWriter(audit_path)
    fsync_calls: list[int] = []
    real_fsync = os.fsync

    def recording_fsync(fd: int) -> None:
        fsync_calls.append(fd)
        real_fsync(fd)

    monkeypatch.setattr(os, "fsync", recording_fsync)

    await writer.record_event(event="approval_matched", preview=_preview())

    assert fsync_calls, "audit append must fsync before returning"
