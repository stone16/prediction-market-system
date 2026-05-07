"""Unit tests for the operator helper at scripts/approve_first_order.py.

The script's job is to take an `OperatorApprovalRequiredError` message
(or explicit fields) and write the approval JSON + sidecar metadata file
correctly so the gate matches on the next decision. Mismatches between
what the operator types and what `_approval_payload_matches` expects
(`src/pms/actuator/adapters/polymarket.py:1122-1144`) would silently fail
the gate; centralising the write in one tested helper prevents that.
"""

from __future__ import annotations

import importlib
import json
import stat
from datetime import UTC, datetime
from pathlib import Path

import pytest

from scripts.approve_first_order import (
    ApprovalPreview,
    main,
    parse_preview_from_error,
    write_approval,
)


_GOLDEN_ERROR_MESSAGE = (
    "First Polymarket live order requires operator approval: "
    "venue=polymarket market=m-0x123 token=t-yes-0x456 side=BUY "
    "outcome=YES max_notional_usdc=5.0 limit_price=0.4 "
    "max_slippage_bps=50"
)


def test_parse_preview_extracts_all_fields() -> None:
    """Golden path: the helper must round-trip every field that
    `_approval_payload_matches` checks. A missing field would cause the
    gate to silently reject."""
    preview = parse_preview_from_error(_GOLDEN_ERROR_MESSAGE)

    assert preview.venue == "polymarket"
    assert preview.market_id == "m-0x123"
    assert preview.token_id == "t-yes-0x456"
    assert preview.side == "BUY"
    assert preview.outcome == "YES"
    assert preview.max_notional_usdc == 5.0
    assert preview.limit_price == 0.4
    assert preview.max_slippage_bps == 50


def test_parse_preview_handles_none_token() -> None:
    """LiveOrderPreview.token_id is `str | None`. When None, the
    f-string emits `token=None` — the helper must round-trip that as
    Python None so the JSON contains `"token_id": null` (which the
    gate's exact-equality check handles correctly)."""
    message = _GOLDEN_ERROR_MESSAGE.replace("token=t-yes-0x456", "token=None")

    preview = parse_preview_from_error(message)

    assert preview.token_id is None


def test_parse_preview_rejects_malformed_input() -> None:
    """Malformed input must raise rather than silently produce a
    partial preview (which would cause a confusing gate mismatch)."""
    with pytest.raises(ValueError, match="venue"):
        parse_preview_from_error("not an error message at all")

    with pytest.raises(ValueError, match="max_notional_usdc"):
        parse_preview_from_error(
            "First Polymarket live order requires operator approval: "
            "venue=polymarket market=m side=BUY outcome=YES "
            "limit_price=0.4 max_slippage_bps=50"
        )


def test_write_approval_creates_both_files(tmp_path: Path) -> None:
    """Helper must write the approval JSON and the
    `<path>.meta.json` sidecar in lockstep, so the gate's match and
    the audit's approver_id capture succeed together."""
    preview = ApprovalPreview(
        venue="polymarket",
        market_id="m-cp06",
        token_id="t-yes",
        side="BUY",
        outcome="YES",
        max_notional_usdc=10.0,
        limit_price=0.4,
        max_slippage_bps=50,
    )
    approval_path = tmp_path / "first-order.json"
    ts = datetime(2026, 5, 7, 12, 0, tzinfo=UTC)

    written_approval, written_sidecar = write_approval(
        preview,
        path=approval_path,
        approver_id="operator-alice",
        ts=ts,
    )

    assert written_approval == approval_path
    assert written_sidecar == Path(str(approval_path) + ".meta.json")

    payload = json.loads(approval_path.read_text(encoding="utf-8"))
    assert payload == {
        "approved": True,
        "venue": "polymarket",
        "market_id": "m-cp06",
        "token_id": "t-yes",
        "side": "BUY",
        "outcome": "YES",
        "max_notional_usdc": 10.0,
        "limit_price": 0.4,
        "max_slippage_bps": 50,
    }

    sidecar_payload = json.loads(written_sidecar.read_text(encoding="utf-8"))
    assert sidecar_payload == {
        "approver_id": "operator-alice",
        "ts": "2026-05-07T12:00:00+00:00",
    }


def test_write_approval_creates_parent_directory(tmp_path: Path) -> None:
    """The configured approval path may include directories that don't
    yet exist (e.g. /data/pms/first-order.json on a fresh Fly volume).
    Mirrors LiveEmergencyAuditWriter parent-dir creation behaviour."""
    preview = ApprovalPreview(
        venue="polymarket",
        market_id="m-cp06",
        token_id="t-yes",
        side="BUY",
        outcome="YES",
        max_notional_usdc=10.0,
        limit_price=0.4,
        max_slippage_bps=50,
    )

    write_approval(
        preview,
        path=tmp_path / "fresh" / "subdir" / "first-order.json",
        approver_id="op-a",
        ts=datetime.now(tz=UTC),
    )

    assert (tmp_path / "fresh" / "subdir" / "first-order.json").exists()


def test_write_approval_files_have_owner_only_permissions(tmp_path: Path) -> None:
    """STO-10 security: the runbook requires umask 077 on the approval
    file so only the runner UID can read. Helper enforces this."""
    preview = ApprovalPreview(
        venue="polymarket",
        market_id="m-cp06",
        token_id="t-yes",
        side="BUY",
        outcome="YES",
        max_notional_usdc=10.0,
        limit_price=0.4,
        max_slippage_bps=50,
    )

    approval_path = tmp_path / "first-order.json"
    written_approval, written_sidecar = write_approval(
        preview,
        path=approval_path,
        approver_id="op-a",
        ts=datetime.now(tz=UTC),
    )

    approval_mode = stat.S_IMODE(written_approval.stat().st_mode)
    sidecar_mode = stat.S_IMODE(written_sidecar.stat().st_mode)
    assert approval_mode == 0o600, f"approval mode is 0o{approval_mode:o}, want 0o600"
    assert sidecar_mode == 0o600, f"sidecar mode is 0o{sidecar_mode:o}, want 0o600"


def test_write_approval_refuses_overwrite_without_force(tmp_path: Path) -> None:
    """Safety: if an approval file already exists, the helper must
    refuse to overwrite without --force. Otherwise an operator could
    accidentally clobber a still-pending authorization."""
    preview = ApprovalPreview(
        venue="polymarket",
        market_id="m-cp06",
        token_id="t-yes",
        side="BUY",
        outcome="YES",
        max_notional_usdc=10.0,
        limit_price=0.4,
        max_slippage_bps=50,
    )
    approval_path = tmp_path / "first-order.json"
    approval_path.write_text("pre-existing", encoding="utf-8")

    with pytest.raises(FileExistsError):
        write_approval(
            preview,
            path=approval_path,
            approver_id="op-a",
            ts=datetime.now(tz=UTC),
        )

    # Pre-existing content must be untouched.
    assert approval_path.read_text(encoding="utf-8") == "pre-existing"


def test_write_approval_force_overwrites(tmp_path: Path) -> None:
    preview = ApprovalPreview(
        venue="polymarket",
        market_id="m-cp06",
        token_id="t-yes",
        side="BUY",
        outcome="YES",
        max_notional_usdc=10.0,
        limit_price=0.4,
        max_slippage_bps=50,
    )
    approval_path = tmp_path / "first-order.json"
    approval_path.write_text("pre-existing", encoding="utf-8")

    write_approval(
        preview,
        path=approval_path,
        approver_id="op-a",
        ts=datetime.now(tz=UTC),
        force=True,
    )

    payload = json.loads(approval_path.read_text(encoding="utf-8"))
    assert payload["approved"] is True


def test_write_approval_writes_sidecar_before_approval_json(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """STO-10 review-loop f2: the actuator's gate matches on the
    approval JSON; if the JSON appears before the sidecar, a running
    actuator can match and submit while `read_approver_id` returns
    None — the audit log would record a real authorization with
    `approver_id: null`. Writing the sidecar first closes that race."""
    write_order: list[str] = []
    real_write = importlib.import_module("scripts.approve_first_order")._write_secret_file

    def _tracking_write(target_path: Path, content: str) -> None:
        write_order.append(target_path.name)
        real_write(target_path, content)

    monkeypatch.setattr(
        "scripts.approve_first_order._write_secret_file", _tracking_write
    )

    preview = ApprovalPreview(
        venue="polymarket",
        market_id="m-cp06",
        token_id="t-yes",
        side="BUY",
        outcome="YES",
        max_notional_usdc=10.0,
        limit_price=0.4,
        max_slippage_bps=50,
    )
    approval_path = tmp_path / "first-order.json"

    write_approval(
        preview,
        path=approval_path,
        approver_id="op-a",
        ts=datetime.now(tz=UTC),
    )

    assert write_order == [
        "first-order.json.meta.json",
        "first-order.json",
    ], (
        "sidecar must be written before the approval JSON so the gate "
        f"never sees the approval before the operator identity is on disk; "
        f"actual order: {write_order}"
    )


def test_write_approval_does_not_publish_approval_when_sidecar_write_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """If sidecar writing raises, the approval JSON must NOT exist on
    disk afterwards — otherwise an actuator could match a half-armed
    authorization with no operator identity recorded."""
    real_write = importlib.import_module("scripts.approve_first_order")._write_secret_file

    def _failing_sidecar_write(target_path: Path, content: str) -> None:
        if target_path.name.endswith(".meta.json"):
            raise OSError("simulated sidecar write failure")
        real_write(target_path, content)

    monkeypatch.setattr(
        "scripts.approve_first_order._write_secret_file", _failing_sidecar_write
    )

    preview = ApprovalPreview(
        venue="polymarket",
        market_id="m-cp06",
        token_id="t-yes",
        side="BUY",
        outcome="YES",
        max_notional_usdc=10.0,
        limit_price=0.4,
        max_slippage_bps=50,
    )
    approval_path = tmp_path / "first-order.json"

    with pytest.raises(OSError, match="simulated sidecar write failure"):
        write_approval(
            preview,
            path=approval_path,
            approver_id="op-a",
            ts=datetime.now(tz=UTC),
        )

    assert not approval_path.exists(), (
        "approval JSON must not exist when sidecar write failed first"
    )


def test_main_end_to_end(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """End-to-end: pass an error message and an approver-id; assert
    both files appear with the right shape. The operator's actual
    invocation path."""
    approval_path = tmp_path / "first-order.json"

    exit_code = main(
        [
            "--from-error",
            _GOLDEN_ERROR_MESSAGE,
            "--approver-id",
            "operator-alice",
            "--path",
            str(approval_path),
        ]
    )

    assert exit_code == 0
    assert approval_path.exists()
    assert (Path(str(approval_path) + ".meta.json")).exists()

    stdout = capsys.readouterr().out
    assert str(approval_path) in stdout
    assert "operator-alice" in stdout

    payload = json.loads(approval_path.read_text(encoding="utf-8"))
    assert payload["market_id"] == "m-0x123"
    assert payload["token_id"] == "t-yes-0x456"
