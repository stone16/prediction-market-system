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
import os
import stat
from hashlib import sha256
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


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("max_notional_usdc", "nan"),
        ("max_notional_usdc", "inf"),
        ("max_notional_usdc", "0"),
        ("limit_price", "nan"),
        ("limit_price", "1.5"),
        ("max_slippage_bps", "-1"),
    ],
)
def test_parse_preview_rejects_non_actionable_numeric_fields(
    field: str,
    value: str,
) -> None:
    message = re_sub_field(_GOLDEN_ERROR_MESSAGE, field, value)

    with pytest.raises(ValueError, match=field):
        parse_preview_from_error(message)


@pytest.mark.parametrize(
    ("message_field", "bad_value", "expected_field"),
    [
        ("venue", "kalshi", "venue"),
        ("market", "__FILL_IN_MARKET__", "market_id"),
        ("token", "__FILL_IN_TOKEN__", "token_id"),
        ("side", "HOLD", "side"),
        ("outcome", "MAYBE", "outcome"),
    ],
)
def test_parse_preview_rejects_non_actionable_text_fields(
    message_field: str,
    bad_value: str,
    expected_field: str,
) -> None:
    message = re_sub_field(_GOLDEN_ERROR_MESSAGE, message_field, bad_value)

    with pytest.raises(ValueError, match=expected_field):
        parse_preview_from_error(message)


def test_write_approval_rejects_non_finite_preview_before_writing(
    tmp_path: Path,
) -> None:
    approval_path = tmp_path / "approvals" / "first-order.json"

    with pytest.raises(ValueError, match="limit_price"):
        write_approval(
            ApprovalPreview(
                venue="polymarket",
                market_id="m-cp06",
                token_id="t-yes",
                side="BUY",
                outcome="YES",
                max_notional_usdc=10.0,
                limit_price=float("nan"),
                max_slippage_bps=50,
            ),
            path=approval_path,
            approver_id="op-a",
            ts=datetime.now(tz=UTC),
        )

    assert not approval_path.exists()
    assert not Path(str(approval_path) + ".meta.json").exists()
    assert not approval_path.parent.exists()


def test_write_approval_rejects_invalid_preview_text_before_writing(
    tmp_path: Path,
) -> None:
    approval_path = tmp_path / "approvals" / "first-order.json"

    with pytest.raises(ValueError, match="side"):
        write_approval(
            ApprovalPreview(
                venue="polymarket",
                market_id="m-cp06",
                token_id="t-yes",
                side="HOLD",
                outcome="YES",
                max_notional_usdc=10.0,
                limit_price=0.4,
                max_slippage_bps=50,
            ),
            path=approval_path,
            approver_id="op-a",
            ts=datetime.now(tz=UTC),
        )

    assert not approval_path.exists()
    assert not Path(str(approval_path) + ".meta.json").exists()
    assert not approval_path.parent.exists()


def re_sub_field(message: str, field: str, value: str) -> str:
    return " ".join(
        f"{field}={value}" if token.startswith(f"{field}=") else token
        for token in message.split()
    )


def test_write_secret_file_preserves_existing_target_when_truncate_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A failed helper write must not corrupt the currently armed
    approval artifact. The writer should prepare content off to the
    side, then publish atomically only after the full content is durable."""
    module = importlib.import_module("scripts.approve_first_order")
    target_path = tmp_path / "first-order.json"
    target_path.write_text("old approval\n", encoding="utf-8")
    target_path.chmod(0o600)
    real_ftruncate = os.ftruncate

    def _truncate_then_fail(fd: int, length: int) -> None:
        real_ftruncate(fd, length)
        raise OSError("simulated truncate failure")

    monkeypatch.setattr(os, "ftruncate", _truncate_then_fail)

    with pytest.raises(OSError, match="simulated truncate failure"):
        module._write_secret_file(target_path, "new approval")

    assert target_path.read_text(encoding="utf-8") == "old approval\n"


def test_write_secret_file_does_not_publish_new_target_when_truncate_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A failed helper write for a new approval path must leave no
    visible approval JSON for the runner to consume."""
    module = importlib.import_module("scripts.approve_first_order")
    target_path = tmp_path / "first-order.json"
    real_ftruncate = os.ftruncate

    def _truncate_then_fail(fd: int, length: int) -> None:
        real_ftruncate(fd, length)
        raise OSError("simulated truncate failure")

    monkeypatch.setattr(os, "ftruncate", _truncate_then_fail)

    with pytest.raises(OSError, match="simulated truncate failure"):
        module._write_secret_file(target_path, "new approval")

    assert not target_path.exists()


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
    expected_hash = sha256(
        json.dumps(
            payload,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()
    assert sidecar_payload == {
        "approver_id": "operator-alice",
        "approval_sha256": expected_hash,
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


@pytest.mark.parametrize(
    "approver_id",
    [
        "",
        "   ",
        "__FILL_IN_OPERATOR_ID__",
        "operator|forged",
        "operator\nforged",
        "operator\rforged",
    ],
)
def test_write_approval_refuses_non_actionable_approver_id(
    tmp_path: Path,
    approver_id: str,
) -> None:
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

    with pytest.raises(ValueError, match="approver_id"):
        write_approval(
            preview,
            path=approval_path,
            approver_id=approver_id,
            ts=datetime.now(tz=UTC),
        )

    assert not approval_path.exists()
    assert not Path(str(approval_path) + ".meta.json").exists()


def test_write_approval_refuses_permissive_parent_directory(tmp_path: Path) -> None:
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
    approval_dir = tmp_path / "permissive"
    approval_dir.mkdir(mode=0o700)
    approval_dir.chmod(0o755)
    approval_path = approval_dir / "first-order.json"

    try:
        with pytest.raises(OSError, match="approval artifact parent directory"):
            write_approval(
                preview,
                path=approval_path,
                approver_id="op-a",
                ts=datetime.now(tz=UTC),
            )
    finally:
        approval_dir.chmod(0o700)

    assert not approval_path.exists()
    assert not Path(str(approval_path) + ".meta.json").exists()


def test_write_approval_refuses_path_inside_working_tree(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    (repo_root / ".git").mkdir()
    approval_path = repo_root / "secure" / "first-order.json"
    monkeypatch.chdir(repo_root)

    with pytest.raises(OSError, match="outside the working tree"):
        write_approval(
            preview,
            path=approval_path,
            approver_id="op-a",
            ts=datetime.now(tz=UTC),
        )

    assert not approval_path.exists()
    assert not Path(str(approval_path) + ".meta.json").exists()


def test_write_approval_refuses_symlink_parent_directory(tmp_path: Path) -> None:
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
    approval_dir = tmp_path / "approval-target"
    approval_dir.mkdir(mode=0o700)
    symlink_parent = tmp_path / "approval-link"
    symlink_parent.symlink_to(approval_dir, target_is_directory=True)
    approval_path = symlink_parent / "first-order.json"

    with pytest.raises(OSError, match="approval artifact parent path is not a directory"):
        write_approval(
            preview,
            path=approval_path,
            approver_id="op-a",
            ts=datetime.now(tz=UTC),
        )

    assert not (approval_dir / "first-order.json").exists()
    assert not (approval_dir / "first-order.json.meta.json").exists()


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


def test_write_approval_opens_artifacts_with_no_follow_when_available(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    no_follow_flag = getattr(os, "O_NOFOLLOW", 0)
    if no_follow_flag == 0:
        pytest.skip("os.O_NOFOLLOW is unavailable on this platform")

    observed_write_flags: list[int] = []
    real_open = os.open

    def recording_open(
        path_arg: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        flags: int,
        mode: int = 0o777,
    ) -> int:
        if flags & os.O_WRONLY:
            observed_write_flags.append(flags)
        return real_open(path_arg, flags, mode)

    monkeypatch.setattr(os, "open", recording_open)
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
        path=tmp_path / "first-order.json",
        approver_id="op-a",
        ts=datetime.now(tz=UTC),
    )

    assert len(observed_write_flags) == 2
    assert all(flags & no_follow_flag for flags in observed_write_flags)


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


def test_write_approval_refuses_sidecar_overwrite_without_force(
    tmp_path: Path,
) -> None:
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
    sidecar_path = Path(str(approval_path) + ".meta.json")
    sidecar_path.write_text("pre-existing sidecar", encoding="utf-8")

    with pytest.raises(FileExistsError, match="sidecar"):
        write_approval(
            preview,
            path=approval_path,
            approver_id="op-a",
            ts=datetime.now(tz=UTC),
        )

    assert sidecar_path.read_text(encoding="utf-8") == "pre-existing sidecar"
    assert not approval_path.exists()


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


def test_write_approval_refuses_symlink_approval_path_even_with_force(
    tmp_path: Path,
) -> None:
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
    target_path = tmp_path / "target-approval.json"
    target_path.write_text("target must not be overwritten\n", encoding="utf-8")
    approval_path = tmp_path / "first-order.json"
    approval_path.symlink_to(target_path)

    with pytest.raises(OSError, match="regular file"):
        write_approval(
            preview,
            path=approval_path,
            approver_id="op-a",
            ts=datetime.now(tz=UTC),
            force=True,
        )

    assert target_path.read_text(encoding="utf-8") == "target must not be overwritten\n"
    assert not Path(str(approval_path) + ".meta.json").exists()


def test_write_approval_refuses_hardlinked_approval_path_even_with_force(
    tmp_path: Path,
) -> None:
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
    target_path = tmp_path / "target-approval.json"
    target_path.write_text("target must not be overwritten\n", encoding="utf-8")
    approval_path = tmp_path / "first-order.json"
    os.link(target_path, approval_path)

    with pytest.raises(OSError, match="single-link"):
        write_approval(
            preview,
            path=approval_path,
            approver_id="op-a",
            ts=datetime.now(tz=UTC),
            force=True,
        )

    assert target_path.read_text(encoding="utf-8") == "target must not be overwritten\n"
    assert not Path(str(approval_path) + ".meta.json").exists()


def test_write_approval_hardlink_swap_during_atomic_publish_keeps_linked_target(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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
    target_path = tmp_path / "target-approval.json"
    target_path.write_text("target must not be overwritten\n", encoding="utf-8")
    approval_path = tmp_path / "first-order.json"
    approval_path.write_text("old approval\n", encoding="utf-8")
    approval_path.chmod(0o600)
    real_replace = os.replace
    swapped = False

    def swapping_replace(
        src: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        dst: str | bytes | os.PathLike[str] | os.PathLike[bytes],
    ) -> None:
        nonlocal swapped
        observed_path = Path(os.fsdecode(os.fspath(dst)))
        if observed_path == approval_path and not swapped:
            swapped = True
            approval_path.unlink()
            os.link(target_path, approval_path)
        real_replace(src, dst)

    monkeypatch.setattr(os, "replace", swapping_replace)

    write_approval(
        preview,
        path=approval_path,
        approver_id="op-a",
        ts=datetime.now(tz=UTC),
        force=True,
    )

    assert swapped is True
    assert target_path.read_text(encoding="utf-8") == "target must not be overwritten\n"
    assert json.loads(approval_path.read_text(encoding="utf-8"))["market_id"] == "m-cp06"


def test_write_approval_refuses_symlink_sidecar_path(tmp_path: Path) -> None:
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
    sidecar_target = tmp_path / "sidecar-target.json"
    sidecar_target.write_text("target must not be overwritten\n", encoding="utf-8")
    sidecar_path = Path(str(approval_path) + ".meta.json")
    sidecar_path.symlink_to(sidecar_target)

    with pytest.raises(OSError, match="regular file"):
        write_approval(
            preview,
            path=approval_path,
            approver_id="op-a",
            ts=datetime.now(tz=UTC),
        )

    assert sidecar_target.read_text(encoding="utf-8") == "target must not be overwritten\n"
    assert not approval_path.exists()


def test_write_approval_refuses_hardlinked_sidecar_path_even_with_force(
    tmp_path: Path,
) -> None:
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
    sidecar_target = tmp_path / "sidecar-target.json"
    sidecar_target.write_text("target must not be overwritten\n", encoding="utf-8")
    sidecar_path = Path(str(approval_path) + ".meta.json")
    os.link(sidecar_target, sidecar_path)

    with pytest.raises(OSError, match="single-link"):
        write_approval(
            preview,
            path=approval_path,
            approver_id="op-a",
            ts=datetime.now(tz=UTC),
            force=True,
        )

    assert sidecar_target.read_text(encoding="utf-8") == "target must not be overwritten\n"
    assert not approval_path.exists()


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
    assert not Path(str(approval_path) + ".meta.json").exists()


def test_write_approval_removes_sidecar_when_approval_json_write_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """If the sidecar is written but the approval JSON write fails,
    the helper must not leave stale metadata that later blocks final
    preflight/startup before an operator has actually approved an order."""
    real_write = importlib.import_module("scripts.approve_first_order")._write_secret_file

    def _failing_approval_write(target_path: Path, content: str) -> None:
        if not target_path.name.endswith(".meta.json"):
            raise OSError("simulated approval write failure")
        real_write(target_path, content)

    monkeypatch.setattr(
        "scripts.approve_first_order._write_secret_file", _failing_approval_write
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
    sidecar_path = Path(str(approval_path) + ".meta.json")

    with pytest.raises(OSError, match="simulated approval write failure"):
        write_approval(
            preview,
            path=approval_path,
            approver_id="op-a",
            ts=datetime.now(tz=UTC),
        )

    assert not approval_path.exists()
    assert not sidecar_path.exists()


def test_write_approval_restores_existing_sidecar_when_force_approval_write_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """If --force overwrites an existing sidecar and then the approval
    JSON write fails, restore the old sidecar so the approval/metadata
    pair is not left inconsistent."""
    real_write = importlib.import_module("scripts.approve_first_order")._write_secret_file

    def _failing_approval_write(target_path: Path, content: str) -> None:
        if not target_path.name.endswith(".meta.json"):
            raise OSError("simulated approval write failure")
        real_write(target_path, content)

    monkeypatch.setattr(
        "scripts.approve_first_order._write_secret_file", _failing_approval_write
    )

    old_payload = {
        "approved": True,
        "venue": "polymarket",
        "market_id": "m-old",
        "token_id": "t-yes",
        "side": "BUY",
        "outcome": "YES",
        "max_notional_usdc": 10.0,
        "limit_price": 0.4,
        "max_slippage_bps": 50,
    }
    old_sidecar = {
        "approver_id": "operator-old",
        "approval_sha256": sha256(
            json.dumps(
                old_payload,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8")
        ).hexdigest(),
        "ts": "2026-05-07T12:00:00+00:00",
    }
    approval_path = tmp_path / "first-order.json"
    sidecar_path = Path(str(approval_path) + ".meta.json")
    approval_path.write_text(json.dumps(old_payload, sort_keys=True) + "\n", encoding="utf-8")
    sidecar_path.write_text(json.dumps(old_sidecar, sort_keys=True) + "\n", encoding="utf-8")
    original_sidecar_text = sidecar_path.read_text(encoding="utf-8")

    with pytest.raises(OSError, match="simulated approval write failure"):
        write_approval(
            ApprovalPreview(
                venue="polymarket",
                market_id="m-new",
                token_id="t-yes",
                side="BUY",
                outcome="YES",
                max_notional_usdc=10.0,
                limit_price=0.4,
                max_slippage_bps=50,
            ),
            path=approval_path,
            approver_id="operator-new",
            ts=datetime.now(tz=UTC),
            force=True,
        )

    assert json.loads(approval_path.read_text(encoding="utf-8")) == old_payload
    assert sidecar_path.read_text(encoding="utf-8") == original_sidecar_text


def test_write_approval_removes_partial_approval_json_when_new_write_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """If the approval JSON writer fails after creating/truncating the
    target, the helper must not leave a partial approval artifact that
    preflight/startup later has to interpret."""
    real_write = importlib.import_module("scripts.approve_first_order")._write_secret_file

    def _partial_failing_approval_write(target_path: Path, content: str) -> None:
        if not target_path.name.endswith(".meta.json"):
            target_path.write_text('{"approved": true, "market_id": ', encoding="utf-8")
            raise OSError("simulated partial approval write failure")
        real_write(target_path, content)

    monkeypatch.setattr(
        "scripts.approve_first_order._write_secret_file",
        _partial_failing_approval_write,
    )

    approval_path = tmp_path / "first-order.json"
    sidecar_path = Path(str(approval_path) + ".meta.json")

    with pytest.raises(OSError, match="simulated partial approval write failure"):
        write_approval(
            ApprovalPreview(
                venue="polymarket",
                market_id="m-new",
                token_id="t-yes",
                side="BUY",
                outcome="YES",
                max_notional_usdc=10.0,
                limit_price=0.4,
                max_slippage_bps=50,
            ),
            path=approval_path,
            approver_id="operator-new",
            ts=datetime.now(tz=UTC),
        )

    assert not approval_path.exists()
    assert not sidecar_path.exists()


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


def test_main_reports_normalized_approver_id(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    approval_path = tmp_path / "first-order.json"

    exit_code = main(
        [
            "--from-error",
            _GOLDEN_ERROR_MESSAGE,
            "--approver-id",
            "  operator-alice  ",
            "--path",
            str(approval_path),
        ]
    )

    stdout = capsys.readouterr().out
    sidecar = json.loads(
        Path(str(approval_path) + ".meta.json").read_text(encoding="utf-8")
    )
    assert exit_code == 0
    assert sidecar["approver_id"] == "operator-alice"
    assert "✓ Approver ID:         operator-alice" in stdout
    assert "  operator-alice  " not in stdout


def test_main_reports_absolute_approval_paths_for_relative_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    (repo_root / ".git").mkdir()
    secure_dir = tmp_path / "secure"
    monkeypatch.chdir(repo_root)
    approval_path = Path("../secure/first-order.json")
    expected_approval_path = secure_dir / "first-order.json"
    expected_sidecar_path = Path(str(expected_approval_path) + ".meta.json")

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

    stdout = capsys.readouterr().out
    assert exit_code == 0
    assert expected_approval_path.exists()
    assert expected_sidecar_path.exists()
    assert f"Wrote approval JSON: {expected_approval_path}" in stdout
    assert f"Wrote sidecar:       {expected_sidecar_path}" in stdout


def test_main_returns_operator_error_for_permissive_approval_parent(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    approval_dir = tmp_path / "shared-approvals"
    approval_dir.mkdir(mode=0o700)
    approval_dir.chmod(0o755)
    approval_path = approval_dir / "first-order.json"

    try:
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
        captured = capsys.readouterr()
    finally:
        approval_dir.chmod(0o700)

    assert exit_code == 2
    assert "approval artifact parent directory" in captured.err
    assert "too permissive" in captured.err
    assert not approval_path.exists()
    assert not Path(str(approval_path) + ".meta.json").exists()


def test_main_uses_approval_path_from_config_when_path_omitted(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(
        "PMS_POLYMARKET__FIRST_LIVE_ORDER_APPROVAL_PATH",
        raising=False,
    )
    secure_dir = tmp_path / "secure"
    secure_dir.mkdir(mode=0o700)
    approval_path = secure_dir / "first-order.json"
    config_path = tmp_path / "config.live.yaml"
    config_path.write_text(
        "\n".join(
            [
                "mode: live",
                "polymarket:",
                f"  first_live_order_approval_path: {approval_path}",
                "",
            ]
        ),
        encoding="utf-8",
    )

    exit_code = main(
        [
            "--from-error",
            _GOLDEN_ERROR_MESSAGE,
            "--approver-id",
            "operator-alice",
            "--config",
            str(config_path),
        ]
    )

    assert exit_code == 0
    assert approval_path.exists()
    assert Path(str(approval_path) + ".meta.json").exists()


def test_main_returns_operator_error_for_unsafe_config_path(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    secure_dir = tmp_path / "secure"
    secure_dir.mkdir(mode=0o700)
    approval_path = secure_dir / "first-order.json"
    config_target = tmp_path / "config.live.yaml"
    config_target.write_text(
        "\n".join(
            [
                "mode: live",
                "",
            ]
        ),
        encoding="utf-8",
    )
    config_path = tmp_path / "config-link.yaml"
    config_path.symlink_to(config_target)

    exit_code = main(
        [
            "--from-error",
            _GOLDEN_ERROR_MESSAGE,
            "--approver-id",
            "operator-alice",
            "--path",
            str(approval_path),
            "--config",
            str(config_path),
        ]
    )
    captured = capsys.readouterr()

    assert exit_code == 2
    assert "Config file cannot be read safely" in captured.err
    assert str(config_path) in captured.err
    assert not approval_path.exists()
    assert not Path(str(approval_path) + ".meta.json").exists()


def test_main_returns_operator_error_for_duplicate_config_key(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    secure_dir = tmp_path / "secure"
    secure_dir.mkdir(mode=0o700)
    approval_path = secure_dir / "first-order.json"
    config_path = tmp_path / "config.live.yaml"
    config_path.write_text(
        "\n".join(
            [
                f"local_secret_file: {secure_dir / 'forged-secrets.yaml'}",
                f"local_secret_file: {secure_dir / 'polymarket.local-secrets.yaml'}",
            ]
        ),
        encoding="utf-8",
    )

    exit_code = main(
        [
            "--from-error",
            _GOLDEN_ERROR_MESSAGE,
            "--approver-id",
            "operator-alice",
            "--path",
            str(approval_path),
            "--config",
            str(config_path),
        ]
    )
    captured = capsys.readouterr()

    assert exit_code == 2
    assert "duplicate YAML key: local_secret_file" in captured.err
    assert not approval_path.exists()
    assert not Path(str(approval_path) + ".meta.json").exists()


def test_main_rejects_force_path_reusing_local_secret_file_from_env_config(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    secure_dir = tmp_path / "secure"
    secure_dir.mkdir(mode=0o700)
    secret_path = secure_dir / "polymarket.local-secrets.yaml"
    original_secret_text = "polymarket:\n  private_key: original-secret\n"
    secret_path.write_text(original_secret_text, encoding="utf-8")
    secret_path.chmod(0o600)
    config_path = tmp_path / "config.live.yaml"
    config_path.write_text(
        "\n".join(
            [
                "mode: live",
                f"local_secret_file: {secret_path}",
                "",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("PMS_CONFIG_PATH", str(config_path))

    exit_code = main(
        [
            "--from-error",
            _GOLDEN_ERROR_MESSAGE,
            "--approver-id",
            "operator-alice",
            "--path",
            str(secret_path),
            "--force",
        ]
    )
    captured = capsys.readouterr()

    assert exit_code == 2
    assert "LIVE local secret file" in captured.err
    assert secret_path.read_text(encoding="utf-8") == original_secret_text
    assert not Path(str(secret_path) + ".meta.json").exists()
