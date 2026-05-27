from __future__ import annotations

import asyncio
import json
import math
import os
import stat
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pms.actuator.adapters.polymarket import LiveOrderPreview


@dataclass(frozen=True, slots=True)
class JsonlFirstOrderAuditWriter:
    """Append-only JSONL sink for first-live-order operator events.

    Records the gate's match-keys (the same fields
    `_approval_payload_matches` checks at polymarket.py:1122-1144) plus
    timestamp, event name, and an optional approver_id supplied by the
    operator's tooling. One record per event, one event per `record_event`
    call. Parent directory is created on demand to mirror
    `LiveEmergencyAuditWriter` behaviour at live_emergency_audit.py:75.
    """

    path: Path

    async def record_event(
        self,
        *,
        event: str,
        preview: LiveOrderPreview,
        approver_id: str | None = None,
    ) -> None:
        record = _audit_record(
            event=event,
            preview=preview,
            approver_id=approver_id,
        )
        await asyncio.to_thread(_append_jsonl, self.path, record)


def _audit_record(
    *,
    event: str,
    preview: LiveOrderPreview,
    approver_id: str | None,
) -> dict[str, Any]:
    return {
        "ts": datetime.now(tz=UTC).isoformat(),
        "event": event,
        "approver_id": approver_id,
        "venue": preview.venue,
        "market_id": preview.market_id,
        "token_id": preview.token_id,
        "side": preview.side,
        "outcome": preview.outcome,
        "max_notional_usdc": preview.max_notional_usdc,
        "limit_price": preview.limit_price,
        "max_slippage_bps": preview.max_slippage_bps,
        "market_slug": preview.market_slug,
        "question": preview.question,
    }


def _append_jsonl(path: Path, record: Mapping[str, Any]) -> None:
    _prepare_private_parent_directory(path)
    _require_strict_json_value(record, path="$")
    line = (
        json.dumps(
            record,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    )
    _append_line_no_follow(path, line)


def _require_strict_json_value(value: object, *, path: str) -> None:
    if value is None or isinstance(value, (bool, int, str)):
        return
    if isinstance(value, float):
        if math.isfinite(value):
            return
        msg = f"{path} must be finite for first-order audit JSONL"
        raise ValueError(msg)
    if isinstance(value, Mapping):
        for key, item in value.items():
            key_label = str(key)
            child_path = key_label if path == "$" else f"{path}.{key_label}"
            _require_strict_json_value(item, path=child_path)
        return
    if isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            _require_strict_json_value(item, path=f"{path}[{index}]")
        return
    msg = f"{path} is not JSON-serializable for first-order audit JSONL"
    raise TypeError(msg)


def _append_line_no_follow(path: Path, line: str) -> None:
    _require_regular_file_or_absent(path)
    flags = os.O_WRONLY | os.O_CREAT | os.O_APPEND | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(path, flags, 0o600)
    try:
        _require_open_regular_single_link_file(fd, path)
        os.fchmod(fd, 0o600)
        _write_all(fd, line.encode("utf-8"))
        os.fsync(fd)
    finally:
        os.close(fd)
    _fsync_parent_directory(path)


def _write_all(fd: int, data: bytes) -> None:
    offset = 0
    while offset < len(data):
        written = os.write(fd, data[offset:])
        if written <= 0:
            raise OSError("first-order audit write made no progress")
        offset += written


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
        raise OSError(f"first-order audit path is not a regular file: {path}")
    if path_stat.st_nlink != 1:
        raise OSError(f"first-order audit path is not a single-link file: {path}")


def _prepare_private_parent_directory(path: Path) -> None:
    parent = path.parent
    try:
        parent_stat = parent.lstat()
    except FileNotFoundError:
        parent.mkdir(mode=0o700, parents=True, exist_ok=False)
        os.chmod(parent, 0o700)
        return
    if not stat.S_ISDIR(parent_stat.st_mode):
        raise OSError(f"first-order audit parent path is not a directory: {parent}")
    mode = stat.S_IMODE(parent_stat.st_mode)
    if mode & 0o077:
        raise OSError(
            f"first-order audit parent directory {parent} is too permissive; "
            f"run `chmod 700 {parent}`."
        )
    if not mode & stat.S_IWUSR:
        raise OSError(
            f"first-order audit parent directory {parent} is not owner-writable; "
            f"run `chmod 700 {parent}`."
        )


def _require_regular_file_or_absent(path: Path) -> None:
    try:
        path_stat = path.lstat()
    except FileNotFoundError:
        return
    mode = path_stat.st_mode
    if not stat.S_ISREG(mode):
        raise OSError(f"first-order audit path is not a regular file: {path}")
    if path_stat.st_nlink != 1:
        raise OSError(f"first-order audit path is not a single-link file: {path}")
