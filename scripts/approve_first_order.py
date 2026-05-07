"""Operator helper for the first-live-order approval gate (STO-10).

Given an `OperatorApprovalRequiredError` message (or the equivalent
preview fields) and an `approver_id`, write:

  1. The approval JSON at the configured path. Fields match exactly
     what `_approval_payload_matches`
     (`src/pms/actuator/adapters/polymarket.py`) checks, so the gate
     matches on the next decision without operator typo risk.
  2. The sidecar `<path>.meta.json` so
     `FileFirstLiveOrderGate.read_approver_id` populates the
     `approver_id` field on every audit-log event for this
     authorization.

Both files are written with mode 0o600 — the runner UID is the only
reader. By default the helper refuses to overwrite an existing approval
file (would clobber a still-pending authorization); pass `--force` to
override.

Operator usage:

    uv run python scripts/approve_first_order.py \\
        --from-error 'First Polymarket live order requires operator \\
            approval: venue=polymarket market=... token=... side=BUY \\
            outcome=YES max_notional_usdc=5.0 limit_price=0.4 \\
            max_slippage_bps=50' \\
        --approver-id alice@example \\
        --path /data/pms/first-order.json
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


@dataclass(frozen=True, slots=True)
class ApprovalPreview:
    venue: str
    market_id: str
    token_id: str | None
    side: str
    outcome: str
    max_notional_usdc: float
    limit_price: float
    max_slippage_bps: int


# Map error-message keys (e.g. `market=`) to ApprovalPreview field names.
# The actuator's f-string format uses the short keys for readability;
# the JSON the gate matches uses the long names. Centralise the mapping.
_KEY_ALIASES: dict[str, str] = {
    "venue": "venue",
    "market": "market_id",
    "token": "token_id",
    "side": "side",
    "outcome": "outcome",
    "max_notional_usdc": "max_notional_usdc",
    "limit_price": "limit_price",
    "max_slippage_bps": "max_slippage_bps",
}

_TOKEN_RE = re.compile(r"(\w+)=(\S+)")


def parse_preview_from_error(message: str) -> ApprovalPreview:
    """Parse an `OperatorApprovalRequiredError` message into an
    `ApprovalPreview`. Raises ValueError if any required field is
    missing — better than silently producing a partial JSON that would
    fail the gate match without a clear cause."""
    raw: dict[str, str] = {}
    for match in _TOKEN_RE.finditer(message):
        key = match.group(1)
        value = match.group(2)
        if key in _KEY_ALIASES:
            raw[_KEY_ALIASES[key]] = value

    required_fields = (
        "venue",
        "market_id",
        "token_id",
        "side",
        "outcome",
        "max_notional_usdc",
        "limit_price",
        "max_slippage_bps",
    )
    missing = [field for field in required_fields if field not in raw]
    if missing:
        raise ValueError(
            f"missing required preview fields in error message: {missing!r}"
        )

    token_id: str | None = raw["token_id"]
    if token_id == "None":
        token_id = None

    return ApprovalPreview(
        venue=raw["venue"],
        market_id=raw["market_id"],
        token_id=token_id,
        side=raw["side"],
        outcome=raw["outcome"],
        max_notional_usdc=float(raw["max_notional_usdc"]),
        limit_price=float(raw["limit_price"]),
        max_slippage_bps=int(raw["max_slippage_bps"]),
    )


def write_approval(
    preview: ApprovalPreview,
    *,
    path: Path,
    approver_id: str,
    ts: datetime,
    force: bool = False,
) -> tuple[Path, Path]:
    """Write the approval JSON and the sidecar `<path>.meta.json`.

    Returns the (approval_path, sidecar_path) pair. Both files have
    mode 0o600. Refuses to overwrite an existing approval file unless
    `force=True` — guards against clobbering a pending authorization.
    """
    sidecar_path = Path(str(path) + ".meta.json")

    if path.exists() and not force:
        raise FileExistsError(
            f"approval file already exists at {path}; pass force=True to overwrite"
        )

    path.parent.mkdir(parents=True, exist_ok=True)

    payload: dict[str, Any] = {"approved": True, **asdict(preview)}
    sidecar_payload: dict[str, Any] = {
        "approver_id": approver_id,
        "ts": ts.isoformat(),
    }

    # Write order matters: the gate matches on the approval JSON, then
    # `read_approver_id` reads the sidecar. If the approval JSON
    # appeared first, a running actuator could match between the two
    # writes and emit `approval_matched` with `approver_id: null`.
    # Writing the sidecar first guarantees identity is on disk before
    # the approval is observable to the gate. If the sidecar write
    # raises, the approval JSON is never written — the gate stays
    # denied and the operator's tool exits non-zero.
    _write_secret_file(
        sidecar_path,
        json.dumps(sidecar_payload, sort_keys=True),
    )
    _write_secret_file(path, json.dumps(payload, sort_keys=True))

    return path, sidecar_path


def _write_secret_file(path: Path, content: str) -> None:
    """Write `content` to `path` with mode 0o600.

    Uses `os.open` with the explicit mode rather than relying on the
    process umask, then sets mode again post-write defensively in case
    a wide umask masked the create mode."""
    flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC
    fd = os.open(path, flags, 0o600)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as file:
            file.write(content)
            file.write("\n")
    except BaseException:
        # If fdopen never claimed the descriptor we'd leak it; the
        # context manager above closes on success. Belt-and-braces:
        # ignore failure here — the original exception is what matters.
        try:
            os.close(fd)
        except OSError:
            pass
        raise
    os.chmod(path, 0o600)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Write a first-live-order approval JSON + sidecar.",
    )
    parser.add_argument(
        "--from-error",
        required=True,
        help="The OperatorApprovalRequiredError message to parse.",
    )
    parser.add_argument(
        "--approver-id",
        required=True,
        help="Identity of the human authorizing this order.",
    )
    parser.add_argument(
        "--path",
        default=os.environ.get("PMS_POLYMARKET__FIRST_LIVE_ORDER_APPROVAL_PATH"),
        help=(
            "Approval file path. Defaults to "
            "$PMS_POLYMARKET__FIRST_LIVE_ORDER_APPROVAL_PATH."
        ),
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite an existing approval file. Default: refuse.",
    )
    args = parser.parse_args(argv)

    if not args.path:
        parser.error(
            "no approval path provided; pass --path or set "
            "PMS_POLYMARKET__FIRST_LIVE_ORDER_APPROVAL_PATH"
        )

    try:
        preview = parse_preview_from_error(args.from_error)
    except ValueError as exc:
        # parser.error() is typed NoReturn (calls sys.exit), so no
        # follow-up return statement is needed and mypy strict (with
        # warn_unreachable) flags any dead code after.
        parser.error(f"could not parse error message: {exc}")

    ts = datetime.now(tz=UTC)
    approval_path, sidecar_path = write_approval(
        preview,
        path=Path(args.path),
        approver_id=args.approver_id,
        ts=ts,
        force=args.force,
    )

    print(f"✓ Wrote approval JSON: {approval_path}")
    print(f"✓ Wrote sidecar:       {sidecar_path}")
    print(f"✓ Approver ID:         {args.approver_id}")
    print(f"✓ Timestamp:           {ts.isoformat()}")
    print(
        "The first-order gate will match on the next decision and submit; "
        "consume() will then unlink both files."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
