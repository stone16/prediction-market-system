from __future__ import annotations

import asyncio
import json
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
    `_approval_payload_matches` checks at polymarket.py:976-998) plus
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
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as file:
        file.write(json.dumps(record, sort_keys=True, separators=(",", ":")))
        file.write("\n")
