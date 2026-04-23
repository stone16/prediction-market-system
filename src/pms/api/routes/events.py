from __future__ import annotations

import json

from pms.event_stream import RuntimeEvent


def encode_sse_event(event: RuntimeEvent) -> str:
    payload = {
        "event_id": event.event_id,
        "event_type": event.event_type,
        "created_at": event.created_at.isoformat(),
        "summary": event.summary,
        "market_id": event.market_id,
        "decision_id": event.decision_id,
        "fill_id": event.fill_id,
    }
    return (
        f"id: {event.event_id}\n"
        f"event: {event.event_type}\n"
        f"data: {json.dumps(payload, separators=(',', ':'))}\n\n"
    )
