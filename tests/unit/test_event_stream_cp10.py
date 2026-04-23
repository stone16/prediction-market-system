from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import pytest

from pms.api.routes.events import encode_sse_event
from pms.event_stream import RuntimeEvent, RuntimeEventBus


@pytest.mark.asyncio
async def test_runtime_event_bus_replays_after_last_event_id_and_broadcasts_live_events() -> None:
    bus = RuntimeEventBus(buffer_limit=5, subscriber_queue_limit=5)
    created_at = datetime(2026, 4, 23, 12, 0, tzinfo=UTC)

    first = await bus.publish(
        "sensor.signal",
        "Signal market-cp10 @ 41.0¢",
        created_at=created_at,
        market_id="market-cp10",
    )
    second = await bus.publish(
        "controller.decision",
        "Accepted BUY $25.00 on market-cp10",
        created_at=created_at,
        market_id="market-cp10",
        decision_id="decision-cp10",
    )

    replay, subscriber = await bus.subscribe(last_event_id=first.event_id)

    assert [item.event_id for item in replay] == [second.event_id]
    assert replay[0].decision_id == "decision-cp10"

    third = await bus.publish(
        "actuator.fill",
        "Filled BUY $25.00 on market-cp10",
        created_at=created_at,
        market_id="market-cp10",
        fill_id="fill-cp10",
    )
    live = await asyncio.wait_for(subscriber.get(), timeout=0.1)

    assert live == third
    await bus.unsubscribe(subscriber)


def test_encode_sse_event_frame_includes_id_event_and_json_payload() -> None:
    event = RuntimeEvent(
        event_id=7,
        event_type="error",
        created_at=datetime(2026, 4, 23, 12, 0, tzinfo=UTC),
        summary="actuator execution failed: boom",
        market_id="market-cp10",
        decision_id="decision-cp10",
        fill_id=None,
    )

    frame = encode_sse_event(event)

    assert frame.startswith("id: 7\n")
    assert "event: error\n" in frame
    compact = frame.replace(" ", "")
    assert '"summary":"actuatorexecutionfailed:boom"' in compact
    assert '"market_id":"market-cp10"' in compact
