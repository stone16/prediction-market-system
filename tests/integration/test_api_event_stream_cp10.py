from __future__ import annotations

import asyncio
import json
import os
from datetime import UTC, datetime

import httpx
import pytest

from pms.api.app import create_app
from pms.config import PMSSettings
from pms.runner import Runner


pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.environ.get("PMS_RUN_INTEGRATION") != "1",
        reason="set PMS_RUN_INTEGRATION=1 to run integration tests",
    ),
]


def _runner() -> Runner:
    return Runner(
        config=PMSSettings(
            auto_migrate_default_v2=False,
        )
    )


async def _read_events(
    response: httpx.Response,
    *,
    count: int,
) -> list[dict[str, object]]:
    events: list[dict[str, object]] = []
    current_event: str | None = None
    current_id: str | None = None
    async for line in response.aiter_lines():
        if not line:
            continue
        if line.startswith("id: "):
            current_id = line[4:]
            continue
        if line.startswith("event: "):
            current_event = line[7:]
            continue
        if not line.startswith("data: "):
            continue
        payload = json.loads(line[6:])
        payload["stream_event"] = current_event
        payload["stream_id"] = current_id
        events.append(payload)
        if len(events) >= count:
            break
    return events


@pytest.mark.asyncio
async def test_stream_events_replays_from_last_event_id_and_emits_live_frames() -> None:
    runner = _runner()
    app = create_app(runner, auto_start=False)
    created_at = datetime(2026, 4, 23, 12, 0, tzinfo=UTC)

    first = await runner.event_bus.publish(
        "sensor.signal",
        "Signal market-cp10 @ 41.0¢",
        created_at=created_at,
        market_id="market-cp10",
    )
    await runner.event_bus.publish(
        "controller.decision",
        "Accepted BUY $25.00 on market-cp10",
        created_at=created_at,
        market_id="market-cp10",
        decision_id="decision-cp10",
    )

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
        timeout=2.0,
    ) as client:
        async with client.stream("GET", f"/stream/events?last_event_id={first.event_id}") as response:
            live_publish = asyncio.create_task(
                runner.event_bus.publish(
                    "actuator.fill",
                    "Filled BUY $25.00 on market-cp10",
                    created_at=created_at,
                    market_id="market-cp10",
                    decision_id="decision-cp10",
                    fill_id="fill-cp10",
                )
            )
            events = await asyncio.wait_for(_read_events(response, count=2), timeout=1.0)
            await live_publish

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/event-stream")
    assert [item["stream_event"] for item in events] == [
        "controller.decision",
        "actuator.fill",
    ]
    assert events[0]["decision_id"] == "decision-cp10"
    assert events[1]["fill_id"] == "fill-cp10"
