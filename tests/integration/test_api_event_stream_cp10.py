from __future__ import annotations

import asyncio
import json
import os
from collections.abc import AsyncIterator
from datetime import UTC, datetime

import pytest
from fastapi.routing import APIRoute
from starlette.requests import Request

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
    body_iterator: AsyncIterator[str | bytes],
    *,
    count: int,
) -> list[dict[str, object]]:
    events: list[dict[str, object]] = []
    current_event: str | None = None
    current_id: str | None = None
    async for chunk in body_iterator:
        text = chunk.decode("utf-8") if isinstance(chunk, bytes) else str(chunk)
        for line in text.splitlines():
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
                return events
    return events


def _event_route(app: object) -> APIRoute:
    for route in app.router.routes:  # type: ignore[attr-defined]
        if isinstance(route, APIRoute) and route.path == "/stream/events":
            return route
    raise AssertionError("missing /stream/events route")


async def _receive() -> dict[str, object]:
    return {"type": "http.request", "body": b"", "more_body": False}


def _request(last_event_id: int) -> Request:
    return Request(
        {
            "type": "http",
            "asgi": {"version": "3.0"},
            "http_version": "1.1",
            "method": "GET",
            "path": "/stream/events",
            "query_string": f"last_event_id={last_event_id}".encode("utf-8"),
            "headers": [],
            "client": ("127.0.0.1", 12345),
            "server": ("test", 80),
            "scheme": "http",
        },
        receive=_receive,
    )


@pytest.mark.asyncio
async def test_stream_events_replays_from_last_event_id_and_emits_live_frames() -> None:
    runner = _runner()
    app = create_app(runner, auto_start=False)
    route = _event_route(app)
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

    response = await route.endpoint(
        request=_request(first.event_id),
        last_event_id=first.event_id,
    )

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
    try:
        events = await asyncio.wait_for(_read_events(response.body_iterator, count=2), timeout=1.0)
    finally:
        await response.body_iterator.aclose()
    await live_publish

    assert response.status_code == 200
    assert response.media_type == "text/event-stream"
    assert [item["stream_event"] for item in events] == [
        "controller.decision",
        "actuator.fill",
    ]
    assert events[0]["decision_id"] == "decision-cp10"
    assert events[1]["fill_id"] == "fill-cp10"
