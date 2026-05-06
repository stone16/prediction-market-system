from __future__ import annotations

import asyncio
import os
from pathlib import Path
from typing import cast

import httpx
import pytest

from pms.actuator.executor import ActuatorExecutor
from pms.actuator.feedback import ActuatorFeedback
from pms.actuator.risk import RiskManager
from pms.alerting.discord import DiscordWebhookClient
from pms.alerting.subscriber import run_alerting_subscription
from pms.config import RiskSettings
from pms.event_stream import RuntimeEventBus
from pms.storage.feedback_store import FeedbackStore
from tests.support.fake_stores import InMemoryFeedbackStore
from tests.unit.test_executor_publishes_halt import (
    RecordingAdapter,
    _decision,
    _portfolio,
)


pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.environ.get("PMS_RUN_INTEGRATION") != "1",
        reason="set PMS_RUN_INTEGRATION=1 to run integration tests",
    ),
]


async def test_alerting_chain_real_auto_halt_to_discord(tmp_path: Path) -> None:
    posts: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        posts.append(request)
        return httpx.Response(204)

    bus = RuntimeEventBus()
    replay, capture = await bus.subscribe()
    assert replay == []
    client = DiscordWebhookClient(
        "https://discord.example/webhooks/app/token",
        alert_dir=tmp_path,
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )
    stop = asyncio.Event()
    subscriber_task = asyncio.create_task(run_alerting_subscription(bus, client, stop_event=stop))
    await asyncio.sleep(0)
    manager = RiskManager(RiskSettings(max_drawdown_pct=20.0))
    executor = ActuatorExecutor(
        adapter=RecordingAdapter(),
        risk=manager,
        feedback=ActuatorFeedback(cast(FeedbackStore, InMemoryFeedbackStore())),
        event_bus=bus,
    )

    try:
        await executor.execute(_decision(), _portfolio())
        event = capture.get_nowait()
        deadline = asyncio.get_running_loop().time() + 5.0
        while not posts:
            if asyncio.get_running_loop().time() > deadline:
                raise AssertionError("discord post not received")
            await asyncio.sleep(0.01)
    finally:
        stop.set()
        await asyncio.wait_for(subscriber_task, timeout=1.0)

    assert manager.halt_events[-1].state.reason == "drawdown_circuit_breaker"
    assert event.event_type == "pms.halt.drawdown_circuit_breaker"
    assert len(posts) == 1
    assert "drawdown_circuit_breaker" in posts[0].content.decode()
