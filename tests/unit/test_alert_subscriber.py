from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import pytest

from pms.alerting.subscriber import run_alerting_subscription
from pms.event_stream import RuntimeEventBus


class RecordingDiscordClient:
    def __init__(self) -> None:
        self.messages: list[str] = []

    async def send(
        self,
        content: str,
        *,
        embed: dict[str, object] | None = None,
    ) -> bool:
        del embed
        self.messages.append(content)
        return True


@pytest.mark.asyncio
async def test_alert_subscriber_converts_halt_event_to_discord_message() -> None:
    bus = RuntimeEventBus()
    client = RecordingDiscordClient()
    stop = asyncio.Event()
    task = asyncio.create_task(run_alerting_subscription(bus, client, stop_event=stop))
    await asyncio.sleep(0)

    try:
        await bus.publish(
            "pms.halt.drawdown_circuit_breaker",
            "Auto-halt drawdown_circuit_breaker: drawdown_circuit_breaker",
            created_at=datetime(2026, 5, 6, 8, 0, tzinfo=UTC),
            market_id="market-alert",
            decision_id="decision-alert",
        )
        deadline = asyncio.get_running_loop().time() + 1.0
        while not client.messages:
            if asyncio.get_running_loop().time() > deadline:
                raise AssertionError("subscriber did not deliver alert")
            await asyncio.sleep(0.01)
    finally:
        stop.set()
        await asyncio.wait_for(task, timeout=1.0)

    assert "drawdown_circuit_breaker" in client.messages[0]
    assert "decision-alert" in client.messages[0]
