from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest
from pydantic import SecretStr

from pms.api.app import _stop_alerting_if_started, create_app
from pms.alerting.discord import DiscordWebhookClient
from pms.config import DiscordSettings, PMSSettings
from pms.runner import Runner


@pytest.mark.asyncio
async def test_alerting_lifespan_spawns_and_cancels_subscription_task() -> None:
    runner = Runner(
        config=PMSSettings(
            auto_migrate_default_v2=False,
            discord=DiscordSettings(
                webhook_url=SecretStr("https://discord.example/webhooks/a/b")
            ),
        )
    )
    app = create_app(runner, auto_start=False)

    async with app.router.lifespan_context(app):
        task = app.state.alerting_task
        assert isinstance(task, asyncio.Task)
        assert not task.done()

    assert task.done()


@pytest.mark.asyncio
async def test_alerting_lifespan_uses_configured_fallback_alert_dir(
    tmp_path: Path,
) -> None:
    alert_dir = tmp_path / "discord-alerts"
    runner = Runner(
        config=PMSSettings(
            auto_migrate_default_v2=False,
            discord=DiscordSettings(
                webhook_url=SecretStr("https://discord.example/webhooks/a/b"),
                alert_dir=str(alert_dir),
            ),
        )
    )
    app = create_app(runner, auto_start=False)

    async with app.router.lifespan_context(app):
        client = app.state.discord_client
        assert isinstance(client, DiscordWebhookClient)
        assert client._alert_dir == alert_dir


class _ClosableClient:
    def __init__(self) -> None:
        self.closed = False

    async def aclose(self) -> None:
        self.closed = True


@pytest.mark.asyncio
async def test_alerting_shutdown_runs_remaining_cleanup_after_task_failure() -> None:
    async def boom() -> None:
        raise RuntimeError("eod failed")

    app = SimpleNamespace()
    client = _ClosableClient()
    alerting_stop_event = asyncio.Event()
    eod_stop_event = asyncio.Event()
    app.state = SimpleNamespace(
        alerting_stop_event=alerting_stop_event,
        eod_stop_event=eod_stop_event,
        eod_scheduler_task=asyncio.create_task(boom()),
        alerting_task=asyncio.create_task(asyncio.sleep(60)),
        discord_client=client,
    )
    await asyncio.sleep(0)

    with pytest.raises(RuntimeError, match="eod failed"):
        await _stop_alerting_if_started(cast(Any, app))

    assert eod_stop_event.is_set()
    assert alerting_stop_event.is_set()
    assert app.state.alerting_task.done()
    assert client.closed is True
