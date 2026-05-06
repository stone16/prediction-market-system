from __future__ import annotations

import asyncio

import pytest

from pms.api.app import create_app
from pms.config import DiscordSettings, PMSSettings
from pms.runner import Runner


@pytest.mark.asyncio
async def test_alerting_lifespan_spawns_and_cancels_subscription_task() -> None:
    runner = Runner(
        config=PMSSettings(
            auto_migrate_default_v2=False,
            discord=DiscordSettings(webhook_url="https://discord.example/webhooks/a/b"),
        )
    )
    app = create_app(runner, auto_start=False)

    async with app.router.lifespan_context(app):
        task = app.state.alerting_task
        assert isinstance(task, asyncio.Task)
        assert not task.done()

    assert task.done()
