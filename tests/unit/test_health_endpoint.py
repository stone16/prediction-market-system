from __future__ import annotations

import asyncio

import httpx
import pytest

from pms.api.app import create_app
from pms.config import PMSSettings
from pms.runner import Runner


@pytest.mark.asyncio
async def test_health_liveness_is_independent_from_runner_readiness() -> None:
    runner = Runner(config=PMSSettings(auto_migrate_default_v2=False))
    app = create_app(runner, auto_start=False)
    transport = httpx.ASGITransport(app=app)

    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            health = await client.get("/health")
            readiness = await client.get("/readiness")

    assert health.status_code == 200
    assert health.json()["status"] == "ok"
    assert readiness.status_code == 503
    assert readiness.json()["status"] == "not_ready"
    assert readiness.json()["checks"]["halt_subscriber"] == "not_started"


@pytest.mark.asyncio
async def test_readiness_reports_ready_when_runner_and_alerting_are_running() -> None:
    runner = Runner(config=PMSSettings(auto_migrate_default_v2=False))
    app = create_app(runner, auto_start=False)
    app.state.alerting_task = asyncio.create_task(asyncio.sleep(60))
    app.state.eod_scheduler_task = asyncio.create_task(asyncio.sleep(60))
    app.state.runner_started_for_test = True

    try:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.get("/readiness")
    finally:
        app.state.alerting_task.cancel()
        app.state.eod_scheduler_task.cancel()

    assert response.status_code == 200
    assert response.json()["status"] == "ready"
