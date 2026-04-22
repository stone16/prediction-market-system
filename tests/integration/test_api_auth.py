from __future__ import annotations

import logging
import os
from datetime import UTC, datetime
from typing import Any, cast

import httpx
import pytest

from pms.api.app import create_app
from pms.config import PMSSettings
from pms.core.enums import FeedbackSource, FeedbackTarget, RunMode
from pms.core.models import Feedback
from pms.runner import Runner
from pms.storage.feedback_store import FeedbackStore
from tests.support.fake_stores import InMemoryFeedbackStore


pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.environ.get("PMS_RUN_INTEGRATION") != "1",
        reason="set PMS_RUN_INTEGRATION=1 to run integration tests",
    ),
]


def _feedback(feedback_id: str) -> Feedback:
    return Feedback(
        feedback_id=feedback_id,
        target=FeedbackTarget.CONTROLLER.value,
        source=FeedbackSource.EVALUATOR.value,
        message="threshold crossed",
        severity="warning",
        created_at=datetime(2026, 4, 22, tzinfo=UTC),
        category="brier:model-a",
    )


def _app(api_token: str | None) -> Any:
    runner = Runner(
        config=PMSSettings(
            mode=RunMode.BACKTEST,
            auto_migrate_default_v2=False,
            api_token=api_token,
        ),
        feedback_store=cast(
            FeedbackStore,
            InMemoryFeedbackStore([_feedback("fb-pending")]),
        ),
    )
    return create_app(runner, auto_start=False)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("path", "request_kwargs"),
    [
        ("/research/backtest", {"content": ""}),
        ("/research/backtest/any-id/compare", {"json": {"denominator": "backtest_set"}}),
        ("/feedback/fb-pending/resolve", {}),
        ("/config", {"json": {"mode": "backtest"}}),
        ("/run/start", {}),
        ("/run/stop", {}),
    ],
)
async def test_mutating_routes_require_bearer_token_when_configured(
    path: str,
    request_kwargs: dict[str, Any],
) -> None:
    app = _app(api_token="expected-token")

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        missing = await client.post(path, **request_kwargs)
        wrong = await client.post(
            path,
            headers={"Authorization": "Bearer wrong-token"},
            **request_kwargs,
        )

    assert missing.status_code == 401
    assert missing.json() == {"detail": "Missing or invalid API token."}
    assert wrong.status_code == 401
    assert wrong.json() == {"detail": "Missing or invalid API token."}


@pytest.mark.asyncio
async def test_mutating_route_accepts_correct_bearer_token() -> None:
    app = _app(api_token="expected-token")

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        response = await client.post(
            "/config",
            headers={"Authorization": "Bearer expected-token"},
            json={"mode": "backtest"},
        )

    assert response.status_code == 200
    assert response.json() == {"mode": "backtest"}


@pytest.mark.asyncio
async def test_get_routes_remain_open_when_api_token_is_configured() -> None:
    app = _app(api_token="expected-token")

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        status = await client.get("/status")
        feedback = await client.get("/feedback")

    assert status.status_code == 200
    assert feedback.status_code == 200


@pytest.mark.asyncio
async def test_mutating_routes_remain_open_when_api_token_is_unset() -> None:
    app = _app(api_token=None)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        response = await client.post("/config", json={"mode": "backtest"})

    assert response.status_code == 200
    assert response.json() == {"mode": "backtest"}


@pytest.mark.asyncio
async def test_auth_logging_redacts_bearer_token(
    caplog: pytest.LogCaptureFixture,
) -> None:
    app = _app(api_token="expected-token")
    caplog.set_level(logging.WARNING, logger="pms.api.auth")

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        response = await client.post(
            "/config",
            headers={"Authorization": "Bearer super-secret-token"},
            json={"mode": "backtest"},
        )

    assert response.status_code == 401
    assert "super-secret-token" not in caplog.text
    assert "Bearer [REDACTED]" in caplog.text
