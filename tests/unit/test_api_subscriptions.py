from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, cast

import httpx
import pytest

from pms.api.app import create_app
from pms.config import PMSSettings, RiskSettings
from pms.core.enums import RunMode
from pms.market_selection.subscription_controller import SensorSubscriptionController


@dataclass
class _RunnerState:
    mode: RunMode
    runner_started_at: datetime | None = None


@dataclass
class _RunnerDouble:
    config: PMSSettings
    state: _RunnerState
    tasks: tuple[asyncio.Future[None], ...] = ()
    _subscription_controller: SensorSubscriptionController | None = None

    @property
    def subscription_controller(self) -> SensorSubscriptionController | None:
        return self._subscription_controller


def _settings(mode: RunMode) -> PMSSettings:
    return PMSSettings(
        mode=mode,
        auto_migrate_default_v2=False,
        risk=RiskSettings(
            max_position_per_market=1000.0,
            max_total_exposure=10_000.0,
        ),
    )


def _runner(
    *,
    mode: RunMode,
    started: bool,
    current_asset_ids: list[str],
    last_updated_at: datetime | None,
    running: bool,
) -> _RunnerDouble:
    controller = SensorSubscriptionController(_SubscriptionSink())
    setattr(controller, "_current_asset_ids", frozenset(current_asset_ids))
    setattr(controller, "_last_updated_at", last_updated_at)
    future: asyncio.Future[None] | None = None
    if running:
        loop = asyncio.get_running_loop()
        future = loop.create_future()
    return _RunnerDouble(
        config=_settings(mode),
        state=_RunnerState(
            mode=mode,
            runner_started_at=(
                datetime(2026, 4, 16, tzinfo=UTC) if started else None
            ),
        ),
        tasks=(() if future is None else (future,)),
        _subscription_controller=controller,
    )


class _SubscriptionSink:
    async def update_subscription(self, asset_ids: list[str]) -> None:
        del asset_ids
        return None


async def _get_subscriptions_payload(runner: _RunnerDouble) -> dict[str, Any]:
    app = create_app(cast(Any, runner))
    transport = httpx.ASGITransport(app=app)

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/subscriptions")

    assert response.status_code == 200
    return cast(dict[str, Any], response.json())


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("mode", "started", "running", "controller_ids", "controller_updated_at"),
    [
        (
            RunMode.LIVE,
            False,
            False,
            ["asset-a", "asset-b"],
            datetime(2026, 4, 17, 8, 30, tzinfo=UTC),
        ),
        (
            RunMode.BACKTEST,
            True,
            True,
            ["asset-a", "asset-b"],
            datetime(2026, 4, 17, 8, 30, tzinfo=UTC),
        ),
    ],
)
async def test_get_subscriptions_returns_empty_payload_when_inactive(
    mode: RunMode,
    started: bool,
    running: bool,
    controller_ids: list[str],
    controller_updated_at: datetime,
) -> None:
    payload = await _get_subscriptions_payload(
        _runner(
            mode=mode,
            started=started,
            current_asset_ids=controller_ids,
            last_updated_at=controller_updated_at,
            running=running,
        )
    )

    assert payload == {
        "asset_ids": [],
        "count": 0,
        "last_updated_at": None,
    }


@pytest.mark.asyncio
async def test_get_subscriptions_returns_runner_controller_state_when_live_and_running() -> None:
    last_updated_at = datetime(2026, 4, 17, 8, 30, tzinfo=UTC)
    payload = await _get_subscriptions_payload(
        _runner(
            mode=RunMode.LIVE,
            started=True,
            current_asset_ids=["asset-b", "asset-a"],
            last_updated_at=last_updated_at,
            running=True,
        )
    )

    assert payload == {
        "asset_ids": ["asset-a", "asset-b"],
        "count": 2,
        "last_updated_at": last_updated_at.isoformat(),
    }
