from __future__ import annotations

import asyncio
import os
from datetime import UTC, datetime, timedelta
from typing import Any, cast

import asyncpg
import httpx
import pytest

from pms.api.app import create_app
from pms.config import DatabaseSettings, PMSSettings
from pms.core.enums import RunMode
from pms.core.models import Market, Token
from pms.market_selection.subscription_controller import SensorSubscriptionController
from pms.runner import Runner
from pms.storage.market_data_store import PostgresMarketDataStore


PMS_TEST_DATABASE_URL = os.environ.get("PMS_TEST_DATABASE_URL")

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.environ.get("PMS_RUN_INTEGRATION") != "1",
        reason="set PMS_RUN_INTEGRATION=1 to run PostgreSQL integration tests",
    ),
    pytest.mark.skipif(
        PMS_TEST_DATABASE_URL is None,
        reason="set PMS_TEST_DATABASE_URL to the compose-backed PostgreSQL URI",
    ),
]


class _SubscriptionSink:
    async def update_subscription(self, asset_ids: list[str]) -> None:
        del asset_ids
        return None


def _settings() -> PMSSettings:
    return PMSSettings(
        mode=RunMode.LIVE,
        auto_migrate_default_v2=False,
        database=DatabaseSettings(dsn=cast(str, PMS_TEST_DATABASE_URL)),
    )


async def _seed_market(
    store: PostgresMarketDataStore,
    *,
    market_id: str,
    question: str,
    resolves_at: datetime | None,
    created_at: datetime,
    updated_at: datetime,
    volume_24h: float,
) -> tuple[str, str]:
    await store.write_market(
        Market(
            condition_id=market_id,
            slug=f"slug-{market_id}",
            question=question,
            venue="polymarket",
            resolves_at=resolves_at,
            created_at=created_at,
            last_seen_at=updated_at,
            volume_24h=volume_24h,
        )
    )
    yes_token_id = f"{market_id}-yes"
    no_token_id = f"{market_id}-no"
    await store.write_token(
        Token(token_id=yes_token_id, condition_id=market_id, outcome="YES")
    )
    await store.write_token(
        Token(token_id=no_token_id, condition_id=market_id, outcome="NO")
    )
    return yes_token_id, no_token_id


def _client(pg_pool: asyncpg.Pool) -> httpx.AsyncClient:
    runner = Runner(config=_settings())
    runner.bind_pg_pool(pg_pool)
    runner.state.mode = RunMode.LIVE
    runner.state.runner_started_at = datetime(2026, 4, 23, 8, 0, tzinfo=UTC)
    controller = SensorSubscriptionController(_SubscriptionSink())
    setattr(
        controller,
        "_current_asset_ids",
        frozenset(
            {
                "market-00-yes",
                "market-03-no",
                "market-09-yes",
            }
        ),
    )
    setattr(
        controller,
        "_last_updated_at",
        datetime(2026, 4, 23, 8, 30, tzinfo=UTC),
    )
    runner._subscription_controller = controller  # noqa: SLF001
    runner._controller_task = asyncio.get_running_loop().create_future()  # type: ignore[assignment]
    app = create_app(runner)
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    )


@pytest.mark.asyncio(loop_scope="session")
async def test_get_markets_returns_20_active_rows_with_subscription_state(
    pg_pool: asyncpg.Pool,
) -> None:
    store = PostgresMarketDataStore(pg_pool)
    now = datetime(2026, 4, 23, 12, 0, tzinfo=UTC)

    for index in range(20):
        market_id = f"market-{index:02d}"
        await _seed_market(
            store,
            market_id=market_id,
            question=f"Will checkpoint {index:02d} pass?",
            resolves_at=now + timedelta(days=1 + index),
            created_at=now - timedelta(days=7),
            updated_at=now - timedelta(minutes=index),
            volume_24h=2_000.0 - index,
        )

    await _seed_market(
        store,
        market_id="market-expired",
        question="Should not be returned",
        resolves_at=now - timedelta(days=2),
        created_at=now - timedelta(days=14),
        updated_at=now - timedelta(days=1),
        volume_24h=9_999.0,
    )

    async with _client(pg_pool) as client:
        response = await client.get("/markets?limit=20&offset=0")

    assert response.status_code == 200
    payload = response.json()

    assert payload["limit"] == 20
    assert payload["offset"] == 0
    assert payload["total"] == 20
    assert len(payload["markets"]) == 20
    assert payload["markets"][0] == {
        "market_id": "market-00",
        "question": "Will checkpoint 00 pass?",
        "venue": "polymarket",
        "volume_24h": 2000.0,
        "updated_at": now.isoformat(),
        "yes_token_id": "market-00-yes",
        "no_token_id": "market-00-no",
        "subscribed": True,
    }
    assert payload["markets"][3]["subscribed"] is True
    assert payload["markets"][9]["subscribed"] is True
    assert all(row["market_id"] != "market-expired" for row in payload["markets"])
