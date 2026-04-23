from __future__ import annotations

import asyncio
import os
from datetime import UTC, datetime, timedelta
from typing import cast

import asyncpg
import httpx
import pytest

from pms.actuator.adapters.paper import PaperActuator
from pms.actuator.executor import ActuatorExecutor
from pms.actuator.feedback import ActuatorFeedback
from pms.actuator.risk import RiskManager
from pms.api.app import create_app
from pms.config import PMSSettings, RiskSettings
from pms.core.enums import RunMode, Side, TimeInForce
from pms.core.models import Opportunity, TradeDecision
from pms.runner import Runner
from pms.storage.dedup_store import PgDedupStore
from pms.storage.feedback_store import FeedbackStore
from tests.support.fake_stores import InMemoryFeedbackStore


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


def _settings(*, api_token: str | None = None) -> PMSSettings:
    return PMSSettings(
        mode=RunMode.PAPER,
        auto_migrate_default_v2=False,
        api_token=api_token,
        risk=RiskSettings(
            max_position_per_market=1_000.0,
            max_total_exposure=10_000.0,
        ),
    )


def _decision() -> TradeDecision:
    return TradeDecision(
        decision_id="decision-cp08",
        market_id="market-cp08",
        token_id="token-cp08-yes",
        venue="polymarket",
        side=Side.BUY.value,
        notional_usdc=25.0,
        order_type="limit",
        max_slippage_bps=50,
        stop_conditions=["cp08"],
        prob_estimate=0.67,
        expected_edge=0.18,
        time_in_force=TimeInForce.GTC,
        opportunity_id="opportunity-cp08",
        strategy_id="default",
        strategy_version_id="default-v1",
        limit_price=0.41,
        action=Side.BUY.value,
        model_id="model-cp08",
    )


def _opportunity() -> Opportunity:
    created_at = datetime(2026, 4, 23, 10, 0, tzinfo=UTC)
    return Opportunity(
        opportunity_id="opportunity-cp08",
        market_id="market-cp08",
        token_id="token-cp08-yes",
        side="yes",
        selected_factor_values={"edge": 0.18, "liquidity": 0.04},
        expected_edge=0.18,
        rationale="cp08 rationale",
        target_size_usdc=25.0,
        expiry=created_at + timedelta(minutes=15),
        staleness_policy="cp08",
        strategy_id="default",
        strategy_version_id="default-v1",
        created_at=created_at,
        factor_snapshot_hash="snapshot-cp08",
        composition_trace={"kind": "integration"},
    )


def _runner(pg_pool: asyncpg.Pool, *, api_token: str | None = None) -> Runner:
    runner = Runner(config=_settings(api_token=api_token))
    runner.bind_pg_pool(pg_pool)
    runner.actuator_executor = ActuatorExecutor(
        adapter=PaperActuator(
            orderbooks={
                "market-cp08": {
                    "bids": [{"price": 0.39, "size": 100.0}],
                    "asks": [{"price": 0.41, "size": 100.0}],
                }
            }
        ),
        risk=RiskManager(
            RiskSettings(
                max_position_per_market=1_000.0,
                max_total_exposure=10_000.0,
            )
        ),
        feedback=ActuatorFeedback(cast(FeedbackStore, InMemoryFeedbackStore())),
        dedup_store=PgDedupStore(pg_pool),
    )
    return runner


async def _seed_decision(runner: Runner) -> None:
    opportunity = _opportunity()
    decision = _decision()
    await runner.opportunity_store.insert(opportunity)
    await runner.decision_store.insert(
        decision,
        factor_snapshot_hash=opportunity.factor_snapshot_hash,
        created_at=opportunity.created_at,
        expires_at=cast(datetime, opportunity.expiry),
    )


async def _decision_status(
    pg_pool: asyncpg.Pool,
    decision_id: str,
) -> str | None:
    async with pg_pool.acquire() as connection:
        return cast(
            str | None,
            await connection.fetchval(
                "SELECT status FROM decisions WHERE decision_id = $1",
                decision_id,
            ),
        )


async def _fill_count(
    pg_pool: asyncpg.Pool,
    decision_id: str,
) -> int:
    async with pg_pool.acquire() as connection:
        count = await connection.fetchval(
            """
            SELECT COUNT(*)
            FROM fill_payloads
            WHERE payload->>'decision_id' = $1
            """,
            decision_id,
        )
    return int(cast(int, count))


def _mark_controller_done(runner: Runner) -> None:
    runner._controller_task = asyncio.create_task(asyncio.sleep(0))  # noqa: SLF001


@pytest.mark.asyncio(loop_scope="session")
async def test_accept_endpoint_is_idempotent_and_creates_single_fill(
    pg_pool: asyncpg.Pool,
) -> None:
    runner = _runner(pg_pool)
    await _seed_decision(runner)
    app = create_app(runner, auto_start=False)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        accepted = await client.post(
            "/decisions/decision-cp08/accept",
            json={"factor_snapshot_hash": "snapshot-cp08"},
        )

    assert accepted.status_code == 200
    assert accepted.json() == {
        "decision_id": "decision-cp08",
        "status": "accepted",
        "fill_id": None,
    }
    assert await _decision_status(pg_pool, "decision-cp08") == "accepted"

    _mark_controller_done(runner)
    await asyncio.wait_for(runner._actuator_loop(), timeout=1.0)  # noqa: SLF001
    assert await _fill_count(pg_pool, "decision-cp08") == 1

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        duplicate = await client.post(
            "/decisions/decision-cp08/accept",
            json={"factor_snapshot_hash": "snapshot-cp08"},
        )

    assert duplicate.status_code == 200
    assert duplicate.json() == {
        "decision_id": "decision-cp08",
        "status": "accepted",
        "fill_id": None,
    }
    assert await _fill_count(pg_pool, "decision-cp08") == 1


@pytest.mark.asyncio(loop_scope="session")
async def test_accept_endpoint_returns_409_and_list_endpoint_can_include_opportunity(
    pg_pool: asyncpg.Pool,
) -> None:
    runner = _runner(pg_pool)
    await _seed_decision(runner)
    app = create_app(runner, auto_start=False)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        stale = await client.post(
            "/decisions/decision-cp08/accept",
            json={"factor_snapshot_hash": "snapshot-stale"},
        )
        decisions = await client.get("/decisions?include=opportunity&limit=1")

    assert stale.status_code == 409
    assert stale.json() == {
        "detail": "market_changed",
        "current_factor_snapshot_hash": "snapshot-cp08",
    }

    assert decisions.status_code == 200
    payload = decisions.json()
    assert len(payload) == 1
    assert payload[0]["decision_id"] == "decision-cp08"
    assert payload[0]["opportunity"]["selected_factor_values"] == {
        "edge": 0.18,
        "liquidity": 0.04,
    }
    assert payload[0]["opportunity"]["expected_edge"] == 0.18


@pytest.mark.asyncio(loop_scope="session")
async def test_accept_endpoint_requires_bearer_token_when_configured(
    pg_pool: asyncpg.Pool,
) -> None:
    runner = _runner(pg_pool, api_token="testtoken")
    await _seed_decision(runner)
    app = create_app(runner, auto_start=False)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        missing = await client.post(
            "/decisions/decision-cp08/accept",
            json={"factor_snapshot_hash": "snapshot-cp08"},
        )
        accepted = await client.post(
            "/decisions/decision-cp08/accept",
            headers={"Authorization": "Bearer testtoken"},
            json={"factor_snapshot_hash": "snapshot-cp08"},
        )

    assert missing.status_code == 401
    assert missing.json() == {"detail": "Missing or invalid API token."}
    assert accepted.status_code == 200
    assert accepted.json() == {
        "decision_id": "decision-cp08",
        "status": "accepted",
        "fill_id": None,
    }
