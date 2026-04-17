from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import asyncpg
import pytest
import yaml

from pms.config import DatabaseSettings, PMSSettings, RiskSettings
from pms.core.enums import MarketStatus, RunMode
from pms.core.models import MarketSignal
from pms.runner import Runner
from pms.storage.eval_store import EvalStore
from pms.storage.feedback_store import FeedbackStore
from tests.support.fake_stores import InMemoryEvalStore, InMemoryFeedbackStore


WORKFLOW_PATH = Path(".github/workflows/ci.yml")
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


@dataclass
class HoldingSensor:
    def __aiter__(self) -> AsyncIterator[MarketSignal]:
        return self._iterate()

    async def _iterate(self) -> AsyncIterator[MarketSignal]:
        while True:
            await asyncio.sleep(60.0)
            yield _signal()


def _settings() -> PMSSettings:
    assert PMS_TEST_DATABASE_URL is not None
    return PMSSettings(
        mode=RunMode.BACKTEST,
        database=DatabaseSettings(
            dsn=PMS_TEST_DATABASE_URL,
            pool_min_size=1,
            pool_max_size=2,
        ),
        risk=RiskSettings(
            max_position_per_market=1000.0,
            max_total_exposure=10_000.0,
        ),
    )


def _signal() -> MarketSignal:
    return MarketSignal(
        market_id="runner-pool-integration",
        token_id="yes-token",
        venue="polymarket",
        title="Will the runner release PostgreSQL connections?",
        yes_price=0.42,
        volume_24h=1_000.0,
        resolves_at=datetime(2026, 4, 30, tzinfo=UTC),
        orderbook={
            "bids": [{"price": 0.41, "size": 100.0}],
            "asks": [{"price": 0.43, "size": 100.0}],
        },
        external_signal={"fair_value": 0.55, "resolved_outcome": 1.0},
        fetched_at=datetime(2026, 4, 16, tzinfo=UTC),
        market_status=MarketStatus.OPEN.value,
    )


async def _count_other_connections(connection: asyncpg.Connection) -> int:
    count = await connection.fetchval(
        """
        SELECT COUNT(*)
        FROM pg_stat_activity
        WHERE datname = current_database()
          AND pid <> pg_backend_pid()
        """
    )
    assert isinstance(count, int)
    return count


async def _wait_for_connection_count(
    connection: asyncpg.Connection,
    predicate: Any,
    *,
    timeout: float = 5.0,
) -> int:
    async with asyncio.timeout(timeout):
        while True:
            count = await _count_other_connections(connection)
            if predicate(count):
                return count
            await asyncio.sleep(0.05)


def test_ci_workflow_postgres_service_matches_compose() -> None:
    compose = yaml.safe_load(Path("compose.yml").read_text())
    workflow = yaml.safe_load(WORKFLOW_PATH.read_text())

    assert compose["services"]["postgres"]["image"] == "postgres:16"
    assert (
        workflow["jobs"]["test"]["services"]["postgres"]["image"]
        == compose["services"]["postgres"]["image"]
    )


@pytest.mark.asyncio(loop_scope="session")
async def test_runner_start_stop_leaves_no_leaked_postgres_connections(
    tmp_path: Path,
) -> None:
    assert PMS_TEST_DATABASE_URL is not None

    runner = Runner(
        config=_settings(),
        sensors=[HoldingSensor()],
        eval_store=cast(EvalStore, InMemoryEvalStore()),
        feedback_store=cast(FeedbackStore, InMemoryFeedbackStore()),
    )
    monitor = await asyncpg.connect(PMS_TEST_DATABASE_URL)

    try:
        baseline_connections = await _count_other_connections(monitor)

        await runner.start()
        during_connections = await _wait_for_connection_count(
            monitor,
            lambda count: count >= baseline_connections + 1,
        )

        await runner.stop()
        after_connections = await _wait_for_connection_count(
            monitor,
            lambda count: count == baseline_connections,
        )
    finally:
        if runner.pg_pool is not None or runner.tasks:
            await runner.stop()
        await monitor.close()

    assert during_connections >= baseline_connections + 1
    assert after_connections == baseline_connections
