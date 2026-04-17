from __future__ import annotations

import os
from collections.abc import AsyncIterator
from datetime import UTC, datetime

import asyncpg
import pytest

from pms.config import DatabaseSettings, PMSSettings, RiskSettings
from pms.core.enums import MarketStatus, RunMode
from pms.core.models import MarketSignal
from pms.runner import Runner
from pms.storage.eval_store import EvalStore
from pms.storage.feedback_store import FeedbackStore


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


class SequenceSensor:
    def __init__(self, signals: list[MarketSignal]) -> None:
        self._signals = list(signals)

    def __aiter__(self) -> AsyncIterator[MarketSignal]:
        return self._iterate()

    async def _iterate(self) -> AsyncIterator[MarketSignal]:
        for signal in self._signals:
            yield signal


def _settings() -> PMSSettings:
    assert PMS_TEST_DATABASE_URL is not None
    return PMSSettings(
        mode=RunMode.PAPER,
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


def _signal(
    *,
    market_id: str,
    orderbook: dict[str, object],
    external_signal: dict[str, object],
) -> MarketSignal:
    return MarketSignal(
        market_id=market_id,
        token_id="yes-token",
        venue="polymarket",
        title="Will CP06 default-tag runtime writes?",
        yes_price=0.4,
        volume_24h=1000.0,
        resolves_at=datetime(2026, 4, 30, tzinfo=UTC),
        orderbook=orderbook,
        external_signal=external_signal,
        fetched_at=datetime(2026, 4, 17, tzinfo=UTC),
        market_status=MarketStatus.OPEN.value,
    )


async def _count_null_strategy_tags(connection: asyncpg.Connection, table: str) -> int:
    query = f"""
    SELECT COUNT(*)
    FROM {table}
    WHERE strategy_id IS NULL OR strategy_version_id IS NULL
    """
    count = await connection.fetchval(query)
    assert isinstance(count, int)
    return count


async def _strategy_pairs(
    connection: asyncpg.Connection,
    table: str,
) -> set[tuple[str, str]]:
    query = f"""
    SELECT DISTINCT strategy_id, strategy_version_id
    FROM {table}
    WHERE strategy_id IS NOT NULL AND strategy_version_id IS NOT NULL
    """
    rows = await connection.fetch(query)
    return {
        (row["strategy_id"], row["strategy_version_id"])
        for row in rows
    }


@pytest.mark.asyncio(loop_scope="session")
async def test_runner_tags_inner_ring_rows_with_default_strategy(
    pg_pool: asyncpg.Pool,
) -> None:
    async with pg_pool.acquire() as connection:
        await connection.execute(
            """
            BEGIN;
            SET CONSTRAINTS ALL DEFERRED;
            INSERT INTO strategies (strategy_id, active_version_id)
            VALUES ('default', 'default-v1')
            ON CONFLICT (strategy_id) DO NOTHING;
            INSERT INTO strategy_versions (
                strategy_version_id,
                strategy_id,
                config_json
            ) VALUES (
                'default-v1',
                'default',
                '{"config":{},"risk":{},"eval":{},"forecaster":{},"market_selection":{}}'::jsonb
            )
            ON CONFLICT (strategy_version_id) DO NOTHING;
            COMMIT;
            """
        )

    runner = Runner(
        config=_settings(),
        sensors=[
            SequenceSensor(
                [
                    _signal(
                        market_id="paper-empty-book",
                        orderbook={"bids": [], "asks": []},
                        external_signal={"fair_value": 0.7, "resolved_outcome": 1.0},
                    ),
                    _signal(
                        market_id="paper-with-depth",
                        orderbook={
                            "bids": [{"price": 0.39, "size": 250.0}],
                            "asks": [{"price": 0.41, "size": 250.0}],
                        },
                        external_signal={"metaculus_prob": 0.9, "resolved_outcome": 1.0},
                    ),
                ]
            )
        ],
        eval_store=EvalStore(),
        feedback_store=FeedbackStore(),
    )

    try:
        await runner.start()
        await runner.wait_until_idle()
    finally:
        await runner.stop()

    async with pg_pool.acquire() as connection:
        strategies_count = await connection.fetchval(
            "SELECT COUNT(*) FROM strategies WHERE strategy_id = 'default'"
        )
        versions_count = await connection.fetchval(
            """
            SELECT COUNT(*)
            FROM strategy_versions
            WHERE strategy_id = 'default' AND strategy_version_id = 'default-v1'
            """
        )
        feedback_count = await connection.fetchval("SELECT COUNT(*) FROM feedback")
        eval_count = await connection.fetchval("SELECT COUNT(*) FROM eval_records")
        orders_count = await connection.fetchval("SELECT COUNT(*) FROM orders")
        fills_count = await connection.fetchval("SELECT COUNT(*) FROM fills")

        counts = {
            "feedback": feedback_count,
            "eval_records": eval_count,
            "orders": orders_count,
            "fills": fills_count,
        }
        null_counts = {
            table: await _count_null_strategy_tags(connection, table)
            for table in counts
        }
        tagged_counts = {
            table: await connection.fetchval(
                f"""
                SELECT COUNT(*)
                FROM {table}
                WHERE strategy_id = 'default' AND strategy_version_id = 'default-v1'
                """
            )
            for table in counts
        }
        strategy_pairs = {
            table: await _strategy_pairs(connection, table)
            for table in counts
        }

    assert strategies_count == 1
    assert versions_count == 1
    assert counts["feedback"] > 0
    assert counts["eval_records"] > 0
    assert null_counts == {
        "feedback": 0,
        "eval_records": 0,
        "orders": 0,
        "fills": 0,
    }
    assert tagged_counts["feedback"] > 0
    assert tagged_counts["eval_records"] > 0
    assert strategy_pairs["feedback"] == {("default", "default-v1")}
    assert strategy_pairs["eval_records"] == {("default", "default-v1")}
    assert strategy_pairs["orders"] == set()
    assert strategy_pairs["fills"] == set()
