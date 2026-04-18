from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Literal, cast

import asyncpg
import pytest

from pms.config import DatabaseSettings, PMSSettings
from pms.core.enums import MarketStatus, RunMode
from pms.core.models import BookLevel, BookSnapshot, Market, MarketSignal, Token
from pms.runner import Runner
from pms.storage.eval_store import EvalStore
from pms.storage.feedback_store import FeedbackStore
from pms.storage.market_data_store import PostgresMarketDataStore
from tests.support.default_strategy_seed import seed_default_v1_strategy
from tests.support.fake_stores import InMemoryEvalStore, InMemoryFeedbackStore
from tests.support.strategy_catalog import seed_factor_catalog


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


class RepeatingSensor:
    def __init__(self, signals: list[MarketSignal], *, interval_s: float = 0.02) -> None:
        self._signals = list(signals)
        self._interval_s = interval_s

    def __aiter__(self) -> AsyncIterator[MarketSignal]:
        return self._iterate()

    async def _iterate(self) -> AsyncIterator[MarketSignal]:
        tick = 0
        while True:
            for signal in self._signals:
                yield replace(signal, fetched_at=signal.fetched_at + timedelta(milliseconds=20 * tick))
                tick += 1
                await asyncio.sleep(self._interval_s)


def _settings(*, factor_cadence_s: float) -> PMSSettings:
    assert PMS_TEST_DATABASE_URL is not None
    return PMSSettings(
        mode=RunMode.PAPER,
        auto_migrate_default_v2=True,
        factor_cadence_s=factor_cadence_s,
        database=DatabaseSettings(
            dsn=PMS_TEST_DATABASE_URL,
            pool_min_size=1,
            pool_max_size=2,
        ),
    )


def _signal(
    *,
    market_id: str,
    token_id: str,
    orderbook: dict[str, list[dict[str, float]]],
    fetched_at: datetime,
) -> MarketSignal:
    return MarketSignal(
        market_id=market_id,
        token_id=token_id,
        venue="polymarket",
        title="Will FactorService run on cadence?",
        yes_price=0.4,
        volume_24h=1000.0,
        resolves_at=datetime(2026, 4, 30, tzinfo=UTC),
        orderbook=orderbook,
        external_signal={},
        fetched_at=fetched_at,
        market_status=MarketStatus.OPEN.value,
    )


async def _seed_market(
    store: PostgresMarketDataStore,
    *,
    market_id: str,
    token_id: str,
    ts: datetime,
    orderbook: dict[str, list[dict[str, float]]],
) -> None:
    await store.write_market(
        Market(
            condition_id=market_id,
            slug=market_id,
            question=f"Will {market_id} persist factor rows?",
            venue="polymarket",
            resolves_at=datetime(2026, 4, 30, tzinfo=UTC),
            created_at=ts,
            last_seen_at=ts,
        )
    )
    await store.write_token(
        Token(
            token_id=token_id,
            condition_id=market_id,
            outcome="YES",
        )
    )
    levels: list[BookLevel] = []
    side: Literal["BUY", "SELL"]
    for side, entries in (("BUY", orderbook.get("bids", [])), ("SELL", orderbook.get("asks", []))):
        for entry in entries:
            levels.append(
                BookLevel(
                    snapshot_id=0,
                    market_id=market_id,
                    side=side,
                    price=entry["price"],
                    size=entry["size"],
                )
            )
    if levels:
        await store.write_book_snapshot(
            BookSnapshot(
                id=0,
                market_id=market_id,
                token_id=token_id,
                ts=ts,
                hash=f"{market_id}-hash",
                source="subscribe",
            ),
            levels,
        )


async def _seed_boot_prereqs(pg_pool: asyncpg.Pool) -> None:
    async with pg_pool.acquire() as connection:
        async with connection.transaction():
            await seed_factor_catalog(connection)
            await seed_default_v1_strategy(connection)


async def _wait_for_factor_count(
    pg_pool: asyncpg.Pool,
    *,
    market_id: str,
    minimum: int,
    timeout: float = 2.0,
) -> int:
    async with asyncio.timeout(timeout):
        while True:
            async with pg_pool.acquire() as connection:
                count = await connection.fetchval(
                    """
                    SELECT COUNT(*)
                    FROM factor_values
                    WHERE factor_id = 'orderbook_imbalance'
                      AND market_id = $1
                    """,
                    market_id,
                )
            assert isinstance(count, int)
            if count >= minimum:
                return count
            await asyncio.sleep(0.05)


@pytest.mark.asyncio(loop_scope="session")
async def test_factor_service_persists_orderbook_imbalance_on_cadence(
    pg_pool: asyncpg.Pool,
) -> None:
    await _seed_boot_prereqs(pg_pool)
    store = PostgresMarketDataStore(pg_pool)
    ts = datetime(2026, 4, 18, 9, 0, tzinfo=UTC)
    depth_orderbook: dict[str, list[dict[str, float]]]
    depth_orderbook = {
        "bids": [{"price": 0.39, "size": 100.0}],
        "asks": [{"price": 0.41, "size": 50.0}],
    }
    empty_orderbook: dict[str, list[dict[str, float]]]
    empty_orderbook = {"bids": [], "asks": []}

    await _seed_market(
        store,
        market_id="factor-depth",
        token_id="factor-depth-token",
        ts=ts,
        orderbook=depth_orderbook,
    )
    await _seed_market(
        store,
        market_id="factor-empty",
        token_id="factor-empty-token",
        ts=ts,
        orderbook=empty_orderbook,
    )

    runner = Runner(
        config=_settings(factor_cadence_s=0.1),
        sensors=[
            RepeatingSensor(
                [
                    _signal(
                        market_id="factor-depth",
                        token_id="factor-depth-token",
                        orderbook=depth_orderbook,
                        fetched_at=ts,
                    ),
                    _signal(
                        market_id="factor-empty",
                        token_id="factor-empty-token",
                        orderbook=empty_orderbook,
                        fetched_at=ts,
                    ),
                ]
            )
        ],
        eval_store=cast(EvalStore, InMemoryEvalStore()),
        feedback_store=cast(FeedbackStore, InMemoryFeedbackStore()),
    )

    try:
        await runner.start()
        populated_count = await _wait_for_factor_count(
            pg_pool,
            market_id="factor-depth",
            minimum=3,
        )
    finally:
        await runner.stop()

    async with pg_pool.acquire() as connection:
        empty_count = await connection.fetchval(
            """
            SELECT COUNT(*)
            FROM factor_values
            WHERE factor_id = 'orderbook_imbalance'
              AND market_id = 'factor-empty'
            """
        )

    assert populated_count >= 3
    assert empty_count == 0
