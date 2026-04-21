from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any, cast

import pytest

from pms.research.specs import (
    BacktestDataset,
    BacktestExecutionConfig,
    BacktestSpec,
    ExecutionModel,
    RiskPolicy,
)


def _spec(*, market_id: str = "market-cancel") -> BacktestSpec:
    dataset = BacktestDataset(
        source="postgresql",
        version="outer-ring-v1",
        coverage_start=datetime(2026, 3, 1, tzinfo=UTC),
        coverage_end=datetime(2026, 3, 31, 23, 59, tzinfo=UTC),
        market_universe_filter={"venue": "polymarket", "market_ids": (market_id,)},
        data_quality_gaps=(),
    )
    return BacktestSpec(
        strategy_versions=(("alpha", "v1"),),
        dataset=dataset,
        execution_model=ExecutionModel.polymarket_paper(),
        risk_policy=RiskPolicy(
            max_position_notional_usdc=100.0,
            max_daily_drawdown_pct=2.5,
            min_order_size_usdc=1.0,
        ),
        date_range_start=datetime(2026, 3, 1, tzinfo=UTC),
        date_range_end=datetime(2026, 3, 31, 23, 59, tzinfo=UTC),
    )


class _CancellationConnection:
    def __init__(self) -> None:
        self._fetch_calls = 0

    async def fetch(self, query: str, *args: object) -> list[dict[str, object]]:
        del query, args
        self._fetch_calls += 1
        if self._fetch_calls == 1:
            return [
                {
                    "condition_id": "market-cancel",
                    "question": "Will CP02 replay cancel cleanly?",
                    "venue": "polymarket",
                    "resolves_at": None,
                }
            ]
        raise asyncio.CancelledError


class _LeakTrackingPool:
    def __init__(self, connection: _CancellationConnection) -> None:
        self._connection = connection
        self.acquire_count = 0
        self.release_count = 0
        self.currently_acquired = 0

    async def acquire(self) -> _CancellationConnection:
        self.acquire_count += 1
        self.currently_acquired += 1
        return self._connection

    async def release(self, connection: _CancellationConnection) -> None:
        assert connection is self._connection
        self.release_count += 1
        self.currently_acquired -= 1


@pytest.mark.asyncio
async def test_market_universe_replay_engine_releases_pool_connection_on_cancellation() -> None:
    from pms.research.replay import MarketUniverseReplayEngine

    pool = _LeakTrackingPool(_CancellationConnection())
    engine = MarketUniverseReplayEngine(pool=cast(Any, pool))

    with pytest.raises(asyncio.CancelledError):
        async for _ in engine.stream(_spec(), BacktestExecutionConfig(chunk_days=7)):
            pass

    assert pool.acquire_count == pool.release_count
    assert pool.currently_acquired == 0


class _EmptyMetadataConnection:
    async def fetch(self, query: str, *args: object) -> list[dict[str, object]]:
        del query, args
        return []


class _EmptyMetadataPool:
    def __init__(self) -> None:
        self._connection = _EmptyMetadataConnection()

    async def acquire(self) -> _EmptyMetadataConnection:
        return self._connection

    async def release(self, connection: _EmptyMetadataConnection) -> None:
        del connection


@pytest.mark.asyncio
async def test_market_universe_replay_engine_rejects_empty_filter_match() -> None:
    from pms.research.replay import MarketUniverseReplayEngine

    engine = MarketUniverseReplayEngine(pool=cast(Any, _EmptyMetadataPool()))

    with pytest.raises(ValueError, match="matched zero markets"):
        async for _ in engine.stream(_spec(), BacktestExecutionConfig(chunk_days=7)):
            pass
