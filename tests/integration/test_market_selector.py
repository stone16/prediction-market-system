from __future__ import annotations

from datetime import UTC, datetime, timedelta
import importlib
import os
from typing import Any, cast

import asyncpg
import pytest

from pms.core.models import Market, Token, Venue
from pms.storage.market_data_store import PostgresMarketDataStore
from pms.storage.strategy_registry import PostgresStrategyRegistry
from pms.strategies.aggregate import Strategy
from pms.strategies.projections import (
    EvalSpec,
    FactorCompositionStep,
    ForecasterSpec,
    MarketSelectionSpec,
    RiskParams,
    StrategyConfig,
)


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


class _AcquireConnection:
    def __init__(self, connection: asyncpg.Connection) -> None:
        self._connection = connection

    async def __aenter__(self) -> asyncpg.Connection:
        return self._connection

    async def __aexit__(self, *_: object) -> None:
        return None


class _SingleConnectionPool:
    def __init__(self, connection: asyncpg.Connection) -> None:
        self._connection = connection

    def acquire(self) -> _AcquireConnection:
        return _AcquireConnection(self._connection)


def _load_symbol(module_name: str, symbol_name: str) -> Any:
    try:
        module = importlib.import_module(module_name)
    except ModuleNotFoundError as exc:  # pragma: no cover - exercised in red phase
        pytest.fail(f"{module_name} is missing: {exc}")
    return getattr(module, symbol_name)


def _strategy(
    strategy_id: str,
    *,
    venue: str,
    resolution_time_max_horizon_days: int | None,
    volume_min_usdc: float = 500.0,
) -> Strategy:
    return Strategy(
        config=StrategyConfig(
            strategy_id=strategy_id,
            factor_composition=(
                FactorCompositionStep(
                    factor_id="factor-a",
                    role="weighted",
                    param="",
                    weight=1.0,
                    threshold=None,
                ),
            ),
            metadata=(("owner", "system"), ("tier", "default")),
        ),
        risk=RiskParams(
            max_position_notional_usdc=100.0,
            max_daily_drawdown_pct=2.5,
            min_order_size_usdc=1.0,
        ),
        eval_spec=EvalSpec(metrics=("brier",)),
        forecaster=ForecasterSpec(forecasters=(("rules", (("threshold", "0.55"),)),)),
        market_selection=MarketSelectionSpec(
            venue=venue,
            resolution_time_max_horizon_days=resolution_time_max_horizon_days,
            volume_min_usdc=volume_min_usdc,
        ),
    )


async def _seed_market(
    store: PostgresMarketDataStore,
    *,
    market_id: str,
    venue: Venue,
    resolves_at: datetime | None,
    volume_24h: float,
) -> None:
    created_at = datetime(2026, 4, 18, 9, 0, tzinfo=UTC)
    await store.write_market(
        Market(
            condition_id=market_id,
            slug=f"slug-{market_id}",
            question=f"Question {market_id}",
            venue=venue,
            resolves_at=resolves_at,
            created_at=created_at,
            last_seen_at=created_at,
            volume_24h=volume_24h,
        )
    )
    await store.write_token(
        Token(
            token_id=f"{market_id}-yes",
            condition_id=market_id,
            outcome="YES",
        )
    )
    await store.write_token(
        Token(
            token_id=f"{market_id}-no",
            condition_id=market_id,
            outcome="NO",
        )
    )


@pytest.mark.asyncio(loop_scope="session")
async def test_market_selector_returns_merged_asset_ids_from_active_strategies(
    db_conn: asyncpg.Connection,
) -> None:
    market_selector_cls = _load_symbol("pms.market_selection.selector", "MarketSelector")
    union_merge_policy_cls = _load_symbol("pms.market_selection.merge", "UnionMergePolicy")
    pool = cast(asyncpg.Pool, _SingleConnectionPool(db_conn))
    registry = PostgresStrategyRegistry(pool)
    store = PostgresMarketDataStore(pool)
    now = datetime.now(tz=UTC)

    await registry.create_version(
        _strategy(
            "polymarket-fast",
            venue="polymarket",
            resolution_time_max_horizon_days=30,
        )
    )
    await registry.create_version(
        _strategy(
            "kalshi-near",
            venue="kalshi",
            resolution_time_max_horizon_days=10,
            volume_min_usdc=1_000.0,
        )
    )
    await registry.create_strategy("inactive", metadata={"owner": "system"})

    await _seed_market(
        store,
        market_id="pm-fast",
        venue="polymarket",
        resolves_at=now + timedelta(days=5),
        volume_24h=800.0,
    )
    await _seed_market(
        store,
        market_id="pm-slow",
        venue="polymarket",
        resolves_at=now + timedelta(days=45),
        volume_24h=800.0,
    )
    await _seed_market(
        store,
        market_id="pm-past",
        venue="polymarket",
        resolves_at=now - timedelta(days=1),
        volume_24h=800.0,
    )
    await _seed_market(
        store,
        market_id="pm-low-volume",
        venue="polymarket",
        resolves_at=now + timedelta(days=2),
        volume_24h=100.0,
    )
    await _seed_market(
        store,
        market_id="ka-near",
        venue="kalshi",
        resolves_at=now + timedelta(days=4),
        volume_24h=1_500.0,
    )
    await _seed_market(
        store,
        market_id="ka-far",
        venue="kalshi",
        resolves_at=now + timedelta(days=20),
        volume_24h=1_500.0,
    )
    await _seed_market(
        store,
        market_id="ka-low-volume",
        venue="kalshi",
        resolves_at=now + timedelta(days=3),
        volume_24h=200.0,
    )

    selector = market_selector_cls(
        store=store,
        registry=registry,
        merge_policy=union_merge_policy_cls(),
    )

    result = await selector.select()

    assert result.asset_ids == [
        "ka-near-no",
        "ka-near-yes",
        "pm-fast-no",
        "pm-fast-yes",
    ]
    assert result.conflicts == []
