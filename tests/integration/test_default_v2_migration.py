from __future__ import annotations

from dataclasses import replace
import os
from typing import cast

import asyncpg
import pytest

from pms.config import DatabaseSettings, PMSSettings
from pms.core.enums import RunMode
from pms.factors.defaults import DEFAULT_STRATEGY_COMPOSITION
from pms.runner import Runner
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
from pms.strategies.versioning import (
    compute_strategy_version_id,
    serialize_strategy_config_json,
)
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


def _settings(*, auto_migrate_default_v2: bool) -> PMSSettings:
    assert PMS_TEST_DATABASE_URL is not None
    return PMSSettings(
        mode=RunMode.PAPER,
        auto_migrate_default_v2=auto_migrate_default_v2,
        database=DatabaseSettings(
            dsn=PMS_TEST_DATABASE_URL,
            pool_min_size=1,
            pool_max_size=2,
        ),
    )


def _default_v1_strategy() -> Strategy:
    return Strategy(
        config=StrategyConfig(
            strategy_id="default",
            factor_composition=(
                FactorCompositionStep(
                    factor_id="factor-a",
                    role="weighted",
                    param="",
                    weight=0.6,
                    threshold=None,
                ),
                FactorCompositionStep(
                    factor_id="factor-b",
                    role="weighted",
                    param="",
                    weight=0.4,
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
        eval_spec=EvalSpec(metrics=("brier", "pnl", "fill_rate")),
        forecaster=ForecasterSpec(
            forecasters=(
                ("rules", (("threshold", "0.55"),)),
                ("stats", (("window", "15m"),)),
            )
        ),
        market_selection=MarketSelectionSpec(
            venue="polymarket",
            resolution_time_max_horizon_days=7,
            volume_min_usdc=500.0,
        ),
    )


async def _seed_default_v1(connection: asyncpg.Connection) -> None:
    strategy = _default_v1_strategy()
    async with connection.transaction():
        await connection.execute("SET CONSTRAINTS ALL DEFERRED")
        await seed_factor_catalog(
            connection,
            factor_ids=(
                "fair_value_spread",
                "subset_pricing_violation",
                "metaculus_prior",
                "yes_count",
                "no_count",
            ),
        )
        await connection.execute(
            """
            INSERT INTO strategies (strategy_id, active_version_id)
            VALUES ('default', 'default-v1')
            """
        )
        await connection.execute(
            """
            INSERT INTO strategy_versions (
                strategy_version_id,
                strategy_id,
                config_json
            ) VALUES (
                'default-v1',
                'default',
                $1::jsonb
            )
            """,
            serialize_strategy_config_json(*strategy.snapshot()),
        )


async def _boot_runner(
    pg_pool: asyncpg.Pool,
    *,
    auto_migrate_default_v2: bool,
) -> None:
    runner = Runner(
        config=_settings(auto_migrate_default_v2=auto_migrate_default_v2),
        sensors=[],
    )
    runner.bind_pg_pool(pg_pool)
    try:
        await runner.start()
    finally:
        await runner.stop()


async def _expected_default_v2_version_id(pg_pool: asyncpg.Pool) -> str:
    strategy = await PostgresStrategyRegistry(pg_pool).get_by_id("default")
    assert strategy is not None
    migrated = Strategy(
        config=replace(
            strategy.config,
            factor_composition=cast(
                tuple[FactorCompositionStep, ...],
                DEFAULT_STRATEGY_COMPOSITION,
            ),
        ),
        risk=strategy.risk,
        eval_spec=strategy.eval_spec,
        forecaster=strategy.forecaster,
        market_selection=strategy.market_selection,
    )
    return compute_strategy_version_id(*migrated.snapshot())


@pytest.mark.asyncio(loop_scope="session")
async def test_default_v2_migration_is_idempotent_and_respects_disable_flag(
    pg_pool: asyncpg.Pool,
) -> None:
    async with pg_pool.acquire() as connection:
        await _seed_default_v1(connection)

    expected_version_id = await _expected_default_v2_version_id(pg_pool)

    await _boot_runner(pg_pool, auto_migrate_default_v2=True)

    async with pg_pool.acquire() as connection:
        first_active_version_id = await connection.fetchval(
            "SELECT active_version_id FROM strategies WHERE strategy_id = 'default'"
        )
        first_version_count = await connection.fetchval(
            "SELECT COUNT(*) FROM strategy_versions WHERE strategy_id = 'default'"
        )
        first_strategy_factor_count = await connection.fetchval(
            "SELECT COUNT(*) FROM strategy_factors WHERE strategy_id = 'default'"
        )

    await _boot_runner(pg_pool, auto_migrate_default_v2=True)

    async with pg_pool.acquire() as connection:
        second_active_version_id = await connection.fetchval(
            "SELECT active_version_id FROM strategies WHERE strategy_id = 'default'"
        )
        second_version_count = await connection.fetchval(
            "SELECT COUNT(*) FROM strategy_versions WHERE strategy_id = 'default'"
        )
        second_strategy_factor_count = await connection.fetchval(
            "SELECT COUNT(*) FROM strategy_factors WHERE strategy_id = 'default'"
        )

    await _boot_runner(pg_pool, auto_migrate_default_v2=False)

    async with pg_pool.acquire() as connection:
        third_active_version_id = await connection.fetchval(
            "SELECT active_version_id FROM strategies WHERE strategy_id = 'default'"
        )
        third_version_count = await connection.fetchval(
            "SELECT COUNT(*) FROM strategy_versions WHERE strategy_id = 'default'"
        )
        third_strategy_factor_count = await connection.fetchval(
            "SELECT COUNT(*) FROM strategy_factors WHERE strategy_id = 'default'"
        )

    assert first_active_version_id == expected_version_id
    assert first_version_count == 2
    assert first_strategy_factor_count >= 3
    assert second_active_version_id == expected_version_id
    assert second_version_count == first_version_count
    assert second_strategy_factor_count == first_strategy_factor_count
    assert third_active_version_id == expected_version_id
    assert third_version_count == first_version_count
    assert third_strategy_factor_count == first_strategy_factor_count
