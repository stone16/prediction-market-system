from __future__ import annotations

import os

import asyncpg
import pytest

from pms.config import DatabaseSettings, PMSSettings
from pms.core.enums import RunMode
from pms.runner import Runner
from pms.strategies.aggregate import Strategy
from pms.strategies.defaults import DEFAULT_STRATEGY_COMPOSITION
from pms.strategies.projections import (
    EvalSpec,
    FactorCompositionStep,
    ForecasterSpec,
    MarketSelectionSpec,
    RiskParams,
    StrategyConfig,
)
from pms.strategies.versioning import serialize_strategy_config_json


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


def _raw_factor_ids() -> set[str]:
    return {
        step.factor_id
        for step in DEFAULT_STRATEGY_COMPOSITION
        if step.role not in {"runtime_probability", "blend_weighted"}
    }


@pytest.mark.asyncio(loop_scope="session")
async def test_strategy_factors_populate_when_default_v2_migration_is_enabled(
    pg_pool: asyncpg.Pool,
) -> None:
    async with pg_pool.acquire() as connection:
        await _seed_default_v1(connection)

    await _boot_runner(pg_pool, auto_migrate_default_v2=True)

    async with pg_pool.acquire() as connection:
        active_version_id = await connection.fetchval(
            "SELECT active_version_id FROM strategies WHERE strategy_id = 'default'"
        )
        rows = await connection.fetch(
            """
            SELECT factor_id, direction
            FROM strategy_factors
            WHERE strategy_id = 'default'
            ORDER BY factor_id
            """
        )

    assert active_version_id != "default-v1"
    assert {row["factor_id"] for row in rows} == _raw_factor_ids()
    assert {row["direction"] for row in rows} == {"long"}


@pytest.mark.asyncio(loop_scope="session")
async def test_strategy_factors_remain_empty_when_default_v2_migration_is_disabled(
    pg_pool: asyncpg.Pool,
) -> None:
    async with pg_pool.acquire() as connection:
        await _seed_default_v1(connection)

    await _boot_runner(pg_pool, auto_migrate_default_v2=False)

    async with pg_pool.acquire() as connection:
        active_version_id = await connection.fetchval(
            "SELECT active_version_id FROM strategies WHERE strategy_id = 'default'"
        )
        count = await connection.fetchval("SELECT COUNT(*) FROM strategy_factors")

    assert active_version_id == "default-v1"
    assert count == 0
