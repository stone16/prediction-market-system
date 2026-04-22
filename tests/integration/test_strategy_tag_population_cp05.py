from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime
import os
from typing import Any, cast

import asyncpg
import pytest

from pms.config import DatabaseSettings, PMSSettings, RiskSettings
from pms.controller.calibrators.netcal import NetcalCalibrator
from pms.controller.pipeline import ControllerPipeline
from pms.controller.router import Router
from pms.controller.sizers.kelly import KellySizer
from pms.core.enums import RunMode
from pms.core.models import MarketSignal
from pms.market_selection.merge import StrategyMarketSet
from pms.runner import Runner
from pms.strategies.aggregate import Strategy
from pms.strategies.projections import (
    ActiveStrategy,
    EvalSpec,
    FactorCompositionStep,
    ForecasterSpec,
    MarketSelectionSpec,
    RiskParams,
    StrategyConfig,
)
from pms.storage.eval_store import EvalStore
from pms.storage.feedback_store import FeedbackStore
from pms.storage.strategy_registry import PostgresStrategyRegistry


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


class StaticSelector:
    async def select(self) -> Any:
        return type("SelectResult", (), {"asset_ids": ["shared-token"]})()

    async def select_per_strategy(self) -> list[StrategyMarketSet]:
        return [
            StrategyMarketSet(
                strategy_id="alpha",
                strategy_version_id="alpha-v1",
                asset_ids=frozenset({"shared-token"}),
            ),
            StrategyMarketSet(
                strategy_id="beta",
                strategy_version_id="beta-v1",
                asset_ids=frozenset({"shared-token"}),
            ),
        ]


class _NoopFactorService:
    def __init__(self, **kwargs: Any) -> None:
        del kwargs

    async def run(self) -> None:
        return None


class StaticForecaster:
    def predict(self, signal: MarketSignal) -> tuple[float, float, str]:
        del signal
        return (0.67, 0.9, "strategy-tag-test")

    async def forecast(self, signal: MarketSignal) -> float:
        del signal
        return 0.67


class DeterministicFactory:
    def __init__(self, settings: PMSSettings) -> None:
        self.settings = settings

    def build_many(
        self,
        strategies: list[ActiveStrategy],
    ) -> dict[str, ControllerPipeline]:
        return {
            strategy.strategy_id: ControllerPipeline(
                strategy_id=strategy.strategy_id,
                strategy_version_id=strategy.strategy_version_id,
                forecasters=[StaticForecaster()],
                calibrator=NetcalCalibrator(),
                sizer=KellySizer(
                    risk=RiskSettings(
                        max_position_per_market=500.0,
                        max_total_exposure=10_000.0,
                    )
                ),
                router=Router(),
                settings=self.settings,
            )
            for strategy in strategies
        }


class FilteredRegistry(PostgresStrategyRegistry):
    async def list_active_strategies(self) -> list[Any]:
        strategies = await super().list_active_strategies()
        return [item for item in strategies if item.strategy_id in {"alpha", "beta"}]


def _settings() -> PMSSettings:
    assert PMS_TEST_DATABASE_URL is not None
    return PMSSettings(
        mode=RunMode.PAPER,
        auto_migrate_default_v2=False,
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
        market_id="market-cp05",
        token_id="shared-token",
        venue="polymarket",
        title="Will CP05 persist per-strategy eval tags?",
        yes_price=0.4,
        volume_24h=1000.0,
        resolves_at=datetime(2026, 4, 30, tzinfo=UTC),
        orderbook={
            "bids": [{"price": 0.39, "size": 1000.0}],
            "asks": [{"price": 0.41, "size": 1000.0}],
        },
        external_signal={"fair_value": 0.7, "resolved_outcome": 1.0},
        fetched_at=datetime(2026, 4, 19, tzinfo=UTC),
        market_status="open",
    )


def _strategy(strategy_id: str, *, drawdown_pct: float) -> Strategy:
    return Strategy(
        config=StrategyConfig(
            strategy_id=strategy_id,
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
            metadata=(("owner", "system"), ("tier", strategy_id)),
        ),
        risk=RiskParams(
            max_position_notional_usdc=100.0,
            max_daily_drawdown_pct=drawdown_pct,
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


@pytest.mark.asyncio(loop_scope="session")
async def test_runner_persists_distinct_eval_record_strategy_pairs_for_two_strategies(
    pg_pool: asyncpg.Pool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = PostgresStrategyRegistry(pg_pool)
    default_version = await registry.create_version(_strategy("default", drawdown_pct=2.5))
    alpha_version = await registry.create_version(_strategy("alpha", drawdown_pct=3.0))
    beta_version = await registry.create_version(_strategy("beta", drawdown_pct=3.5))
    assert default_version.strategy_version_id

    async def fake_ensure_factor_catalog(pool: object, *, factor_ids: object = None) -> None:
        del pool, factor_ids

    monkeypatch.setattr("pms.runner.ensure_factor_catalog", fake_ensure_factor_catalog)
    monkeypatch.setattr("pms.runner.FactorService", _NoopFactorService)
    monkeypatch.setattr("pms.runner.MarketSelector", lambda *args, **kwargs: StaticSelector())
    monkeypatch.setattr("pms.runner.PostgresStrategyRegistry", FilteredRegistry)

    runner = Runner(
        config=_settings(),
        sensors=[SequenceSensor([_signal()])],
        eval_store=EvalStore(),
        feedback_store=FeedbackStore(),
    )
    runner.bind_pg_pool(pg_pool)
    runner._controller_factory = cast(Any, DeterministicFactory(_settings()))

    try:
        await runner.start()
        await runner.wait_until_idle()
    finally:
        await runner.stop()

    async with pg_pool.acquire() as connection:
        rows = await connection.fetch(
            """
            SELECT DISTINCT strategy_id, strategy_version_id
            FROM eval_records
            ORDER BY strategy_id, strategy_version_id
            """
        )

    assert {
        (row["strategy_id"], row["strategy_version_id"])
        for row in rows
    } == {
        ("alpha", alpha_version.strategy_version_id),
        ("beta", beta_version.strategy_version_id),
    }
