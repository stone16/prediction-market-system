from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

from pms.actuator.executor import ActuatorExecutor
from pms.config import ControllerSettings, PMSSettings, RiskSettings
from pms.controller.calibrators.netcal import NetcalCalibrator
from pms.controller.factor_snapshot import FactorSnapshot
from pms.controller.outcome_tokens import OutcomeTokens
from pms.controller.pipeline import ControllerPipeline
from pms.controller.router import Router
from pms.controller.sizers.kelly import KellySizer
from pms.core.enums import OrderStatus, RunMode
from pms.core.models import MarketSignal, OrderState, Portfolio, TradeDecision
from pms.runner import Runner
from tests.support.fake_stores import InMemoryOpportunityStore


FIXTURE_PATH = Path("tests/fixtures/polymarket_7day_synthetic.jsonl")


class StaticForecaster:
    def predict(self, signal: MarketSignal) -> tuple[float, float, str]:
        del signal
        return (0.67, 0.9, "runner opportunity")

    async def forecast(self, signal: MarketSignal) -> float:
        del signal
        return 0.67


class IdleSensor:
    def __aiter__(self) -> AsyncIterator[MarketSignal]:
        return self._iterate()

    async def _iterate(self) -> AsyncIterator[MarketSignal]:
        while True:
            await asyncio.sleep(60.0)
            yield _signal()


class StaticLowForecaster:
    def predict(self, signal: MarketSignal) -> tuple[float, float, str]:
        del signal
        return (0.30, 0.9, "runner skip diagnostic")

    async def forecast(self, signal: MarketSignal) -> float:
        del signal
        return 0.30


class RecordingFactorReader:
    async def snapshot(
        self,
        *,
        market_id: str,
        as_of: datetime,
        required: object,
        strategy_id: str,
        strategy_version_id: str,
    ) -> FactorSnapshot:
        del market_id, as_of, required, strategy_id, strategy_version_id
        return FactorSnapshot(
            values={("snapshot_probability", ""): 0.30},
            missing_factors=(),
            snapshot_hash="snapshot-runner-skip",
        )


class NoNoTokenResolver:
    async def resolve(
        self,
        *,
        market_id: str,
        signal_token_id: str | None,
    ) -> OutcomeTokens:
        del market_id, signal_token_id
        return OutcomeTokens(yes_token_id="runner-token", no_token_id=None)


def _signal() -> MarketSignal:
    return MarketSignal(
        market_id="runner-cp02",
        token_id="runner-token",
        venue="polymarket",
        title="Will CP02 runner emit opportunities?",
        yes_price=0.4,
        volume_24h=1_000.0,
        resolves_at=datetime(2026, 4, 30, tzinfo=UTC),
        orderbook={"bids": [], "asks": []},
        external_signal={"fair_value": 0.61},
        fetched_at=datetime(2026, 4, 19, tzinfo=UTC),
        market_status="open",
    )


def _settings() -> PMSSettings:
    return PMSSettings(
        mode=RunMode.BACKTEST,
        auto_migrate_default_v2=False,
        risk=RiskSettings(
            max_position_per_market=1_000.0,
            max_total_exposure=10_000.0,
        ),
    )


async def _wait_for_opportunities(runner: Runner, count: int) -> None:
    deadline = asyncio.get_running_loop().time() + 2.0
    while len(runner.state.opportunities) < count:
        if asyncio.get_running_loop().time() >= deadline:
            msg = f"timed out waiting for {count} opportunities"
            raise AssertionError(msg)
        await asyncio.sleep(0.01)


async def _wait_for_diagnostics(runner: Runner, count: int) -> None:
    deadline = asyncio.get_running_loop().time() + 2.0
    while len(runner.state.controller_diagnostics) < count:
        if asyncio.get_running_loop().time() >= deadline:
            msg = f"timed out waiting for {count} controller diagnostics"
            raise AssertionError(msg)
        await asyncio.sleep(0.01)


@pytest.mark.asyncio
async def test_runner_forwards_opportunity_id_to_actuator() -> None:
    captured_decisions: list[TradeDecision] = []

    async def fake_execute(
        decision: TradeDecision,
        portfolio: Portfolio | None = None,
        *,
        dedup_acquired: bool = False,
    ) -> OrderState:
        del portfolio, dedup_acquired
        captured_decisions.append(decision)
        return OrderState(
            order_id=f"order-{decision.decision_id}",
            decision_id=decision.decision_id,
            status=OrderStatus.INVALID.value,
            market_id=decision.market_id,
            token_id=decision.token_id,
            venue=decision.venue,
            requested_notional_usdc=decision.notional_usdc,
            filled_notional_usdc=0.0,
            remaining_notional_usdc=decision.notional_usdc,
            fill_price=None,
            submitted_at=datetime(2026, 4, 19, tzinfo=UTC),
            last_updated_at=datetime(2026, 4, 19, tzinfo=UTC),
            raw_status="rejected",
            strategy_id=decision.strategy_id,
            strategy_version_id=decision.strategy_version_id,
            filled_quantity=0.0,
        )

    opportunity_store = InMemoryOpportunityStore()
    runner = Runner(
        config=_settings(),
        historical_data_path=FIXTURE_PATH,
        sensors=[IdleSensor()],
        controller=ControllerPipeline(
            forecasters=[StaticForecaster()],
            calibrator=NetcalCalibrator(),
            sizer=KellySizer(risk=RiskSettings(max_position_per_market=500.0)),
            router=Router(),
        ),
        opportunity_store=cast(Any, opportunity_store),
    )
    runner.actuator_executor = cast(
        ActuatorExecutor,
        SimpleNamespace(execute=fake_execute),
    )

    try:
        await runner.start()
        await runner.sensor_stream.queue.put(_signal())
        await _wait_for_opportunities(runner, 1)
        deadline = asyncio.get_running_loop().time() + 2.0
        while not captured_decisions:
            if asyncio.get_running_loop().time() >= deadline:
                raise AssertionError("timed out waiting for actuator execution")
            await asyncio.sleep(0.01)
    finally:
        await runner.stop()

    persisted = await opportunity_store.all()
    assert len(persisted) == 1
    opportunity = persisted[0]
    assert runner.state.opportunities[0].opportunity_id == opportunity.opportunity_id
    assert captured_decisions[0].opportunity_id == opportunity.opportunity_id
    assert captured_decisions[0].stop_conditions


@pytest.mark.asyncio
async def test_runner_records_structured_controller_diagnostics_for_skipped_bearish_signal() -> None:
    from pms.strategies.projections import (
        ActiveStrategy,
        EvalSpec,
        FactorCompositionStep,
        ForecasterSpec,
        MarketSelectionSpec,
        RiskParams,
        StrategyConfig,
    )

    strategy = ActiveStrategy(
        strategy_id="alpha",
        strategy_version_id="alpha-v1",
        config=StrategyConfig(
            strategy_id="alpha",
            factor_composition=(
                FactorCompositionStep(
                    factor_id="snapshot_probability",
                    role="runtime_probability",
                    param="",
                    weight=1.0,
                    threshold=None,
                ),
            ),
            metadata=(),
        ),
        risk=RiskParams(
            max_position_notional_usdc=100.0,
            max_daily_drawdown_pct=2.5,
            min_order_size_usdc=1.0,
        ),
        eval_spec=EvalSpec(metrics=("brier",)),
        forecaster=ForecasterSpec(forecasters=(("rules", ()),)),
        market_selection=MarketSelectionSpec(
            venue="polymarket",
            resolution_time_max_horizon_days=7,
            volume_min_usdc=100.0,
        ),
    )
    runner = Runner(
        config=_settings(),
        historical_data_path=FIXTURE_PATH,
        sensors=[IdleSensor()],
        controller=ControllerPipeline(
            strategy=strategy,
            factor_reader=RecordingFactorReader(),
            outcome_token_resolver=NoNoTokenResolver(),
            forecasters=[StaticLowForecaster()],
            calibrator=NetcalCalibrator(),
            sizer=KellySizer(risk=RiskSettings(max_position_per_market=500.0)),
            router=Router(ControllerSettings(min_volume=100.0)),
        ),
        opportunity_store=cast(Any, InMemoryOpportunityStore()),
    )

    try:
        await runner.start()
        await runner.sensor_stream.queue.put(_signal())
        await _wait_for_diagnostics(runner, 1)
    finally:
        await runner.stop()

    assert runner.state.opportunities == []
    assert runner.state.decisions == []
    assert runner.state.controller_diagnostics[0].code == "missing_no_token"
    assert runner.state.controller_diagnostics[0].metadata["signal_token_id"] == "runner-token"
