from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

from pms.actuator.executor import ActuatorExecutor
from pms.config import PMSSettings, RiskSettings
from pms.controller.calibrators.netcal import NetcalCalibrator
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


@pytest.mark.asyncio
async def test_runner_forwards_opportunity_id_to_actuator() -> None:
    captured_decisions: list[TradeDecision] = []

    async def fake_execute(
        decision: TradeDecision,
        portfolio: Portfolio | None = None,
    ) -> OrderState:
        del portfolio
        captured_decisions.append(decision)
        return OrderState(
            order_id=f"order-{decision.decision_id}",
            decision_id=decision.decision_id,
            status=OrderStatus.INVALID.value,
            market_id=decision.market_id,
            token_id=decision.token_id,
            venue=decision.venue,
            requested_size=decision.size,
            filled_size=0.0,
            remaining_size=decision.size,
            fill_price=None,
            submitted_at=datetime(2026, 4, 19, tzinfo=UTC),
            last_updated_at=datetime(2026, 4, 19, tzinfo=UTC),
            raw_status="rejected",
            strategy_id=decision.strategy_id,
            strategy_version_id=decision.strategy_version_id,
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
