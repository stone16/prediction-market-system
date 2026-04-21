from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator
from datetime import UTC, datetime

import pytest

from pms.config import ControllerSettings, PMSSettings, RiskSettings
from pms.controller.calibrators.netcal import NetcalCalibrator
from pms.controller.forecasters.rules import RulesForecaster
from pms.controller.forecasters.statistical import StatisticalForecaster
from pms.controller.pipeline import ControllerPipeline
from pms.controller.router import Router
from pms.controller.sizers.kelly import KellySizer
from pms.core.enums import MarketStatus, RunMode
from pms.core.models import MarketSignal, Portfolio
from pms.runner import Runner


pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.environ.get("PMS_RUN_INTEGRATION") != "1",
        reason="set PMS_RUN_INTEGRATION=1 to run integration tests",
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


def _signal(index: int) -> MarketSignal:
    return MarketSignal(
        market_id=f"neutral-{index}",
        token_id=f"token-{index}",
        venue="polymarket",
        title="Will the neutral runtime stay neutral?",
        yes_price=0.4,
        volume_24h=1_000.0,
        resolves_at=datetime(2026, 4, 30, tzinfo=UTC),
        orderbook={
            "bids": [{"price": 0.39, "size": 100.0}],
            "asks": [{"price": 0.41, "size": 100.0}],
        },
        external_signal={},
        fetched_at=datetime(2026, 4, 19, tzinfo=UTC),
        market_status=MarketStatus.OPEN.value,
    )


def _settings() -> PMSSettings:
    return PMSSettings(
        mode=RunMode.BACKTEST,
        auto_migrate_default_v2=False,
        risk=RiskSettings(
            max_position_per_market=1_000.0,
            max_total_exposure=10_000.0,
            min_order_usdc=1.0,
        ),
    )


@pytest.mark.asyncio
async def test_neutral_runtime_emits_no_decisions_or_orders() -> None:
    signals = [_signal(index) for index in range(100)]
    controller = ControllerPipeline(
        forecasters=[RulesForecaster(), StatisticalForecaster()],
        calibrator=NetcalCalibrator(),
        sizer=KellySizer(risk=_settings().risk),
        router=Router(ControllerSettings(min_volume=100.0)),
        settings=_settings(),
    )
    runner = Runner(
        config=_settings(),
        sensors=[SequenceSensor(signals)],
        controller=controller,
        portfolio=Portfolio(
            total_usdc=100.0,
            free_usdc=100.0,
            locked_usdc=0.0,
            open_positions=[],
        ),
    )

    try:
        await runner.start()
        await asyncio.wait_for(runner.wait_until_idle(), timeout=5.0)
    finally:
        await asyncio.wait_for(runner.stop(), timeout=5.0)

    assert len(runner.state.signals) == 100
    assert runner.state.decisions == []
    assert runner.state.orders == []
    assert runner.state.fills == []
    assert not any(order.filled_notional_usdc > 0.0 for order in runner.state.orders)
    assert controller.suppressed_zero_size == 100
