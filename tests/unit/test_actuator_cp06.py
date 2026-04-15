from __future__ import annotations

import inspect
import json
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import httpx
import pytest

from pms.actuator import executor
from pms.actuator.adapters import backtest
from pms.actuator.adapters.backtest import BacktestActuator
from pms.actuator.adapters.paper import PaperActuator
from pms.actuator.adapters.polymarket import PolymarketActuator
from pms.actuator.feedback import ActuatorFeedback
from pms.actuator.risk import InsufficientLiquidityError, RiskManager
from pms.config import PMSSettings, RiskSettings
from pms.core.enums import FeedbackSource, FeedbackTarget, OrderStatus, RunMode, Side
from pms.core.models import (
    LiveTradingDisabledError,
    MarketSignal,
    OrderState,
    Portfolio,
    TradeDecision,
)
from pms.runner import Runner
from pms.sensor.adapters.polymarket_rest import PolymarketRestSensor
from pms.storage.eval_store import EvalStore
from pms.storage.feedback_store import FeedbackStore


def _decision(
    *,
    decision_id: str = "d-cp06",
    market_id: str = "m-cp06",
    side: str = Side.BUY.value,
    price: float = 0.4,
    size: float = 10.0,
) -> TradeDecision:
    return TradeDecision(
        decision_id=decision_id,
        market_id=market_id,
        token_id="t-yes",
        venue="polymarket",
        side=side,
        price=price,
        size=size,
        order_type="limit",
        max_slippage_bps=100,
        stop_conditions=["unit-test"],
        prob_estimate=0.6,
        expected_edge=0.2,
        time_in_force="GTC",
    )


def _portfolio(
    *,
    locked_usdc: float = 0.0,
    max_drawdown_pct: float | None = None,
) -> Portfolio:
    return Portfolio(
        total_usdc=1000.0,
        free_usdc=1000.0 - locked_usdc,
        locked_usdc=locked_usdc,
        open_positions=[],
        max_drawdown_pct=max_drawdown_pct,
    )


def _order_state() -> OrderState:
    now = datetime(2026, 4, 14, tzinfo=UTC)
    return OrderState(
        order_id="order-cp06",
        decision_id="d-cp06",
        status=OrderStatus.INVALID.value,
        market_id="m-cp06",
        token_id="t-yes",
        venue="polymarket",
        requested_size=10.0,
        filled_size=0.0,
        remaining_size=10.0,
        fill_price=None,
        submitted_at=now,
        last_updated_at=now,
        raw_status="rejected",
    )


def _gamma_market_payload() -> list[dict[str, object]]:
    return [
        {
            "conditionId": "pm-paper-1",
            "clobTokenIds": json.dumps(["yes-token", "no-token"]),
            "question": "Will paper mode fill?",
            "outcomePrices": json.dumps(["0.42", "0.58"]),
            "volume24hr": 1200.0,
            "endDateIso": "2026-04-20T00:00:00Z",
            "active": True,
            "closed": False,
            "acceptingOrders": True,
            "liquidity": 3000.0,
        }
    ]


@dataclass(frozen=True)
class OneShotSensor:
    signal: MarketSignal

    def __aiter__(self) -> AsyncIterator[MarketSignal]:
        return self._iterate()

    async def _iterate(self) -> AsyncIterator[MarketSignal]:
        yield self.signal


class AlwaysBuyController:
    async def decide(
        self,
        signal: MarketSignal,
        portfolio: Portfolio | None = None,
    ) -> TradeDecision:
        return _decision(
            decision_id="d-paper-runner",
            market_id=signal.market_id,
            price=signal.yes_price,
            size=10.0,
        )


def test_backtest_adapter_documents_license_decision_before_internal_replay() -> None:
    source = inspect.getsource(backtest)

    assert "prediction-market-backtesting" in source
    assert "LGPL-3.0-or-later" in source
    assert "internal replay" in source


def test_risk_manager_position_breakpoint_exact_limit_and_plus_one() -> None:
    manager = RiskManager(
        RiskSettings(max_position_per_market=100.0, max_total_exposure=1000.0)
    )

    assert manager.check(_decision(size=100.0), _portfolio()).approved is True
    rejected = manager.check(_decision(size=101.0), _portfolio())

    assert rejected.approved is False
    assert rejected.reason == "max_position_per_market"


def test_risk_manager_rejects_total_exposure_and_drawdown() -> None:
    manager = RiskManager(
        RiskSettings(
            max_position_per_market=1000.0,
            max_total_exposure=1000.0,
            max_drawdown_pct=0.2,
        )
    )

    assert (
        manager.check(_decision(size=501.0), _portfolio(locked_usdc=500.0)).reason
        == "max_total_exposure"
    )
    assert (
        manager.check(_decision(size=10.0), _portfolio(max_drawdown_pct=0.21)).reason
        == "drawdown_circuit_breaker"
    )


@pytest.mark.asyncio
async def test_paper_actuator_fills_buy_at_best_ask() -> None:
    actuator = PaperActuator(
        orderbooks={
            "m-cp06": {
                "bids": [{"price": 0.39, "size": 100.0}],
                "asks": [{"price": 0.41, "size": 100.0}],
            }
        }
    )

    state = await actuator.execute(_decision(size=10.0))

    assert state.status == OrderStatus.MATCHED.value
    assert state.fill_price == 0.41
    assert state.filled_size == 10.0
    assert state.remaining_size == 0.0


@pytest.mark.asyncio
async def test_paper_actuator_empty_orderbook_raises_insufficient_liquidity() -> None:
    actuator = PaperActuator(orderbooks={"m-cp06": {"bids": [], "asks": []}})

    with pytest.raises(InsufficientLiquidityError):
        await actuator.execute(_decision())


@pytest.mark.asyncio
async def test_paper_runner_records_fill_from_gamma_derived_depth() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/markets"
        return httpx.Response(200, json=_gamma_market_payload())

    sensor = PolymarketRestSensor(
        client=httpx.AsyncClient(
            transport=httpx.MockTransport(handler),
            base_url="https://gamma.example.test",
        ),
        poll_interval_s=0.01,
    )
    signals = await sensor.poll_once()
    await sensor.aclose()
    runner = Runner(
        config=PMSSettings(
            mode=RunMode.PAPER,
            risk=RiskSettings(
                max_position_per_market=1000.0,
                max_total_exposure=10_000.0,
            ),
        ),
        sensors=[OneShotSensor(signals[0])],
        controller=cast(Any, AlwaysBuyController()),
        eval_store=EvalStore(path=None),
        feedback_store=FeedbackStore(path=None),
    )

    try:
        await runner.start()
        await runner.wait_until_idle()
    finally:
        await runner.stop()

    assert len(runner.state.decisions) == 1
    assert len(runner.state.fills) == 1
    assert runner.state.fills[0].fill_price == 0.42


@pytest.mark.asyncio
async def test_backtest_actuator_replays_fill_from_fixture() -> None:
    fixture_path = Path("tests/fixtures/polymarket_7day_synthetic.jsonl")
    actuator = BacktestActuator(fixture_path)

    state = await actuator.execute(_decision(market_id="pm-synthetic-000", size=10.0))

    assert state.status == OrderStatus.MATCHED.value
    assert state.fill_price == 0.31


@pytest.mark.asyncio
async def test_polymarket_actuator_raises_when_live_trading_disabled() -> None:
    actuator = PolymarketActuator(PMSSettings(live_trading_enabled=False))

    with pytest.raises(LiveTradingDisabledError):
        await actuator.execute(_decision())


def test_actuator_feedback_appends_controller_feedback() -> None:
    store = FeedbackStore(path=None)
    generator = ActuatorFeedback(store)

    feedback = generator.generate(_order_state(), reason="insufficient_liquidity")

    assert feedback.source == FeedbackSource.ACTUATOR.value
    assert feedback.target == FeedbackTarget.CONTROLLER.value
    assert feedback.category == "insufficient_liquidity"
    assert store.all() == [feedback]


@pytest.mark.asyncio
async def test_executor_releases_dedup_token_on_success_and_liquidity_rejection() -> None:
    store = FeedbackStore(path=None)
    tokens = executor.DedupTokenStore()
    ok_executor = executor.ActuatorExecutor(
        adapter=PaperActuator(
            orderbooks={
                "m-cp06": {
                    "bids": [{"price": 0.39, "size": 100.0}],
                    "asks": [{"price": 0.41, "size": 100.0}],
                }
            }
        ),
        risk=RiskManager(
            RiskSettings(max_position_per_market=100.0, max_total_exposure=1000.0)
        ),
        feedback=ActuatorFeedback(store),
        dedup_tokens=tokens,
    )

    await ok_executor.execute(_decision(decision_id="d-ok"), _portfolio())
    assert tokens.contains("d-ok") is False

    failing_executor = executor.ActuatorExecutor(
        adapter=PaperActuator(orderbooks={"m-cp06": {"bids": [], "asks": []}}),
        risk=RiskManager(
            RiskSettings(max_position_per_market=100.0, max_total_exposure=1000.0)
        ),
        feedback=ActuatorFeedback(store),
        dedup_tokens=tokens,
    )

    rejected = await failing_executor.execute(_decision(decision_id="d-fail"), _portfolio())

    assert rejected.status == OrderStatus.INVALID.value
    assert rejected.raw_status == "insufficient_liquidity"
    assert tokens.contains("d-fail") is False
    assert store.all()[-1].category == "insufficient_liquidity"
