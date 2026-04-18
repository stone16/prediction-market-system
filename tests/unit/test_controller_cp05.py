from __future__ import annotations

import json
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import pytest

from pms.config import ControllerSettings, LLMSettings, RiskSettings
from pms.controller.calibrators.netcal import NetcalCalibrator
from pms.controller.forecasters.llm import LLMForecaster
from pms.controller.forecasters.rules import RulesForecaster
from pms.controller.forecasters.statistical import StatisticalForecaster
from pms.controller.pipeline import ControllerPipeline
from pms.controller.router import Router
from pms.controller.sizers.kelly import KellySizer
from pms.core.enums import MarketStatus
from pms.core.models import MarketSignal, Portfolio


def _signal(
    *,
    yes_price: float = 0.4,
    volume_24h: float | None = 1000.0,
    external_signal: dict[str, Any] | None = None,
) -> MarketSignal:
    return MarketSignal(
        market_id="m-cp05",
        token_id="t-yes",
        venue="polymarket",
        title="Will CP05 pass?",
        yes_price=yes_price,
        volume_24h=volume_24h,
        resolves_at=datetime(2026, 4, 20, tzinfo=UTC),
        orderbook={
            "bids": [
                {"price": 0.39, "size": 10},
                {"price": 0.38, "size": 20},
                {"price": 0.37, "size": 30},
                {"price": 0.36, "size": 40},
                {"price": 0.35, "size": 50},
                {"price": 0.34, "size": 60},
            ],
            "asks": [
                {"price": 0.41, "size": 11},
                {"price": 0.42, "size": 22},
                {"price": 0.43, "size": 33},
                {"price": 0.44, "size": 44},
                {"price": 0.45, "size": 55},
                {"price": 0.46, "size": 66},
            ],
        },
        external_signal=external_signal
        or {"fair_value": 0.65, "metaculus_prob": 0.7, "yes_count": 7, "no_count": 3},
        fetched_at=datetime(2026, 4, 13, tzinfo=UTC),
        market_status=MarketStatus.OPEN.value,
    )


def _portfolio() -> Portfolio:
    return Portfolio(
        total_usdc=1000.0,
        free_usdc=1000.0,
        locked_usdc=0.0,
        open_positions=[],
    )


class FakeMessages:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def create(self, **kwargs: Any) -> object:
        self.calls.append(kwargs)
        payload = {
            "prob_estimate": 0.8,
            "confidence": 0.6,
            "rationale": "orderbook and external context support yes",
        }
        return SimpleNamespace(content=[SimpleNamespace(text=json.dumps(payload))])


class FakeClaudeClient:
    def __init__(self) -> None:
        self.messages = FakeMessages()


class FailingForecaster:
    def predict(self, signal: MarketSignal) -> tuple[float, float, str]:
        raise RuntimeError("forecast failed")

    async def forecast(self, signal: MarketSignal) -> float:
        raise RuntimeError("forecast failed")


def test_llm_forecaster_returns_neutral_tuple_without_calling_client() -> None:
    client = FakeClaudeClient()
    forecaster = LLMForecaster(
        config=LLMSettings(enabled=True, api_key="test-key", model="claude-test"),
        client=client,
    )

    result = forecaster.predict(_signal())

    assert result == pytest.approx((0.4, 0.0, "pre-s5-neutral"))
    assert getattr(result, "model_id") == "neutral"
    assert client.messages.calls == []


def test_llm_forecaster_returns_neutral_tuple_regardless_of_enablement(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)

    assert LLMForecaster(
        config=LLMSettings(enabled=False),
        client=FakeClaudeClient(),
    ).predict(_signal()) == pytest.approx((0.4, 0.0, "pre-s5-neutral"))
    assert LLMForecaster(
        config=LLMSettings(enabled=True),
        client=FakeClaudeClient(),
    ).predict(_signal()) == pytest.approx((0.4, 0.0, "pre-s5-neutral"))


@pytest.mark.asyncio
async def test_controller_pipeline_averages_three_neutralized_forecasters_to_yes_price() -> None:
    llm_client = FakeClaudeClient()
    pipeline = ControllerPipeline(
        forecasters=[
            RulesForecaster(min_edge=0.01),
            StatisticalForecaster(),
            LLMForecaster(
                config=LLMSettings(enabled=True, api_key="test-key", model="claude-test"),
                client=llm_client,
            ),
        ],
        calibrator=NetcalCalibrator(),
        sizer=KellySizer(risk=RiskSettings(max_position_per_market=1000.0)),
        router=Router(ControllerSettings(min_volume=100.0)),
    )

    decision = await pipeline.on_signal(_signal(), portfolio=_portfolio())

    assert decision is not None
    assert decision.market_id == "m-cp05"
    assert decision.side == "BUY"
    assert decision.prob_estimate == pytest.approx(0.4)
    assert decision.expected_edge == pytest.approx(0.0)
    assert decision.price == 0.4
    assert decision.size == 0.0
    assert decision.stop_conditions


@pytest.mark.asyncio
async def test_controller_pipeline_excludes_disabled_llm_and_failed_forecasters(
    caplog: pytest.LogCaptureFixture,
) -> None:
    pipeline = ControllerPipeline(
        forecasters=[
            FailingForecaster(),
            StatisticalForecaster(),
            LLMForecaster(config=LLMSettings(enabled=False), client=FakeClaudeClient()),
        ],
        calibrator=NetcalCalibrator(),
        sizer=KellySizer(risk=RiskSettings(max_position_per_market=1000.0)),
        router=Router(ControllerSettings(min_volume=100.0)),
    )

    decision = await pipeline.on_signal(_signal(), portfolio=_portfolio())

    assert decision is not None
    assert decision.prob_estimate == pytest.approx(0.4)
    assert "forecaster failed" in caplog.text


def test_router_gate_filters_low_volume_and_near_resolution_markets() -> None:
    router = Router(ControllerSettings(min_volume=100.0))

    assert router.gate(_signal(volume_24h=99.0)) is False
    assert router.gate(_signal(yes_price=0.01)) is False
    assert router.gate(_signal(yes_price=0.99)) is False
    assert router.gate(_signal(yes_price=0.5, volume_24h=100.0)) is True
