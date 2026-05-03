from __future__ import annotations

import json
from datetime import UTC, date, datetime
from math import inf, nan
from types import SimpleNamespace
from typing import Any, Protocol

import pytest

from pms.config import ControllerSettings, LLMSettings, RiskSettings
from pms.controller.calibrators.netcal import NetcalCalibrator
from pms.controller.forecasters.llm import (
    LLMForecaster,
    _as_float,
    _clamp,
    _load_json,
    _prompt,
    _response_text_anthropic,
    _response_text_openai,
)
from pms.controller.forecasters.rules import RulesForecaster
from pms.controller.forecasters.statistical import StatisticalForecaster
from pms.controller.factor_snapshot import NullFactorSnapshotReader
from pms.controller.outcome_tokens import OutcomeTokens
from pms.controller.pipeline import ControllerPipeline
from pms.controller.router import Router
from pms.controller.sizers.kelly import KellySizer
from pms.core.enums import MarketStatus
from pms.core.models import MarketSignal, Portfolio
from pms.strategies.projections import (
    ActiveStrategy,
    EvalSpec,
    FactorCompositionStep,
    ForecasterSpec,
    MarketSelectionSpec,
    RiskParams,
    StrategyConfig,
)


class _MessagesDouble(Protocol):
    calls: list[dict[str, Any]]

    def create(self, **kwargs: Any) -> object: ...


def _signal(
    *,
    market_id: str = "m-cp05",
    yes_price: float = 0.4,
    volume_24h: float | None = 1000.0,
    external_signal: dict[str, Any] | None = None,
) -> MarketSignal:
    return MarketSignal(
        market_id=market_id,
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
    def __init__(
        self,
        *,
        prob_estimate: float = 0.8,
        confidence: float = 0.6,
        rationale: str = "orderbook and external context support yes",
    ) -> None:
        self._payload = {
            "prob_estimate": prob_estimate,
            "confidence": confidence,
            "rationale": rationale,
        }
        self.calls: list[dict[str, Any]] = []

    def create(self, **kwargs: Any) -> object:
        self.calls.append(kwargs)
        return SimpleNamespace(content=[SimpleNamespace(text=json.dumps(self._payload))])


class FakeSequenceMessages:
    def __init__(self, payloads: list[dict[str, Any]]) -> None:
        self._payloads = list(payloads)
        self.calls: list[dict[str, Any]] = []

    def create(self, **kwargs: Any) -> object:
        self.calls.append(kwargs)
        payload = self._payloads.pop(0)
        return SimpleNamespace(content=[SimpleNamespace(text=json.dumps(payload))])


class FakeClaudeClient:
    def __init__(self) -> None:
        self.messages: _MessagesDouble = FakeMessages()


class FakeOpenAIChatCompletions:
    def __init__(
        self,
        *,
        prob_estimate: float = 0.72,
        confidence: float = 0.55,
        rationale: str = "openai-fake",
    ) -> None:
        self._payload = {
            "prob_estimate": prob_estimate,
            "confidence": confidence,
            "rationale": rationale,
        }
        self.calls: list[dict[str, Any]] = []

    def create(self, **kwargs: Any) -> object:
        self.calls.append(kwargs)
        message = SimpleNamespace(content=json.dumps(self._payload))
        return SimpleNamespace(choices=[SimpleNamespace(message=message)])


class FakeOpenAIChat:
    def __init__(self) -> None:
        self.completions = FakeOpenAIChatCompletions()


class FakeOpenAIClient:
    def __init__(self) -> None:
        self.chat = FakeOpenAIChat()


class FailingForecaster:
    def predict(self, signal: MarketSignal) -> tuple[float, float, str]:
        raise RuntimeError("forecast failed")

    async def forecast(self, signal: MarketSignal) -> float:
        raise RuntimeError("forecast failed")


class ConstantForecaster:
    def __init__(self, probability: float) -> None:
        self.probability = probability

    def predict(self, signal: MarketSignal) -> tuple[float, float, str]:
        del signal
        return (self.probability, 0.0, "constant")

    async def forecast(self, signal: MarketSignal) -> float:
        del signal
        return self.probability


class NoNoTokenResolver:
    async def resolve(
        self,
        *,
        market_id: str,
        signal_token_id: str | None,
    ) -> OutcomeTokens:
        del market_id, signal_token_id
        return OutcomeTokens(yes_token_id="yes-token", no_token_id=None)


class ResolvedNoTokenResolver:
    async def resolve(
        self,
        *,
        market_id: str,
        signal_token_id: str | None,
    ) -> OutcomeTokens:
        del market_id, signal_token_id
        return OutcomeTokens(yes_token_id="yes-token", no_token_id="no-token")


class FixedSizer:
    def __init__(self, size: float) -> None:
        self._size = size

    def size(self, *, prob: float, market_price: float, portfolio: Portfolio) -> float:
        del prob, market_price, portfolio
        return self._size


def test_llm_forecaster_predict_uses_anthropic_system_prompt_and_caches() -> None:
    client = FakeClaudeClient()
    forecaster = LLMForecaster(
        config=LLMSettings(
            enabled=True,
            provider="anthropic",
            api_key="test-key",
            model="claude-test",
            cache_ttl_s=30.0,
        ),
        client=client,
    )

    result = forecaster.predict(_signal())

    assert result is not None
    assert result == pytest.approx(
        (0.8, 0.6, "orderbook and external context support yes")
    )
    assert result.model_id == "claude-test"
    assert len(client.messages.calls) == 1
    call = client.messages.calls[0]
    assert call["system"].startswith("You are a calibrated prediction-market forecaster")
    assert call["messages"][0]["role"] == "user"
    assert "# Market" in call["messages"][0]["content"]

    assert forecaster.predict(_signal()) is result
    assert len(client.messages.calls) == 1


def test_llm_forecaster_predict_uses_openai_system_message() -> None:
    client = FakeOpenAIClient()
    forecaster = LLMForecaster(
        config=LLMSettings(
            enabled=True,
            provider="openai",
            api_key="test-key",
            base_url="https://llm-gateway.example/v1",
            model="openai-test",
        ),
        client=client,
    )

    result = forecaster.predict(_signal())

    assert result is not None
    assert result == pytest.approx((0.72, 0.55, "openai-fake"))
    assert result.model_id == "openai-test"
    call = client.chat.completions.calls[0]
    assert call["response_format"] == {"type": "json_object"}
    assert call["messages"][0]["role"] == "system"
    assert call["messages"][0]["content"].startswith(
        "You are a calibrated prediction-market forecaster"
    )
    assert call["messages"][1]["role"] == "user"
    assert "# Market" in call["messages"][1]["content"]


def test_llm_forecaster_returns_none_when_disabled_and_real_result_when_enabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)

    assert (
        LLMForecaster(
            config=LLMSettings(enabled=False),
            client=FakeClaudeClient(),
        ).predict(_signal())
        is None
    )
    result = LLMForecaster(
        config=LLMSettings(enabled=True, provider="anthropic", api_key="test-key"),
        client=FakeClaudeClient(),
    ).predict(_signal())
    assert result is not None
    assert result == pytest.approx(
        (0.8, 0.6, "orderbook and external context support yes")
    )


@pytest.mark.asyncio
async def test_llm_forecaster_forecast_uses_neutral_probability_and_default_config() -> None:
    forecaster = LLMForecaster()

    probability = await forecaster.forecast(_signal(yes_price=0.27))

    assert forecaster.config is not None
    assert probability == pytest.approx(0.27)


def test_llm_forecaster_client_paths_cover_injected_missing_and_cached_factory(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    injected_client = FakeClaudeClient()
    injected_result = LLMForecaster(
        config=LLMSettings(enabled=True, provider="anthropic", api_key="ignored"),
        client=injected_client,
    )._client()
    assert injected_result is not None
    assert injected_result is injected_client

    def raise_import_error(module_name: str) -> object:
        raise ImportError(module_name)

    monkeypatch.setattr("pms.controller.forecasters.llm.import_module", raise_import_error)
    assert (
        LLMForecaster(
            config=LLMSettings(enabled=True, provider="anthropic", api_key="missing")
        )._client()
        is None
    )

    monkeypatch.setattr(
        "pms.controller.forecasters.llm.import_module",
        lambda _: SimpleNamespace(Anthropic="not-callable"),
    )
    assert (
        LLMForecaster(
            config=LLMSettings(enabled=True, provider="anthropic", api_key="bad")
        )._client()
        is None
    )

    created_calls: list[str] = []

    def create_client(**kwargs: Any) -> FakeClaudeClient:
        created_calls.append(str(kwargs["api_key"]))
        return FakeClaudeClient()

    monkeypatch.setattr(
        "pms.controller.forecasters.llm.import_module",
        lambda _: SimpleNamespace(Anthropic=create_client),
    )
    forecaster = LLMForecaster(
        config=LLMSettings(enabled=True, provider="anthropic", api_key="cache-key")
    )
    created_client = forecaster._client()

    assert created_client is not None
    assert hasattr(created_client, "messages")
    assert forecaster.client is not None
    cached_client = forecaster._client()
    assert cached_client is not None
    assert cached_client is created_client
    assert created_calls == ["cache-key"]


def test_llm_forecaster_budget_exhaustion_and_midnight_utc_reset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = FakeClaudeClient()
    forecaster = LLMForecaster(
        config=LLMSettings(
            enabled=True,
            provider="anthropic",
            api_key="test-key",
            model="claude-test",
            cache_ttl_s=0.0,
            max_daily_llm_cost_usdc=0.003,
        ),
        client=client,
    )
    days = iter([date(2026, 5, 3), date(2026, 5, 3), date(2026, 5, 4)])
    monkeypatch.setattr(
        "pms.controller.forecasters.llm._today_utc",
        lambda: next(days),
    )

    assert forecaster.predict(_signal(market_id="m-1")) is not None
    assert forecaster.predict(_signal(market_id="m-2")) is None
    assert forecaster.predict(_signal(market_id="m-3")) is not None
    assert len(client.messages.calls) == 2


def test_llm_forecaster_clamps_probability_away_from_impossible_extremes() -> None:
    client = FakeClaudeClient()
    client.messages = FakeMessages(prob_estimate=1.2, confidence=1.5)
    high = LLMForecaster(
        config=LLMSettings(enabled=True, provider="anthropic", api_key="test-key"),
        client=client,
    ).predict(_signal())

    assert high is not None
    assert high[0] == pytest.approx(0.99)
    assert high[1] == pytest.approx(1.0)

    low_client = FakeClaudeClient()
    low_client.messages = FakeMessages(prob_estimate=-0.3, confidence=-0.2)
    low = LLMForecaster(
        config=LLMSettings(enabled=True, provider="anthropic", api_key="test-key"),
        client=low_client,
    ).predict(_signal())

    assert low is not None
    assert low[0] == pytest.approx(0.01)
    assert low[1] == pytest.approx(0.0)


def test_llm_forecaster_rejects_non_finite_numeric_fields() -> None:
    client = FakeClaudeClient()
    client.messages = FakeMessages(prob_estimate=float("nan"), confidence=0.5)
    forecaster = LLMForecaster(
        config=LLMSettings(enabled=True, provider="anthropic", api_key="test-key"),
        client=client,
    )

    assert forecaster.predict(_signal()) is None
    assert len(client.messages.calls) == 1


def test_llm_forecaster_counts_malformed_provider_attempt_against_budget() -> None:
    client = FakeClaudeClient()
    client.messages = FakeSequenceMessages(
        [
            {
                "prob_estimate": 0.8,
                "confidence": 0.6,
            },
            {
                "prob_estimate": 0.7,
                "confidence": 0.5,
                "rationale": "would be valid if called",
            },
        ]
    )
    forecaster = LLMForecaster(
        config=LLMSettings(
            enabled=True,
            provider="anthropic",
            api_key="test-key",
            cache_ttl_s=0.0,
            max_daily_llm_cost_usdc=0.003,
        ),
        client=client,
    )

    assert forecaster.predict(_signal(market_id="m-bad")) is None
    assert forecaster.predict(_signal(market_id="m-budget")) is None
    assert len(client.messages.calls) == 1


def test_llm_prompt_trims_orderbook_and_serializes_external_signal() -> None:
    prompt = _prompt(_signal())

    assert "# Market" in prompt
    assert "title: Will CP05 pass?" in prompt
    assert "yes_price: 0.4" in prompt
    assert '"price":0.35' in prompt
    assert '"price":0.34' not in prompt
    assert '"fair_value": 0.65' in prompt
    assert '"no_count": 3' in prompt
    assert "Return JSON only" in prompt


def test_llm_response_parsing_helpers_cover_json_text_and_errors() -> None:
    direct_json = '{"prob_estimate":0.6,"confidence":0.2,"rationale":"direct"}'
    embedded_json = 'prefix {"prob_estimate":0.7,"confidence":0.1,"rationale":"embedded"} suffix'
    openai_response = SimpleNamespace(
        choices=[SimpleNamespace(message=SimpleNamespace(content=direct_json))]
    )

    assert _response_text_anthropic(SimpleNamespace(content="plain-text")) == "plain-text"
    assert (
        _response_text_anthropic(SimpleNamespace(content={"content": "ignored"}))
        == "{'content': 'ignored'}"
    )
    assert _response_text_openai(openai_response) == direct_json
    assert _load_json(direct_json)["prob_estimate"] == pytest.approx(0.6)
    assert _load_json(embedded_json)["rationale"] == "embedded"
    assert _as_float("0.5") == pytest.approx(0.5)
    assert _as_float(2) == pytest.approx(2.0)
    assert _clamp(-0.2) == 0.0
    assert _clamp(1.2) == 1.0

    with pytest.raises(ValueError, match="Expected numeric value"):
        _as_float(object())

    with pytest.raises(ValueError, match="expected JSON object"):
        _load_json("[1, 2, 3]")


@pytest.mark.asyncio
async def test_controller_pipeline_suppresses_zero_size_decision_and_tracks_metric() -> None:
    llm_client = FakeClaudeClient()
    llm_client.messages = FakeMessages(
        prob_estimate=0.4,
        confidence=0.0,
        rationale="neutral mocked LLM branch",
    )
    pipeline = ControllerPipeline(
        forecasters=[
            RulesForecaster(min_edge=0.01),
            StatisticalForecaster(),
            LLMForecaster(
                config=LLMSettings(
                    enabled=True,
                    provider="anthropic",
                    api_key="test-key",
                    model="claude-test",
                ),
                client=llm_client,
            ),
        ],
        calibrator=NetcalCalibrator(),
        sizer=KellySizer(risk=RiskSettings(max_position_per_market=1000.0)),
        router=Router(ControllerSettings(min_volume=100.0)),
    )

    emission = await pipeline.on_signal(_signal(), portfolio=_portfolio())

    assert emission is None
    assert pipeline.suppressed_zero_size == 1


@pytest.mark.asyncio
async def test_controller_pipeline_suppresses_sub_min_order_decision_and_tracks_metric() -> None:
    pipeline = ControllerPipeline(
        forecasters=[
            ConstantForecaster(0.6),
        ],
        calibrator=NetcalCalibrator(),
        sizer=FixedSizer(0.5),
        router=Router(ControllerSettings(min_volume=100.0)),
    )

    emission = await pipeline.on_signal(_signal(), portfolio=_portfolio())

    assert emission is None
    assert pipeline.suppressed_zero_size == 1


@pytest.mark.asyncio
async def test_controller_pipeline_uses_default_dependencies_for_neutral_signals() -> None:
    pipeline = ControllerPipeline()

    assert await pipeline.decide(_signal(), portfolio=_portfolio()) is None
    assert pipeline.suppressed_zero_size == 0
    assert isinstance(pipeline.forecasters, tuple)
    assert pipeline.calibrator is not None
    assert pipeline.sizer is not None
    assert pipeline.router is not None


@pytest.mark.asyncio
async def test_controller_pipeline_returns_none_when_all_forecasters_fail() -> None:
    pipeline = ControllerPipeline(
        forecasters=[FailingForecaster()],
        calibrator=NetcalCalibrator(),
        sizer=KellySizer(risk=RiskSettings(max_position_per_market=1000.0)),
        router=Router(ControllerSettings(min_volume=100.0)),
    )

    emission = await pipeline.on_signal(_signal(), portfolio=_portfolio())

    assert emission is None
    assert pipeline.suppressed_zero_size == 0


@pytest.mark.asyncio
async def test_controller_pipeline_reports_missing_no_token_for_bearish_signal() -> None:
    pipeline = ControllerPipeline(
        forecasters=[ConstantForecaster(0.4)],
        outcome_token_resolver=NoNoTokenResolver(),
        calibrator=NetcalCalibrator(),
        sizer=KellySizer(risk=RiskSettings(max_position_per_market=1000.0)),
        router=Router(ControllerSettings(min_volume=100.0)),
    )

    emission = await pipeline.on_signal(_signal(yes_price=0.6), portfolio=_portfolio())

    assert emission is None
    assert pipeline.suppressed_zero_size == 0
    assert pipeline.last_diagnostic is not None
    assert pipeline.last_diagnostic.code == "missing_no_token"


@pytest.mark.asyncio
async def test_controller_pipeline_uses_strategy_composition_for_positive_emission() -> None:
    strategy = ActiveStrategy(
        strategy_id="alpha",
        strategy_version_id="alpha-v1",
            config=StrategyConfig(
                strategy_id="alpha",
                factor_composition=(
                    FactorCompositionStep(
                        factor_id="rules",
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
    pipeline = ControllerPipeline(
        strategy=strategy,
        factor_reader=NullFactorSnapshotReader(),
        forecasters=[ConstantForecaster(0.6)],
        calibrator=NetcalCalibrator(),
        sizer=FixedSizer(2.0),
        router=Router(ControllerSettings(min_volume=100.0)),
    )

    emission = await pipeline.on_signal(_signal(), portfolio=_portfolio())

    assert emission is not None
    opportunity, decision = emission
    assert decision.strategy_id == "alpha"
    assert decision.notional_usdc == pytest.approx(2.0)
    assert decision.model_id == "ConstantForecaster"
    assert opportunity.factor_snapshot_hash is not None
    assert opportunity.expected_edge == pytest.approx(0.2)
    assert "rules" in opportunity.selected_factor_values


@pytest.mark.asyncio
async def test_controller_pipeline_returns_none_when_strategy_composition_fails() -> None:
    strategy = ActiveStrategy(
        strategy_id="alpha",
        strategy_version_id="alpha-v1",
        config=StrategyConfig(
            strategy_id="alpha",
            factor_composition=(
                FactorCompositionStep(
                    factor_id="missing_factor",
                    role="weighted",
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
    pipeline = ControllerPipeline(
        strategy=strategy,
        factor_reader=NullFactorSnapshotReader(),
        forecasters=[ConstantForecaster(0.6)],
        calibrator=NetcalCalibrator(),
        sizer=FixedSizer(2.0),
        router=Router(ControllerSettings(min_volume=100.0)),
    )

    emission = await pipeline.on_signal(_signal(), portfolio=_portfolio())

    assert emission is None


@pytest.mark.asyncio
async def test_controller_pipeline_uses_resolved_no_token_for_bearish_signal() -> None:
    pipeline = ControllerPipeline(
        forecasters=[ConstantForecaster(0.4)],
        outcome_token_resolver=ResolvedNoTokenResolver(),
        calibrator=NetcalCalibrator(),
        sizer=FixedSizer(2.0),
        router=Router(ControllerSettings(min_volume=100.0)),
    )

    emission = await pipeline.on_signal(_signal(yes_price=0.6), portfolio=_portfolio())

    assert emission is not None
    opportunity, decision = emission
    assert opportunity.side == "no"
    assert decision.token_id == "no-token"
    assert decision.outcome == "NO"
    assert decision.limit_price == pytest.approx(0.4)
    assert decision.prob_estimate == pytest.approx(0.6)
    assert decision.expected_edge == pytest.approx(0.2)
    assert opportunity.expected_edge == pytest.approx(0.2)


@pytest.mark.asyncio
async def test_controller_pipeline_emits_opportunity_and_decision_for_positive_size() -> None:
    pipeline = ControllerPipeline(
        forecasters=[ConstantForecaster(0.6)],
        calibrator=NetcalCalibrator(),
        sizer=FixedSizer(2.0),
        router=Router(ControllerSettings(min_volume=100.0)),
    )

    emission = await pipeline.on_signal(_signal(), portfolio=_portfolio())

    assert emission is not None
    opportunity, decision = emission
    assert opportunity.market_id == "m-cp05"
    assert opportunity.target_size_usdc == pytest.approx(2.0)
    assert decision.market_id == "m-cp05"
    assert decision.notional_usdc == pytest.approx(2.0)
    assert decision.limit_price == pytest.approx(0.4)
    assert decision.model_id == "ConstantForecaster"
    assert decision.opportunity_id == opportunity.opportunity_id
    assert pipeline.suppressed_zero_size == 0


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

    emission = await pipeline.on_signal(_signal(), portfolio=_portfolio())

    assert emission is None
    assert pipeline.suppressed_zero_size == 0
    assert "forecaster failed" in caplog.text


def test_router_gate_filters_low_volume_and_near_resolution_markets() -> None:
    router = Router(ControllerSettings(min_volume=100.0))

    assert router.gate(_signal(volume_24h=99.0)) is False
    assert router.gate(_signal(yes_price=0.01)) is False
    assert router.gate(_signal(yes_price=0.99)) is False
    assert router.gate(_signal(yes_price=0.5, volume_24h=100.0)) is True


@pytest.mark.parametrize("yes_price", [nan, inf, -inf])
def test_router_gate_rejects_non_finite_yes_price(yes_price: float) -> None:
    router = Router(ControllerSettings(min_volume=100.0))

    assert router.gate(_signal(yes_price=yes_price)) is False


@pytest.mark.parametrize(
    "metric,value",
    [
        ("spread_bps", nan),
        ("spread_bps", "NaN"),
        ("spread_bps", inf),
        ("spread_bps", "Inf"),
        ("book_age_ms", nan),
        ("book_age_ms", "NaN"),
        ("book_age_ms", inf),
        ("book_age_ms", "Inf"),
    ],
)
def test_router_gate_rejects_non_finite_quote_metrics(
    metric: str,
    value: object,
) -> None:
    router = Router(ControllerSettings(min_volume=100.0))

    assert router.gate(_signal(external_signal={metric: value})) is False
