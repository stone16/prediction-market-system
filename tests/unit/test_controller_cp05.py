from __future__ import annotations

import json
from datetime import UTC, datetime
from math import inf, nan
from types import SimpleNamespace
from typing import Any, cast

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


class FakeOpenAIChatCompletions:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def create(self, **kwargs: Any) -> object:
        self.calls.append(kwargs)
        payload = {
            "prob_estimate": 0.72,
            "confidence": 0.55,
            "rationale": "openai-fake",
        }
        message = SimpleNamespace(content=json.dumps(payload))
        choice = SimpleNamespace(message=message)
        return SimpleNamespace(choices=[choice])


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


def test_llm_forecaster_predict_uses_injected_client_and_caches() -> None:
    client = FakeClaudeClient()
    forecaster = LLMForecaster(
        config=LLMSettings(
            enabled=True,
            provider="anthropic",
            api_key="test-key",
            model="claude-test",
            timeout_s=2.0,
            cache_ttl_s=30.0,
            max_tokens=128,
        ),
        client=client,
    )

    result = forecaster.predict(_signal())

    assert result is not None
    assert result[0] == pytest.approx(0.8)
    assert result[1] == pytest.approx(0.6)
    assert result.model_id == "claude-test"
    assert client.messages.calls and len(client.messages.calls) == 1

    # second call hits cache
    forecaster.predict(_signal())
    assert len(client.messages.calls) == 1


def test_llm_forecaster_returns_none_when_disabled_and_real_result_when_enabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)

    disabled = LLMForecaster(
        config=LLMSettings(enabled=False),
        client=FakeClaudeClient(),
    )
    assert disabled.predict(_signal()) is None

    enabled = LLMForecaster(
        config=LLMSettings(
            enabled=True,
            provider="anthropic",
            api_key="test-key",
            model="claude-test",
            cache_ttl_s=30.0,
        ),
        client=FakeClaudeClient(),
    )
    result = enabled.predict(_signal())
    assert result is not None
    assert result[0] == pytest.approx(0.8)


@pytest.mark.asyncio
async def test_llm_forecaster_forecast_uses_neutral_probability_and_default_config() -> None:
    forecaster = LLMForecaster()

    probability = await forecaster.forecast(_signal(yes_price=0.27))

    assert forecaster.config is not None
    assert probability == pytest.approx(0.27)


def test_llm_forecaster_client_paths_cover_injection_dispatch_and_caching(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # 1. injected client short-circuits dispatch
    injected_client = FakeClaudeClient()
    forecaster_a = LLMForecaster(
        config=LLMSettings(
            enabled=True,
            provider="anthropic",
            api_key="x",
        ),
        client=injected_client,
    )
    assert forecaster_a._client() is injected_client

    # 2. unknown / missing provider returns None
    forecaster_b = LLMForecaster(config=LLMSettings(enabled=False))
    assert forecaster_b._client() is None

    # 3. anthropic provider, missing SDK -> None
    def raise_import_error(module_name: str) -> object:
        raise ImportError(module_name)

    monkeypatch.setattr(
        "pms.controller.forecasters.llm.import_module", raise_import_error
    )
    forecaster_c = LLMForecaster(
        config=LLMSettings(
            enabled=True, provider="anthropic", api_key="k"
        )
    )
    assert forecaster_c._client() is None

    # 4. anthropic provider, factory returns object; cached on subsequent call
    created_calls: list[dict[str, object]] = []

    def create_anthropic(**kwargs: object) -> FakeClaudeClient:
        created_calls.append(kwargs)
        return FakeClaudeClient()

    monkeypatch.setattr(
        "pms.controller.forecasters.llm.import_module",
        lambda _: SimpleNamespace(Anthropic=create_anthropic),
    )
    forecaster_d = LLMForecaster(
        config=LLMSettings(
            enabled=True, provider="anthropic", api_key="key1"
        )
    )
    first = forecaster_d._client()
    assert first is not None
    assert hasattr(first, "messages")
    cached = forecaster_d._client()
    assert cached is first
    assert len(created_calls) == 1
    assert created_calls[0]["api_key"] == "key1"

    # 5. openai provider with base_url
    def create_openai(**kwargs: object) -> FakeOpenAIClient:
        created_calls.append({"openai": kwargs})
        return FakeOpenAIClient()

    monkeypatch.setattr(
        "pms.controller.forecasters.llm.import_module",
        lambda _: SimpleNamespace(OpenAI=create_openai),
    )
    forecaster_e = LLMForecaster(
        config=LLMSettings(
            enabled=True,
            provider="openai",
            api_key="key2",
            base_url="https://gw.example/v1",
        )
    )
    openai_client = forecaster_e._client()
    assert openai_client is not None
    assert hasattr(openai_client, "chat")


def test_llm_prompt_trims_orderbook_and_serializes_external_signal() -> None:
    prompt = _prompt(_signal())

    assert "market_title: Will CP05 pass?" in prompt
    assert "yes_price: 0.4" in prompt
    assert '"price":0.35' in prompt
    assert '"price":0.34' not in prompt
    assert "Respond with a JSON object only" in prompt


def test_llm_response_parsing_helpers_cover_json_text_and_errors() -> None:
    direct_json = '{"prob_estimate":0.6,"confidence":0.2,"rationale":"direct"}'
    embedded_json = 'prefix {"prob_estimate":0.7,"confidence":0.1,"rationale":"embedded"} suffix'

    assert _response_text_anthropic(SimpleNamespace(content="plain-text")) == "plain-text"
    assert _response_text_anthropic(SimpleNamespace(content={"content": "ignored"})) == "{'content': 'ignored'}"
    assert _load_json(direct_json)["prob_estimate"] == pytest.approx(0.6)
    assert _load_json(embedded_json)["rationale"] == "embedded"
    assert _as_float("0.5") == pytest.approx(0.5)
    assert _as_float(2) == pytest.approx(2.0)
    assert _clamp(-0.2) == 0.0
    assert _clamp(1.2) == 1.0

    with pytest.raises(ValueError, match="Expected numeric value"):
        _as_float(object())


@pytest.mark.asyncio
async def test_controller_pipeline_suppresses_zero_size_decision_and_tracks_metric() -> None:
    llm_client = FakeClaudeClient()
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
        sizer=FixedSizer(0.5),
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


def _enabled_anthropic_settings() -> LLMSettings:
    return LLMSettings(
        enabled=True,
        provider="anthropic",
        api_key="test-key",
        model="claude-test",
        timeout_s=2.0,
        cache_ttl_s=30.0,
        max_tokens=128,
    )


def _enabled_openai_settings() -> LLMSettings:
    return LLMSettings(
        enabled=True,
        provider="openai",
        api_key="test-key",
        base_url="https://gateway.example/v1",
        model="gpt-test",
        timeout_s=2.0,
        cache_ttl_s=30.0,
        max_tokens=128,
    )


def test_predict_returns_none_when_disabled() -> None:
    forecaster = LLMForecaster(
        config=LLMSettings(enabled=False), client=FakeClaudeClient()
    )
    assert forecaster.predict(_signal()) is None


def test_predict_anthropic_provider_calls_client_and_caches() -> None:
    client = FakeClaudeClient()
    forecaster = LLMForecaster(
        config=_enabled_anthropic_settings(), client=client
    )

    first = forecaster.predict(_signal(market_id="mkt-A"))
    assert first is not None
    assert first[0] == pytest.approx(0.8)
    assert first[1] == pytest.approx(0.6)
    assert client.messages.calls and len(client.messages.calls) == 1

    second = forecaster.predict(_signal(market_id="mkt-A"))
    assert second is not None
    assert second[0] == pytest.approx(0.8)
    assert len(client.messages.calls) == 1  # cache hit, no extra call


def test_predict_openai_provider_calls_client_and_caches() -> None:
    client = FakeOpenAIClient()
    forecaster = LLMForecaster(
        config=_enabled_openai_settings(), client=client
    )

    result = forecaster.predict(_signal(market_id="mkt-O"))
    assert result is not None
    assert result[0] == pytest.approx(0.72)
    assert result[1] == pytest.approx(0.55)
    assert len(client.chat.completions.calls) == 1

    # Cache hit
    forecaster.predict(_signal(market_id="mkt-O"))
    assert len(client.chat.completions.calls) == 1


def test_predict_cache_hit_skips_client() -> None:
    client = FakeClaudeClient()
    forecaster = LLMForecaster(
        config=_enabled_anthropic_settings(), client=client
    )
    forecaster.predict(_signal(market_id="mkt-X"))
    forecaster.predict(_signal(market_id="mkt-X"))
    forecaster.predict(_signal(market_id="mkt-X"))
    assert len(client.messages.calls) == 1


def test_predict_cache_expiry_recalls_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = FakeClaudeClient()
    forecaster = LLMForecaster(
        config=_enabled_anthropic_settings(), client=client
    )
    fake_now = [1000.0]
    monkeypatch.setattr(
        "pms.controller.forecasters.llm.time.monotonic",
        lambda: fake_now[0],
    )
    forecaster.predict(_signal(market_id="mkt-T"))
    assert len(client.messages.calls) == 1
    fake_now[0] += 31.0  # past 30s TTL
    forecaster.predict(_signal(market_id="mkt-T"))
    assert len(client.messages.calls) == 2


def test_predict_cache_size_cap_evicts_oldest(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = FakeClaudeClient()
    forecaster = LLMForecaster(
        config=_enabled_anthropic_settings(), client=client
    )
    fake_now = [1000.0]
    monkeypatch.setattr(
        "pms.controller.forecasters.llm.time.monotonic",
        lambda: fake_now[0],
    )
    # Fill cache past its 1000-entry cap
    for i in range(1005):
        fake_now[0] += 0.001
        forecaster.predict(_signal(market_id=f"mkt-{i}"))
    assert len(forecaster._cache) <= 1000
    # Earliest market_id must have been evicted
    assert "mkt-0" not in forecaster._cache


def test_predict_timeout_returns_none() -> None:
    from pms.controller.forecasters.llm import LLMTimeoutError

    class TimeoutClient:
        class _Messages:
            def create(self, **kwargs: Any) -> object:
                raise LLMTimeoutError("simulated timeout")

        def __init__(self) -> None:
            self.messages = TimeoutClient._Messages()

    forecaster = LLMForecaster(
        config=_enabled_anthropic_settings(), client=TimeoutClient()
    )
    assert forecaster.predict(_signal(market_id="mkt-timeout")) is None
    # Failure must not poison the cache
    assert "mkt-timeout" not in forecaster._cache


def test_predict_transient_error_returns_none() -> None:
    from pms.controller.forecasters.llm import LLMTransientError

    class TransientClient:
        class _Messages:
            def create(self, **kwargs: Any) -> object:
                raise LLMTransientError("rate limited")

        def __init__(self) -> None:
            self.messages = TransientClient._Messages()

    forecaster = LLMForecaster(
        config=_enabled_anthropic_settings(), client=TransientClient()
    )
    assert forecaster.predict(_signal(market_id="mkt-rate")) is None
    assert "mkt-rate" not in forecaster._cache


def test_predict_malformed_response_returns_none() -> None:
    class MalformedMessages:
        def __init__(self) -> None:
            self.calls: list[dict[str, Any]] = []

        def create(self, **kwargs: Any) -> object:
            self.calls.append(kwargs)
            return SimpleNamespace(content=[SimpleNamespace(text="not json")])

    class MalformedClient:
        def __init__(self) -> None:
            self.messages = MalformedMessages()

    forecaster = LLMForecaster(
        config=_enabled_anthropic_settings(), client=MalformedClient()
    )
    assert forecaster.predict(_signal(market_id="mkt-bad")) is None
    assert "mkt-bad" not in forecaster._cache


@pytest.mark.asyncio
async def test_forecast_returns_yes_price_on_predict_failure() -> None:
    """forecast() falls back to signal.yes_price when predict returns None."""

    class FailingMessages:
        def create(self, **kwargs: Any) -> object:
            raise RuntimeError("not a known LLM error")

    class CrashingClient:
        def __init__(self) -> None:
            self.messages = FailingMessages()

    forecaster = LLMForecaster(
        config=_enabled_anthropic_settings(), client=CrashingClient()
    )
    # Unknown errors propagate — caller (pipeline gather) catches them.
    with pytest.raises(RuntimeError):
        await forecaster.forecast(_signal(yes_price=0.42))


@pytest.mark.asyncio
async def test_forecast_returns_predicted_value_on_success() -> None:
    forecaster = LLMForecaster(
        config=_enabled_anthropic_settings(), client=FakeClaudeClient()
    )
    probability = await forecaster.forecast(
        _signal(market_id="mkt-OK", yes_price=0.31)
    )
    assert probability == pytest.approx(0.8)
