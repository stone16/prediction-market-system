from __future__ import annotations

from datetime import UTC, datetime
import importlib
from typing import Any

import pytest

from pms.config import PMSSettings
from pms.core.models import EvalRecord, MarketSignal, Portfolio
from pms.strategies.projections import (
    EvalSpec,
    ForecasterSpec,
    MarketSelectionSpec,
    RiskParams,
    StrategyConfig,
)


def _load_symbol(module_name: str, symbol_name: str) -> Any:
    try:
        module = importlib.import_module(module_name)
    except ModuleNotFoundError as exc:  # pragma: no cover - red phase
        pytest.fail(f"{module_name} is missing: {exc}")
    return getattr(module, symbol_name)


def _signal(*, token_id: str = "shared-token") -> MarketSignal:
    return MarketSignal(
        market_id="market-cp01",
        token_id=token_id,
        venue="polymarket",
        title="Will CP01 pass?",
        yes_price=0.4,
        volume_24h=1000.0,
        resolves_at=datetime(2026, 4, 20, tzinfo=UTC),
        orderbook={
            "bids": [{"price": 0.39, "size": 10.0}],
            "asks": [{"price": 0.41, "size": 10.0}],
        },
        external_signal={"fair_value": 0.55},
        fetched_at=datetime(2026, 4, 19, tzinfo=UTC),
        market_status="open",
    )


def _portfolio() -> Portfolio:
    return Portfolio(
        total_usdc=1000.0,
        free_usdc=1000.0,
        locked_usdc=0.0,
        open_positions=[],
    )


def _active_strategy(
    *,
    strategy_id: str,
    strategy_version_id: str,
    forecaster_names: tuple[str, ...],
) -> Any:
    active_strategy_cls = _load_symbol(
        "pms.strategies.projections",
        "ActiveStrategy",
    )
    return active_strategy_cls(
        strategy_id=strategy_id,
        strategy_version_id=strategy_version_id,
        config=StrategyConfig(
            strategy_id=strategy_id,
            factor_composition=(),
            metadata=(("owner", "test"),),
        ),
        risk=RiskParams(
            max_position_notional_usdc=100.0,
            max_daily_drawdown_pct=2.5,
            min_order_size_usdc=1.0,
        ),
        eval_spec=EvalSpec(metrics=("brier", "pnl")),
        forecaster=ForecasterSpec(
            forecasters=tuple((name, ()) for name in forecaster_names)
        ),
        market_selection=MarketSelectionSpec(
            venue="polymarket",
            resolution_time_max_horizon_days=7,
            volume_min_usdc=500.0,
        ),
    )


@pytest.mark.asyncio
async def test_controller_pipeline_factory_builds_distinct_per_strategy_pipelines() -> None:
    factory_cls = _load_symbol("pms.controller.factory", "ControllerPipelineFactory")
    factory = factory_cls(settings=PMSSettings())
    strategies = [
        _active_strategy(
            strategy_id="strat-a",
            strategy_version_id="strat-a-v1",
            forecaster_names=("rules", "stats"),
        ),
        _active_strategy(
            strategy_id="strat-b",
            strategy_version_id="strat-b-v1",
            forecaster_names=("rules", "stats", "llm"),
        ),
    ]

    pipelines = factory.build_many(strategies)

    assert list(pipelines) == ["strat-a", "strat-b"]
    assert [type(item).__name__ for item in pipelines["strat-a"].forecasters] == [
        "RulesForecaster",
        "StatisticalForecaster",
    ]
    assert [type(item).__name__ for item in pipelines["strat-b"].forecasters] == [
        "RulesForecaster",
        "StatisticalForecaster",
        "LLMForecaster",
    ]

    decision_a = await pipelines["strat-a"].decide(_signal(), portfolio=_portfolio())
    decision_b = await pipelines["strat-b"].decide(_signal(), portfolio=_portfolio())

    assert decision_a is not None
    assert decision_b is not None
    assert decision_a.strategy_id == "strat-a"
    assert decision_a.strategy_version_id == "strat-a-v1"
    assert decision_b.strategy_id == "strat-b"
    assert decision_b.strategy_version_id == "strat-b-v1"


def test_controller_pipeline_factory_keeps_calibrator_state_isolated_per_strategy() -> None:
    factory_cls = _load_symbol("pms.controller.factory", "ControllerPipelineFactory")
    factory = factory_cls(settings=PMSSettings())
    pipelines = factory.build_many(
        [
            _active_strategy(
                strategy_id="strat-a",
                strategy_version_id="strat-a-v1",
                forecaster_names=("stats",),
            ),
            _active_strategy(
                strategy_id="strat-b",
                strategy_version_id="strat-b-v1",
                forecaster_names=("stats",),
            ),
        ]
    )

    trained_records = [
        EvalRecord(
            market_id="market-cp01",
            decision_id=f"decision-{index}",
            strategy_id="default",
            strategy_version_id="default-v1",
            prob_estimate=0.4,
            resolved_outcome=1.0,
            brier_score=0.36,
            fill_status="matched",
            recorded_at=datetime(2026, 4, 19, tzinfo=UTC),
            citations=[],
        )
        for index in range(100)
    ]
    model_id = "StatisticalForecaster"

    pipelines["strat-a"].calibrator.add_samples(model_id, trained_records)

    assert pipelines["strat-a"].calibrator.calibrate(0.4, model_id=model_id) == pytest.approx(1.0)
    assert pipelines["strat-b"].calibrator.calibrate(0.4, model_id=model_id) == pytest.approx(0.4)


def test_controller_pipeline_factory_uses_global_total_exposure_for_strategy_risk() -> None:
    factory_cls = _load_symbol("pms.controller.factory", "ControllerPipelineFactory")
    settings = PMSSettings()
    settings.risk.max_total_exposure = 10_000.0
    factory = factory_cls(settings=settings)

    pipeline = factory.build(
        _active_strategy(
            strategy_id="strat-a",
            strategy_version_id="strat-a-v1",
            forecaster_names=("rules",),
        )
    )

    assert pipeline.sizer.risk.max_position_per_market == 100.0
    assert pipeline.sizer.risk.max_total_exposure == 10_000.0


def test_controller_pipeline_factory_rejects_llm_raw_params_until_supported() -> None:
    factory_cls = _load_symbol("pms.controller.factory", "ControllerPipelineFactory")
    active_strategy_cls = _load_symbol(
        "pms.strategies.projections",
        "ActiveStrategy",
    )
    factory = factory_cls(settings=PMSSettings())
    strategy = active_strategy_cls(
        strategy_id="strat-llm",
        strategy_version_id="strat-llm-v1",
        config=StrategyConfig(
            strategy_id="strat-llm",
            factor_composition=(),
            metadata=(("owner", "test"),),
        ),
        risk=RiskParams(
            max_position_notional_usdc=100.0,
            max_daily_drawdown_pct=2.5,
            min_order_size_usdc=1.0,
        ),
        eval_spec=EvalSpec(metrics=("brier",)),
        forecaster=ForecasterSpec(
            forecasters=(("llm", (("temperature", "0.7"),)),)
        ),
        market_selection=MarketSelectionSpec(
            venue="polymarket",
            resolution_time_max_horizon_days=7,
            volume_min_usdc=500.0,
        ),
    )

    with pytest.raises(ValueError, match="does not yet accept per-strategy params"):
        factory.build(strategy)
