from __future__ import annotations

from datetime import UTC, datetime
from hashlib import sha256
import importlib
import json
from pathlib import Path
from typing import Any

import pytest

from pms.config import PMSSettings, StrategyRuntimeSettings
from pms.controller.forecasters.flb import FlbForecaster
from pms.core.enums import RunMode
from pms.core.models import EvalRecord, MarketSignal, Portfolio
from pms.strategies.flb import H1_FLB_STRATEGY_ID, build_h1_flb_strategy
from pms.strategies.projections import (
    EvalSpec,
    CalibrationSpec,
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


def _signal(
    *,
    token_id: str = "shared-token",
    external_signal: dict[str, Any] | None = None,
) -> MarketSignal:
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
        external_signal=external_signal or {"fair_value": 0.55},
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


def _write_valid_flb_calibration_csv(path: Path) -> Path:
    path.write_text(
        "\n".join(
            (
                "signal_name,probability_estimate,sample_count,source_label",
                "longshot_yes_overpriced_buy_no,0.99,150,warehouse-flb-v1",
                "favorite_yes_underpriced_buy_yes,0.97,151,warehouse-flb-v1",
            )
        )
        + "\n",
        encoding="utf-8",
    )
    Path(f"{path}.provenance.json").write_text(
        json.dumps(
            {
                "artifact_type": "flb_calibration_provenance",
                "generated_by": "scripts/flb_data_feasibility.py",
                "source": "warehouse-csv",
                "generated_at": "2026-06-01T00:00:00+00:00",
                "warehouse_csv_sha256": sha256(
                    b"unit warehouse provenance fixture"
                ).hexdigest(),
                "warehouse_market_count": 301,
                "warehouse_longshot_count": 150,
                "warehouse_favorite_count": 151,
                "calibration_csv_sha256": sha256(path.read_bytes()).hexdigest(),
                "calibration_source_label": "warehouse-flb-v1",
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    return path


def _active_strategy(
    *,
    strategy_id: str,
    strategy_version_id: str,
    forecaster_names: tuple[str, ...],
    metadata: tuple[tuple[str, str], ...] = (("owner", "test"),),
    calibration: CalibrationSpec | None = None,
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
            metadata=metadata,
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
        calibration=calibration or CalibrationSpec(),
    )


@pytest.mark.asyncio
async def test_controller_pipeline_factory_builds_distinct_per_strategy_pipelines(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "pms.controller.forecasters.rules.RulesForecaster.predict",
        lambda self, signal: (0.65, 0.9, "test-rules"),
    )
    monkeypatch.setattr(
        "pms.controller.forecasters.statistical.StatisticalForecaster.predict",
        lambda self, signal: (0.65, 0.9, "test-stats"),
    )
    monkeypatch.setattr(
        "pms.controller.forecasters.llm.LLMForecaster.predict",
        lambda self, signal: (0.65, 0.9, "test-llm"),
    )
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


def test_live_controller_factory_rejects_placeholder_strategy_alpha_metadata() -> None:
    factory_cls = _load_symbol("pms.controller.factory", "ControllerPipelineFactory")
    settings = PMSSettings()
    settings.mode = RunMode.LIVE
    factory = factory_cls(settings=settings)
    strategy = _active_strategy(
        strategy_id="strat-placeholder",
        strategy_version_id="strat-placeholder-v1",
        forecaster_names=("rules",),
        metadata=(
            ("owner", "test"),
            ("live_allowed", "true"),
            ("alpha_source", "placeholder"),
        ),
        calibration=CalibrationSpec(enabled=True),
    )

    with pytest.raises(ValueError, match="alpha_source"):
        factory.build(strategy)


def test_live_controller_factory_requires_strategy_evidence_metadata() -> None:
    factory_cls = _load_symbol("pms.controller.factory", "ControllerPipelineFactory")
    settings = PMSSettings()
    settings.mode = RunMode.LIVE
    factory = factory_cls(settings=settings)
    strategy = _active_strategy(
        strategy_id="strat-missing-evidence",
        strategy_version_id="strat-missing-evidence-v1",
        forecaster_names=("rules",),
        metadata=(("owner", "test"), ("live_allowed", "true")),
        calibration=CalibrationSpec(enabled=True),
    )

    with pytest.raises(ValueError, match="alpha_source"):
        factory.build(strategy)


def test_live_controller_factory_builds_h1_flb_with_calibration_artifact(
    tmp_path: Path,
) -> None:
    factory_cls = _load_symbol("pms.controller.factory", "ControllerPipelineFactory")
    calibration_path = _write_valid_flb_calibration_csv(
        tmp_path / "flb-calibration.csv"
    )
    settings = PMSSettings(
        mode=RunMode.LIVE,
        strategies=StrategyRuntimeSettings(
            flb_calibration_path=str(calibration_path),
        ),
    )
    strategy = build_h1_flb_strategy().to_active(
        strategy_version_id="h1-flb-test-v1"
    )

    pipeline = factory_cls(settings=settings).build(strategy)

    assert strategy.strategy_id == H1_FLB_STRATEGY_ID
    assert pipeline.strategy_id == H1_FLB_STRATEGY_ID
    assert tuple(type(item) for item in pipeline.forecasters) == (FlbForecaster,)


def test_live_controller_factory_rejects_h1_flb_without_calibration_provenance(
    tmp_path: Path,
) -> None:
    factory_cls = _load_symbol("pms.controller.factory", "ControllerPipelineFactory")
    calibration_path = tmp_path / "flb-calibration.csv"
    calibration_path.write_text(
        "\n".join(
            (
                "signal_name,probability_estimate,sample_count,source_label",
                "longshot_yes_overpriced_buy_no,0.99,150,warehouse-flb-v1",
                "favorite_yes_underpriced_buy_yes,0.97,151,warehouse-flb-v1",
            )
        )
        + "\n",
        encoding="utf-8",
    )
    settings = PMSSettings(
        mode=RunMode.LIVE,
        strategies=StrategyRuntimeSettings(
            flb_calibration_path=str(calibration_path),
        ),
    )
    strategy = build_h1_flb_strategy().to_active(
        strategy_version_id="h1-flb-test-v1"
    )

    with pytest.raises(ValueError, match="FLB calibration provenance JSON"):
        factory_cls(settings=settings).build(strategy)


def test_live_controller_factory_rejects_h1_flb_without_calibration_artifact() -> None:
    factory_cls = _load_symbol("pms.controller.factory", "ControllerPipelineFactory")
    settings = PMSSettings(
        mode=RunMode.LIVE,
        strategies=StrategyRuntimeSettings(flb_calibration_path=None),
    )
    strategy = build_h1_flb_strategy().to_active(
        strategy_version_id="h1-flb-test-v1"
    )

    with pytest.raises(ValueError, match="flb_calibration_path"):
        factory_cls(settings=settings).build(strategy)


@pytest.mark.asyncio
async def test_controller_pipeline_carries_signal_risk_group_into_decision(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "pms.controller.forecasters.rules.RulesForecaster.predict",
        lambda self, signal: (0.65, 0.9, "test-rules"),
    )
    factory_cls = _load_symbol("pms.controller.factory", "ControllerPipelineFactory")
    factory = factory_cls(settings=PMSSettings())
    pipeline = factory.build(
        _active_strategy(
            strategy_id="strat-a",
            strategy_version_id="strat-a-v1",
            forecaster_names=("rules",),
        )
    )

    decision = await pipeline.decide(
        _signal(
            external_signal={
                "fair_value": 0.55,
                "risk_group_id": "event:2028-us-presidential-election",
            }
        ),
        portfolio=_portfolio(),
    )

    assert decision is not None
    assert decision.risk_group_id == "event:2028-us-presidential-election"


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
