from __future__ import annotations

import json

from pms.storage.strategy_registry import _strategy_from_config_json, _strategy_to_config_json
from pms.strategies.aggregate import Strategy
from pms.strategies.projections import (
    EvalSpec,
    FactorCompositionStep,
    ForecasterSpec,
    MarketSelectionSpec,
    RiskParams,
    StrategyConfig,
)


def _strategy() -> Strategy:
    return Strategy(
        config=StrategyConfig(
            strategy_id="default",
            factor_composition=(
                FactorCompositionStep(
                    factor_id="fair_value_spread",
                    role="threshold_edge",
                    param="",
                    weight=1.0,
                    threshold=0.02,
                ),
                FactorCompositionStep(
                    factor_id="yes_count",
                    role="posterior_success",
                    param="",
                    weight=1.0,
                    threshold=None,
                ),
            ),
            metadata=(("owner", "system"), ("tier", "default")),
        ),
        risk=RiskParams(
            max_position_notional_usdc=100.0,
            max_daily_drawdown_pct=2.5,
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


def test_strategy_registry_rehydrates_legacy_factor_composition_shape() -> None:
    payload = {
        "config": {
            "strategy_id": "default",
            "factor_composition": [["factor-a", 0.6], ["factor-b", 0.4]],
            "metadata": [["owner", "system"], ["tier", "default"]],
        },
        "risk": {
            "max_position_notional_usdc": 100.0,
            "max_daily_drawdown_pct": 2.5,
            "min_order_size_usdc": 1.0,
        },
        "eval_spec": {"metrics": ["brier", "pnl", "fill_rate"]},
        "forecaster": {
            "forecasters": [["rules", [["threshold", "0.55"]]], ["stats", [["window", "15m"]]]]
        },
        "market_selection": {
            "venue": "polymarket",
            "resolution_time_max_horizon_days": 7,
            "volume_min_usdc": 500.0,
        },
    }

    strategy = _strategy_from_config_json(payload)

    assert strategy.config.factor_composition == (
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
    )
    assert strategy.eval_spec == EvalSpec(
        metrics=("brier", "pnl", "fill_rate"),
        max_brier_score=0.30,
        slippage_threshold_bps=50.0,
        min_win_rate=0.50,
    )


def test_strategy_registry_round_trips_new_factor_composition_shape() -> None:
    serialized = _strategy_to_config_json(_strategy())
    strategy = _strategy_from_config_json(json.loads(serialized))

    assert strategy == _strategy()


def test_strategy_registry_rehydrates_step_factor_composition_without_threshold_field() -> None:
    payload = {
        "config": {
            "strategy_id": "default",
            "factor_composition": [
                {
                    "factor_id": "factor-a",
                    "role": "weighted",
                    "param": "",
                    "weight": 0.6,
                },
                {
                    "factor_id": "factor-b",
                    "role": "threshold_edge",
                    "param": "",
                    "weight": 1.0,
                    "threshold": 0.02,
                },
            ],
            "metadata": [["owner", "system"], ["tier", "default"]],
        },
        "risk": {
            "max_position_notional_usdc": 100.0,
            "max_daily_drawdown_pct": 2.5,
            "min_order_size_usdc": 1.0,
        },
        "eval_spec": {"metrics": ["brier", "pnl", "fill_rate"]},
        "forecaster": {
            "forecasters": [["rules", [["threshold", "0.55"]]], ["stats", [["window", "15m"]]]]
        },
        "market_selection": {
            "venue": "polymarket",
            "resolution_time_max_horizon_days": 7,
            "volume_min_usdc": 500.0,
        },
    }

    strategy = _strategy_from_config_json(payload)

    assert strategy.config.factor_composition == (
        FactorCompositionStep(
            factor_id="factor-a",
            role="weighted",
            param="",
            weight=0.6,
            threshold=None,
        ),
        FactorCompositionStep(
            factor_id="factor-b",
            role="threshold_edge",
            param="",
            weight=1.0,
            threshold=0.02,
        ),
    )
