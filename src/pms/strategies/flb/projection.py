from __future__ import annotations

from pms.strategies.aggregate import Strategy
from pms.strategies.projections import (
    CalibrationSpec,
    EvalSpec,
    ForecasterSpec,
    MarketSelectionSpec,
    RiskParams,
    StrategyConfig,
)

H1_FLB_STRATEGY_ID = "h1_flb"


def build_h1_flb_strategy() -> Strategy:
    return Strategy(
        config=StrategyConfig(
            strategy_id=H1_FLB_STRATEGY_ID,
            factor_composition=(),
            metadata=(
                ("owner", "system"),
                ("tier", "live_candidate"),
                ("phase", "H1"),
                ("purpose", "calibrated_h1_flb_paper_soak"),
                ("price_reference", "best_ask"),
                ("live_allowed", "true"),
                ("requires_strict_factor_gates", "true"),
                ("alpha_source", "warehouse_flb_decile_model_v1"),
                ("edge_model_source", "flb_calibration_model_v1"),
                ("calibration_source", "warehouse_flb_v1"),
                ("evidence_source", "paper_soak_go_report_v1"),
            ),
        ),
        risk=RiskParams(
            max_position_notional_usdc=1.0,
            max_daily_drawdown_pct=20.0,
            min_order_size_usdc=1.0,
        ),
        eval_spec=EvalSpec(
            metrics=("brier", "pnl", "fill_rate"),
            max_brier_score=0.20,
            slippage_threshold_bps=50.0,
            min_win_rate=0.45,
        ),
        forecaster=ForecasterSpec(forecasters=(("flb", ()),)),
        market_selection=MarketSelectionSpec(
            venue="polymarket",
            resolution_time_max_horizon_days=7,
            volume_min_usdc=500.0,
            yes_price_min=0.01,
            yes_price_max=0.99,
        ),
        calibration=CalibrationSpec(
            enabled=True,
            shrinkage_factor=1.0,
            shrinkage_bias=0.0,
            extreme_clamp_low=0.001,
            extreme_clamp_high=0.999,
            min_resolved_for_extreme=100,
        ),
    )
