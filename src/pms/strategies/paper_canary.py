from __future__ import annotations

from pms.strategies.aggregate import Strategy
from pms.strategies.projections import (
    EvalSpec,
    ForecasterSpec,
    MarketSelectionSpec,
    RiskParams,
    StrategyConfig,
)

PAPER_CANARY_STRATEGY_ID = "paper_canary_v1"


def build_paper_canary_strategy() -> Strategy:
    return Strategy(
        config=StrategyConfig(
            strategy_id=PAPER_CANARY_STRATEGY_ID,
            factor_composition=(),
            metadata=(
                ("owner", "system"),
                ("purpose", "paper_e2e_canary"),
                ("price_reference", "best_ask"),
                ("event_filter", "book"),
                ("sample", "0/25"),
                ("live_allowed", "false"),
            ),
        ),
        risk=RiskParams(
            max_position_notional_usdc=1.0,
            max_daily_drawdown_pct=0.5,
            min_order_size_usdc=1.0,
        ),
        eval_spec=EvalSpec(metrics=("brier", "pnl", "fill_rate")),
        forecaster=ForecasterSpec(
            forecasters=(
                (
                    "paper_canary",
                    (
                        ("edge_bps", "1000"),
                        ("max_probability", "0.97"),
                        ("min_price", "0.05"),
                        ("max_price", "0.90"),
                        ("sample_modulus", "25"),
                        ("sample_remainder", "0"),
                    ),
                ),
            )
        ),
        market_selection=MarketSelectionSpec(
            venue="polymarket",
            resolution_time_max_horizon_days=90,
            volume_min_usdc=500.0,
        ),
    )
