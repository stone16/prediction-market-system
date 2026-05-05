from __future__ import annotations

from pms.strategies.aggregate import Strategy
from pms.strategies.projections import (
    EvalSpec,
    FactorCompositionStep,
    ForecasterSpec,
    MarketSelectionSpec,
    RiskParams,
    StrategyConfig,
)

PAPER_MULTI_FACTOR_STRATEGY_ID = "paper_multi_factor_v1"
_FACTOR_FRESHNESS_S = 300.0


def build_paper_multi_factor_strategy() -> Strategy:
    return Strategy(
        config=StrategyConfig(
            strategy_id=PAPER_MULTI_FACTOR_STRATEGY_ID,
            factor_composition=(
                FactorCompositionStep(
                    factor_id="orderbook_imbalance",
                    role="threshold_edge",
                    param="",
                    weight=1.0,
                    threshold=0.10,
                    required=True,
                    freshness_sla_s=_FACTOR_FRESHNESS_S,
                    allow_neutral_fallback=False,
                ),
                FactorCompositionStep(
                    factor_id="orderbook_imbalance",
                    role="weighted",
                    param="",
                    weight=1.0,
                    threshold=None,
                    required=False,
                    freshness_sla_s=_FACTOR_FRESHNESS_S,
                    allow_neutral_fallback=False,
                ),
                FactorCompositionStep(
                    factor_id="rules",
                    role="blend_weighted",
                    param="",
                    weight=0.5,
                    threshold=None,
                    required=False,
                    allow_neutral_fallback=True,
                ),
            ),
            metadata=(
                ("owner", "Researcher-Ciga"),
                ("tier", "paper"),
                ("phase", "A"),
                ("purpose", "paper_multi_factor_phase_a"),
                ("price_reference", "best_ask"),
                ("live_allowed", "false"),
                ("requires_strict_factor_gates", "false"),
            ),
        ),
        risk=RiskParams(
            max_position_notional_usdc=2.0,
            max_daily_drawdown_pct=50.0,
            min_order_size_usdc=0.50,
        ),
        eval_spec=EvalSpec(
            metrics=("brier", "pnl", "fill_rate"),
            max_brier_score=0.30,
            slippage_threshold_bps=50.0,
            min_win_rate=0.45,
        ),
        forecaster=ForecasterSpec(
            forecasters=(
                ("rules", (("threshold", "0.55"),)),
                ("stats", (("window", "15m"),)),
                ("llm", ()),
            )
        ),
        market_selection=MarketSelectionSpec(
            venue="polymarket",
            resolution_time_max_horizon_days=90,
            volume_min_usdc=100.0,
        ),
    )
