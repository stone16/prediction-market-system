"""Rule-based ``FeedbackEngineProtocol`` implementation (CP09).

Design notes
------------

* **Rule-based, not ML**: CP09 wants a simple deterministic feedback
  surface. No learning, no stored history. The feedback engine is a
  pure function of a single ``PerformanceReport``.
* **Guardrails**: ``FEEDBACK_GUARDRAILS`` caps every emitted value to a
  sane range. Even if the upstream metrics collector produced a nonsense
  number (e.g. a negative win rate because of a bug), the feedback packet
  that reaches strategies and the risk manager stays inside the bounds.
  A fuzz test in ``tests/test_evaluation.py`` verifies this with 100
  random inputs.
* **Suggestion rules**:
    * ``win_rate < low_win_rate_threshold`` → ``raise_min_spread``
      (strategy is losing too often, trade less aggressively).
    * ``avg_slippage > 0.10`` (10%) → ``reduce_aggression``
      (fills are landing too far from the order price).
    * otherwise → ``hold``.
  Win rate takes precedence over slippage on purpose — if both conditions
  fire, the win-rate fix is more surgical.
* **Risk and connector feedback**: v1 does not observe risk state or
  connector health directly from the evaluation layer. ``risk_adjustments``
  emits a neutral ``hold`` packet with zero exposure; ``connector_adjustments``
  is empty. Future checkpoints can extend the engine without breaking
  the shape.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any, Final

from pms.models import (
    ConnectorFeedback,
    EvaluationFeedback,
    PerformanceReport,
    RiskFeedback,
    StrategyFeedback,
)

# ---------------------------------------------------------------------------
# Guardrail table
# ---------------------------------------------------------------------------

#: Hard bounds enforced on every value in an :class:`EvaluationFeedback`
#: packet. Keys use an explicit ``<component>_<field>_<min|max>`` scheme so
#: lookups are self-documenting.
#:
#: Review-loop fix f11 (round 2): the ``strategy_pnl_*`` keys are renamed
#: to ``strategy_cash_flow_*`` to match the new
#: :attr:`StrategyFeedback.cash_flow` field — the prior ``pnl`` label was
#: misleading because v1 does not yet match cost basis.
FEEDBACK_GUARDRAILS: Final[dict[str, Any]] = {
    "strategy_cash_flow_min": -1_000_000.0,
    "strategy_cash_flow_max": 1_000_000.0,
    "strategy_win_rate_min": 0.0,
    "strategy_win_rate_max": 1.0,
    "strategy_slippage_min": 0.0,
    "strategy_slippage_max": 1.0,
    "risk_exposure_min": Decimal("0"),
    "risk_exposure_max": Decimal("1000000"),
    "connector_staleness_min": 0.0,
    "connector_staleness_max": 3_600_000.0,  # 1 hour in ms
    "connector_error_rate_min": 0.0,
    "connector_error_rate_max": 1.0,
}


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------


class FeedbackEngine:
    """Rule-based feedback generator with hard guardrails on every output."""

    def __init__(
        self,
        low_win_rate_threshold: float = 0.4,
        high_slippage_threshold: float = 0.10,
    ) -> None:
        self._low_win_rate_threshold = low_win_rate_threshold
        self._high_slippage_threshold = high_slippage_threshold

    def generate_feedback(self, metrics: PerformanceReport) -> EvaluationFeedback:
        strategy_adjustments: dict[str, StrategyFeedback] = {}

        for name, sm in metrics.per_strategy.items():
            strategy_adjustments[name] = StrategyFeedback(
                cash_flow=_clamp_float(
                    sm.cash_flow,
                    FEEDBACK_GUARDRAILS["strategy_cash_flow_min"],
                    FEEDBACK_GUARDRAILS["strategy_cash_flow_max"],
                ),
                win_rate=_clamp_float(
                    sm.win_rate,
                    FEEDBACK_GUARDRAILS["strategy_win_rate_min"],
                    FEEDBACK_GUARDRAILS["strategy_win_rate_max"],
                ),
                avg_slippage=_clamp_float(
                    sm.avg_slippage,
                    FEEDBACK_GUARDRAILS["strategy_slippage_min"],
                    FEEDBACK_GUARDRAILS["strategy_slippage_max"],
                ),
                suggestion=self._suggest_for_strategy(
                    sm.win_rate, sm.avg_slippage
                ),
            )

        risk_adjustments = RiskFeedback(
            max_drawdown_hit=False,
            current_exposure=_clamp_decimal(
                Decimal("0"),
                FEEDBACK_GUARDRAILS["risk_exposure_min"],
                FEEDBACK_GUARDRAILS["risk_exposure_max"],
            ),
            suggestion="hold",
        )

        # v1: no connector observability inside the feedback engine.
        connector_adjustments: dict[str, ConnectorFeedback] = {}

        return EvaluationFeedback(
            timestamp=metrics.end,
            period=metrics.end - metrics.start,
            strategy_adjustments=strategy_adjustments,
            risk_adjustments=risk_adjustments,
            connector_adjustments=connector_adjustments,
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _suggest_for_strategy(
        self, win_rate: float, avg_slippage: float
    ) -> str:
        if win_rate < self._low_win_rate_threshold:
            return "raise_min_spread"
        if avg_slippage > self._high_slippage_threshold:
            return "reduce_aggression"
        return "hold"


# ---------------------------------------------------------------------------
# Clamping helpers
# ---------------------------------------------------------------------------


def _clamp_float(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def _clamp_decimal(value: Decimal, lo: Decimal, hi: Decimal) -> Decimal:
    return max(lo, min(hi, value))
