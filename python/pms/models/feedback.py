"""Feedback models — produced by the evaluation layer, consumed by strategies."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal


@dataclass(frozen=True)
class StrategyFeedback:
    """Per-strategy performance summary and tuning suggestion.

    ``cash_flow`` is the signed-cash-flow proxy from
    :class:`pms.models.StrategyMetrics` (review-loop fix f11). The field
    was previously named ``pnl``, which was misleading because v1 has
    no cost-basis tracking. The new name matches the upstream
    ``StrategyMetrics.cash_flow`` field.
    """

    cash_flow: float
    win_rate: float
    avg_slippage: float
    suggestion: str


@dataclass(frozen=True)
class RiskFeedback:
    """Risk-manager-facing feedback (drawdown, exposure, suggested action)."""

    max_drawdown_hit: bool
    current_exposure: Decimal
    suggestion: str


@dataclass(frozen=True)
class ConnectorFeedback:
    """Connector health metrics and suggested adjustments."""

    data_staleness_ms: float
    api_error_rate: float
    suggestion: str


@dataclass(frozen=True)
class EvaluationFeedback:
    """Aggregated feedback packet emitted by the FeedbackEngine each cycle."""

    timestamp: datetime
    period: timedelta
    strategy_adjustments: dict[str, StrategyFeedback]
    risk_adjustments: RiskFeedback
    connector_adjustments: dict[str, ConnectorFeedback]
