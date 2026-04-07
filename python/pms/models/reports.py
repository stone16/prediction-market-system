"""Report models referenced by MetricsCollectorProtocol.

CP01 ships minimal placeholder shapes so the Protocol signatures are
expressible. The full schemas land in CP09 (evaluation layer).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from .feedback import StrategyFeedback


@dataclass(frozen=True)
class PnLReport:
    """Platform-agnostic P&L report over a time window.

    CP01 placeholder — extended in CP09.
    """

    start: datetime
    end: datetime
    realized: Decimal
    unrealized: Decimal


@dataclass(frozen=True)
class PerformanceReport:
    """Per-strategy performance snapshot consumed by the FeedbackEngine.

    CP01 placeholder — extended in CP09.
    """

    per_strategy: dict[str, StrategyFeedback]
