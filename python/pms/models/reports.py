"""Report models referenced by ``MetricsCollectorProtocol``.

CP01 shipped placeholder shapes so the Protocol signatures were
expressible. CP09 lands the full schemas used by the evaluation layer.

Design notes
------------

* ``PnLReport``: carries ``realized``, ``unrealized``, ``total``, ``num_trades``
  over a ``[start, end]`` window. ``unrealized`` is included for forward
  compatibility with mark-to-market but is always ``Decimal("0")`` in the
  v1 in-memory collector (no position bookkeeping yet).
* ``StrategyMetrics``: per-strategy performance numbers consumed by the
  ``FeedbackEngine``. This replaces the previous (incorrect) use of
  ``StrategyFeedback`` in ``PerformanceReport`` — ``StrategyFeedback`` is
  the *output* of the feedback engine, not raw metrics.
* ``PerformanceReport``: carries a ``start``/``end`` window in addition to
  the per-strategy metrics map, so downstream consumers (dashboards,
  feedback engines) can reason about the observation window.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal


@dataclass(frozen=True)
class PnLReport:
    """Platform-agnostic P&L report over a time window.

    ``realized`` + ``unrealized`` must equal ``total`` — ``total`` is stored
    explicitly so callers do not have to re-derive it.
    """

    start: datetime
    end: datetime
    realized: Decimal
    unrealized: Decimal
    total: Decimal
    num_trades: int


@dataclass(frozen=True)
class StrategyMetrics:
    """Per-strategy performance numbers over a window.

    All ratios are ``float`` in ``[0.0, 1.0]``. ``pnl`` is a ``float``
    (dollars) rather than ``Decimal`` because it flows into
    ``StrategyFeedback`` which is already ``float``-typed by CP01.
    """

    strategy_name: str
    num_orders: int
    num_fills: int
    win_rate: float
    avg_slippage: float
    avg_fill_latency_ms: float
    pnl: float


@dataclass(frozen=True)
class PerformanceReport:
    """Per-strategy performance snapshot consumed by the ``FeedbackEngine``."""

    start: datetime
    end: datetime
    per_strategy: dict[str, StrategyMetrics]
