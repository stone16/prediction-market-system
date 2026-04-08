"""Report models referenced by ``MetricsCollectorProtocol``.

CP01 shipped placeholder shapes so the Protocol signatures were
expressible. CP09 lands the full schemas used by the evaluation layer.

Design notes
------------

* ``PnLReport``: carries ``cash_flow``, ``realized_pnl``, ``unrealized_pnl``,
  ``total``, ``num_trades`` over a ``[start, end]`` window. ``cash_flow`` is
  the only field populated by v1's :class:`~pms.evaluation.MetricsCollector`
  (the others are always ``Decimal("0")`` until cost-basis tracking lands).
* ``StrategyMetrics``: per-strategy performance numbers consumed by the
  ``FeedbackEngine``. This replaces the previous (incorrect) use of
  ``StrategyFeedback`` in ``PerformanceReport`` — ``StrategyFeedback`` is
  the *output* of the feedback engine, not raw metrics.
* ``PerformanceReport``: carries a ``start``/``end`` window in addition to
  the per-strategy metrics map, so downstream consumers (dashboards,
  feedback engines) can reason about the observation window.

P&L field semantics (review-loop fix f11, round 2)
--------------------------------------------------

Earlier iterations used a single ``realized`` field on :class:`PnLReport`
and a ``pnl`` field on :class:`StrategyMetrics`. Both contained the
v1 cash-flow proxy, not true cost-basis-matched P&L, so the labels lied
to every reader. Round 2 splits these into honest fields:

- ``cash_flow``: signed cash in/out from filled trades. v1 reports this
  honestly (buy → negative, sell → positive). It is a useful proxy but
  is biased by half-open positions: a single open buy looks like a loss
  until its closing sell happens.
- ``realized_pnl``: cost-basis-matched profit on closed positions. v1
  has no cost-basis ledger so this is permanently ``0`` until a
  post-v1 checkpoint adds position bookkeeping.
- ``unrealized_pnl``: mark-to-market profit on open positions. Same
  story — permanently ``0`` in v1.
- ``total``: ``cash_flow + realized_pnl + unrealized_pnl``. In v1 this
  reduces to ``cash_flow`` because the other two fields are zero.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal


@dataclass(frozen=True)
class PnLReport:
    """Platform-agnostic P&L report over a time window.

    See the module docstring for the semantics of each field. Briefly:

    - ``cash_flow`` is the v1 best-effort signed cash flow from fills.
    - ``realized_pnl`` and ``unrealized_pnl`` are always ``Decimal("0")``
      in v1 (cost-basis tracking is deferred).
    - ``total = cash_flow + realized_pnl + unrealized_pnl``.
    """

    start: datetime
    end: datetime
    cash_flow: Decimal
    realized_pnl: Decimal
    unrealized_pnl: Decimal
    total: Decimal
    num_trades: int


@dataclass(frozen=True)
class StrategyMetrics:
    """Per-strategy performance numbers over a window.

    All ratios are ``float`` in ``[0.0, 1.0]``. ``cash_flow`` and
    ``realized_pnl`` are ``float`` (dollars) rather than ``Decimal``
    because they flow into ``StrategyFeedback`` which is already
    ``float``-typed by CP01.

    ``cash_flow`` is the v1 signed-cash-flow proxy. ``realized_pnl`` is
    permanently ``0.0`` until cost-basis tracking lands (see
    :class:`PnLReport`).
    """

    strategy_name: str
    num_orders: int
    num_fills: int
    win_rate: float
    avg_slippage: float
    avg_fill_latency_ms: float
    cash_flow: float
    realized_pnl: float


@dataclass(frozen=True)
class PerformanceReport:
    """Per-strategy performance snapshot consumed by the ``FeedbackEngine``."""

    start: datetime
    end: datetime
    per_strategy: dict[str, StrategyMetrics]
