"""In-memory ``MetricsCollectorProtocol`` implementation (CP09).

Design notes
------------

* **Non-persistent**: all state lives in process-local Python data
  structures. Restarting the process erases metrics. A persistent backend
  is deferred — ``StorageProtocol`` (CP01) is reserved for that future
  iteration. This is documented both here and in the CP09 output summary.
* **Strategy attribution**: the ``MetricsCollectorProtocol.record_order``
  contract only receives ``Order`` + ``OrderResult``. Neither carries a
  strategy identifier natively, so the collector inspects
  ``result.raw["strategy"]`` for attribution. Orders without a strategy
  tag fall into the ``"unknown"`` bucket. This matches the CP09 spec
  ("attributed via result.raw['strategy']").
* **Fill latency**: CP09 does not receive real timing telemetry yet —
  ``record_order`` is called synchronously after ``submit_order`` in the
  pipeline. ``avg_fill_latency_ms`` is therefore always ``0.0`` for v1;
  the field exists so the shape is stable for a future CP where the
  pipeline timestamps submit/fill.
* **Unrealized P&L**: v1 has no mark-to-market loop (would require
  position bookkeeping + live mid-prices). ``unrealized`` is always
  ``Decimal("0")``. ``total = realized + unrealized`` is preserved so
  consumers never have to re-derive it.
* **Win rate**: v1 uses the fraction of *filled* orders as a proxy for
  win rate. A true win rate needs round-trip position P&L tracking, which
  is out of scope for CP09.

P&L Accounting Model (v1 — review-loop fix f4)
----------------------------------------------

``get_pnl()`` and the per-strategy ``pnl`` field both report **signed
cash flow**, not realized profit-and-loss against cost basis. Concretely:

- A buy fill is a negative cash flow (cash leaves the account).
- A sell fill is a positive cash flow (cash enters the account).
- The ``realized`` field of :class:`pms.models.PnLReport` is the **sum**
  of these signed flows over the requested window.

This means a snapshot taken after a single buy will show a "realized
loss" equal to the cost of the position, and the matching sell on a
later snapshot will show a "realized gain" of the proceeds. **In
isolation, neither number is the trading P&L of the round trip.** Over
the full lifecycle of every position the cash flows do net to true
realized P&L, but at any intermediate point the value is biased by
half-open trades.

The collector preserves the ``realized`` field name on
:class:`PnLReport` so the shape is stable for the post-v1 ledger
checkpoint, when ``_signed_cash_flow`` will be replaced with cost-basis
matching against a positions ledger. That ledger is documented in the
spec under "Out of Scope — Live positions tracking"; until it lands,
treat ``realized`` as a cash-flow proxy and consult the underlying
``num_trades`` and per-strategy slippage for richer insight.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal

from pms.models import (
    Order,
    OrderResult,
    PerformanceReport,
    PnLReport,
    PriceUpdate,
    StrategyMetrics,
)

UTC = timezone.utc


@dataclass
class _OrderRecord:
    """Internal bookkeeping record for a submitted order + its result."""

    order: Order
    result: OrderResult
    recorded_at: datetime


class MetricsCollector:
    """In-memory implementation of :class:`MetricsCollectorProtocol`.

    All storage is per-process. See the module docstring for rationale
    on why this is acceptable for v1 and what the future backends look
    like.
    """

    def __init__(self) -> None:
        self._order_records: list[_OrderRecord] = []
        self._price_snapshots: list[PriceUpdate] = []

    # ------------------------------------------------------------------
    # Write path
    # ------------------------------------------------------------------

    async def record_order(self, order: Order, result: OrderResult) -> None:
        """Store ``(order, result)`` in memory, tagged with the current time."""
        self._order_records.append(
            _OrderRecord(
                order=order,
                result=result,
                recorded_at=datetime.now(UTC),
            )
        )

    async def record_price_snapshot(self, updates: list[PriceUpdate]) -> None:
        """Append all ``updates`` to the in-memory snapshot log."""
        self._price_snapshots.extend(updates)

    # ------------------------------------------------------------------
    # Read path
    # ------------------------------------------------------------------

    def get_pnl(self, since: datetime) -> PnLReport:
        """Compute the v1 cash-flow proxy for realized + unrealized P&L.

        Only ``filled`` order records contribute. ``unrealized`` is
        always zero in v1 (see module docstring).

        **Important** (review-loop fix f4): the ``realized`` field on the
        returned :class:`PnLReport` is the sum of signed *cash flows*
        (``-price*size`` for buys, ``+price*size`` for sells), not the
        cost-basis-matched profit of closed trades. A snapshot taken
        between a buy and its closing sell will therefore report a
        misleading number — see "P&L Accounting Model" in the module
        docstring for the rationale and the deferred-ledger plan.
        """
        end = datetime.now(UTC)
        relevant = [r for r in self._order_records if r.recorded_at >= since]

        # ``cash_flow`` is the variable name we *want* (review-loop fix
        # f4); we still report it on ``PnLReport.realized`` to keep the
        # field name stable for the post-v1 cost-basis upgrade. Renaming
        # the field would break the model contract for callers; renaming
        # the local variable here is documentation that this number is
        # not yet "true realized P&L".
        cash_flow = Decimal("0")
        num_trades = 0
        for record in relevant:
            if record.result.status != "filled":
                continue
            num_trades += 1
            cash_flow += _signed_cash_flow(record.order, record.result)

        unrealized = Decimal("0")
        total = cash_flow + unrealized

        return PnLReport(
            start=since,
            end=end,
            realized=cash_flow,
            unrealized=unrealized,
            total=total,
            num_trades=num_trades,
        )

    def get_performance_metrics(self) -> PerformanceReport:
        """Compute per-strategy performance metrics over all recorded orders."""
        end = datetime.now(UTC)
        start = (
            self._order_records[0].recorded_at
            if self._order_records
            else end
        )

        grouped: dict[str, list[_OrderRecord]] = defaultdict(list)
        for record in self._order_records:
            strategy_name = str(record.result.raw.get("strategy", "unknown"))
            grouped[strategy_name].append(record)

        per_strategy: dict[str, StrategyMetrics] = {}
        for strategy_name, records in grouped.items():
            per_strategy[strategy_name] = _compute_strategy_metrics(
                strategy_name, records
            )

        return PerformanceReport(
            start=start,
            end=end,
            per_strategy=per_strategy,
        )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _signed_cash_flow(order: Order, result: OrderResult) -> Decimal:
    """Return the cash flow of a filled order as a signed ``Decimal``.

    Buy → negative (cash out), sell → positive (cash in).
    """
    sign = Decimal("-1") if order.side == "buy" else Decimal("1")
    return sign * result.filled_price * result.filled_size


def _compute_strategy_metrics(
    strategy_name: str, records: list[_OrderRecord]
) -> StrategyMetrics:
    num_orders = len(records)
    num_fills = sum(1 for r in records if r.result.status == "filled")

    # v1 win rate proxy = fraction of filled orders.
    win_rate = num_fills / num_orders if num_orders > 0 else 0.0

    # Average slippage = |filled_price - order.price| / order.price for
    # every filled record (ignoring zero-price divide).
    slippage_samples: list[float] = []
    pnl_total = Decimal("0")
    for record in records:
        if record.result.status != "filled":
            continue
        pnl_total += _signed_cash_flow(record.order, record.result)
        if record.order.price > 0:
            diff = abs(record.result.filled_price - record.order.price)
            slippage_samples.append(float(diff / record.order.price))

    avg_slippage = (
        sum(slippage_samples) / len(slippage_samples)
        if slippage_samples
        else 0.0
    )

    return StrategyMetrics(
        strategy_name=strategy_name,
        num_orders=num_orders,
        num_fills=num_fills,
        win_rate=win_rate,
        avg_slippage=avg_slippage,
        avg_fill_latency_ms=0.0,
        pnl=float(pnl_total),
    )
