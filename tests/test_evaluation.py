"""Tests for the CP09 evaluation layer — MetricsCollector + FeedbackEngine.

Acceptance criteria covered:

* ``MetricsCollector`` is an in-memory implementation of
  ``MetricsCollectorProtocol`` — explicitly non-persistent for v1.
* ``record_order`` and ``record_price_snapshot`` store data in memory.
* ``get_pnl`` returns correct realized P&L over a time window.
* ``get_performance_metrics`` returns per-strategy win rate, avg slippage,
  fill latency.
* ``FeedbackEngine.generate_feedback`` produces ``EvaluationFeedback`` with
  appropriate suggestions (low win rate → ``raise_min_spread``, high
  slippage → ``reduce_aggression``, otherwise ``hold``).
* All feedback adjustments are bounded by ``FEEDBACK_GUARDRAILS`` — a
  fuzz test with 100 random inputs confirms no adjustment exceeds bounds.
"""

from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

import pytest

from pms.evaluation import FEEDBACK_GUARDRAILS, FeedbackEngine, MetricsCollector
from pms.models import (
    EvaluationFeedback,
    Order,
    OrderResult,
    PerformanceReport,
    PnLReport,
    PriceUpdate,
    StrategyMetrics,
)
from pms.protocols import FeedbackEngineProtocol, MetricsCollectorProtocol

UTC = timezone.utc


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


def _order(
    order_id: str = "o-1",
    side: str = "buy",
    price: str = "0.50",
    size: str = "10",
    platform: str = "mock",
    market_id: str = "m-1",
) -> Order:
    return Order(
        order_id=order_id,
        platform=platform,
        market_id=market_id,
        outcome_id="yes",
        side=side,  # type: ignore[arg-type]
        price=Decimal(price),
        size=Decimal(size),
        order_type="limit",
    )


def _result(
    order_id: str = "o-1",
    status: str = "filled",
    filled_price: str = "0.50",
    filled_size: str = "10",
    strategy: str | None = "arb",
) -> OrderResult:
    raw: dict[str, Any] = {}
    if strategy is not None:
        raw["strategy"] = strategy
    return OrderResult(
        order_id=order_id,
        status=status,  # type: ignore[arg-type]
        filled_size=Decimal(filled_size),
        filled_price=Decimal(filled_price),
        message="",
        raw=raw,
    )


def _price_update(outcome_id: str = "yes") -> PriceUpdate:
    return PriceUpdate(
        platform="mock",
        market_id="m-1",
        outcome_id=outcome_id,
        bid=Decimal("0.49"),
        ask=Decimal("0.51"),
        last=Decimal("0.50"),
        timestamp=datetime(2026, 1, 1, tzinfo=UTC),
    )


# ---------------------------------------------------------------------------
# Protocol conformance
# ---------------------------------------------------------------------------


def test_metrics_collector_implements_protocol() -> None:
    collector = MetricsCollector()
    assert isinstance(collector, MetricsCollectorProtocol)


def test_feedback_engine_implements_protocol() -> None:
    engine = FeedbackEngine()
    assert isinstance(engine, FeedbackEngineProtocol)


# ---------------------------------------------------------------------------
# MetricsCollector.get_pnl
# ---------------------------------------------------------------------------


async def test_get_pnl_empty_collector_returns_zero() -> None:
    collector = MetricsCollector()
    since = datetime.now(UTC) - timedelta(hours=1)

    report = collector.get_pnl(since)

    assert isinstance(report, PnLReport)
    assert report.realized == Decimal("0")
    assert report.unrealized == Decimal("0")
    assert report.total == Decimal("0")
    assert report.num_trades == 0
    assert report.start == since
    assert report.end >= since


async def test_get_pnl_sums_filled_orders() -> None:
    collector = MetricsCollector()

    # Buy 10 @ 0.50 → cash out 5.00
    await collector.record_order(
        _order(order_id="o-1", side="buy", price="0.50", size="10"),
        _result(order_id="o-1", filled_price="0.50", filled_size="10"),
    )
    # Sell 10 @ 0.60 → cash in 6.00
    await collector.record_order(
        _order(order_id="o-2", side="sell", price="0.60", size="10"),
        _result(order_id="o-2", filled_price="0.60", filled_size="10"),
    )

    since = datetime.now(UTC) - timedelta(hours=1)
    report = collector.get_pnl(since)

    assert report.num_trades == 2
    # -5.00 + 6.00 = 1.00
    assert report.realized == Decimal("1.00")
    assert report.unrealized == Decimal("0")
    assert report.total == Decimal("1.00")


async def test_get_pnl_ignores_non_filled_orders() -> None:
    collector = MetricsCollector()

    await collector.record_order(
        _order(order_id="o-1", side="buy", price="0.50", size="10"),
        _result(order_id="o-1", status="rejected", filled_price="0", filled_size="0"),
    )
    await collector.record_order(
        _order(order_id="o-2", side="buy", price="0.50", size="10"),
        _result(order_id="o-2", status="error", filled_price="0", filled_size="0"),
    )

    since = datetime.now(UTC) - timedelta(hours=1)
    report = collector.get_pnl(since)

    assert report.num_trades == 0
    assert report.realized == Decimal("0")


async def test_get_pnl_time_window_filters_older_entries() -> None:
    collector = MetricsCollector()

    # Record three orders; the first two should be kept even when ``since``
    # is "now minus 1 second" because we record right before sampling.
    await collector.record_order(
        _order(order_id="o-1", side="buy", price="0.50", size="10"),
        _result(order_id="o-1", filled_price="0.50", filled_size="10"),
    )

    # Mark a cutoff AFTER the first order was recorded. Orders recorded
    # strictly before ``cutoff`` must not count.
    cutoff = datetime.now(UTC) + timedelta(microseconds=1)

    # Force a monotonic forward step so the "after" orders are strictly
    # later than ``cutoff``.
    import asyncio

    await asyncio.sleep(0.01)

    await collector.record_order(
        _order(order_id="o-2", side="sell", price="0.60", size="10"),
        _result(order_id="o-2", filled_price="0.60", filled_size="10"),
    )

    report = collector.get_pnl(cutoff)

    # Only the second order should be counted.
    assert report.num_trades == 1
    assert report.realized == Decimal("6.00")


async def test_record_price_snapshot_stores_updates() -> None:
    collector = MetricsCollector()
    updates = [_price_update("yes"), _price_update("no")]

    await collector.record_price_snapshot(updates)

    # White-box peek: the collector should remember the snapshots.
    assert len(collector._price_snapshots) == 2


# ---------------------------------------------------------------------------
# MetricsCollector.get_performance_metrics
# ---------------------------------------------------------------------------


async def test_get_performance_metrics_empty() -> None:
    collector = MetricsCollector()
    report = collector.get_performance_metrics()

    assert isinstance(report, PerformanceReport)
    assert report.per_strategy == {}
    # Start and end are both populated even when empty.
    assert report.start <= report.end


async def test_get_performance_metrics_groups_by_strategy() -> None:
    collector = MetricsCollector()

    # Strategy A: 2 filled / 2 orders → win_rate 1.0
    await collector.record_order(
        _order(order_id="a-1"),
        _result(order_id="a-1", filled_price="0.50", filled_size="10", strategy="A"),
    )
    await collector.record_order(
        _order(order_id="a-2"),
        _result(order_id="a-2", filled_price="0.50", filled_size="10", strategy="A"),
    )

    # Strategy B: 1 filled / 2 orders → win_rate 0.5
    await collector.record_order(
        _order(order_id="b-1"),
        _result(order_id="b-1", filled_price="0.50", filled_size="10", strategy="B"),
    )
    await collector.record_order(
        _order(order_id="b-2"),
        _result(
            order_id="b-2",
            status="rejected",
            filled_price="0",
            filled_size="0",
            strategy="B",
        ),
    )

    report = collector.get_performance_metrics()

    assert set(report.per_strategy.keys()) == {"A", "B"}
    assert report.per_strategy["A"].num_orders == 2
    assert report.per_strategy["A"].num_fills == 2
    assert report.per_strategy["A"].win_rate == 1.0
    assert report.per_strategy["B"].num_orders == 2
    assert report.per_strategy["B"].num_fills == 1
    assert report.per_strategy["B"].win_rate == 0.5


async def test_get_performance_metrics_unknown_strategy_attribution() -> None:
    """Orders without a ``strategy`` tag are grouped under ``unknown``."""
    collector = MetricsCollector()

    await collector.record_order(
        _order(order_id="o-1"),
        _result(order_id="o-1", filled_price="0.50", filled_size="10", strategy=None),
    )

    report = collector.get_performance_metrics()
    assert "unknown" in report.per_strategy
    assert report.per_strategy["unknown"].num_orders == 1


async def test_get_performance_metrics_slippage_calculation() -> None:
    collector = MetricsCollector()

    # Order priced at 0.50, filled at 0.55 → slippage = 0.10
    await collector.record_order(
        _order(order_id="o-1", side="buy", price="0.50", size="10"),
        _result(order_id="o-1", filled_price="0.55", filled_size="10", strategy="arb"),
    )
    # Order priced at 0.50, filled at 0.45 → slippage = 0.10
    await collector.record_order(
        _order(order_id="o-2", side="sell", price="0.50", size="10"),
        _result(order_id="o-2", filled_price="0.45", filled_size="10", strategy="arb"),
    )

    report = collector.get_performance_metrics()
    arb = report.per_strategy["arb"]
    # Average slippage is 0.10
    assert arb.avg_slippage == pytest.approx(0.10, rel=1e-6)


async def test_get_performance_metrics_pnl_per_strategy() -> None:
    collector = MetricsCollector()

    # arb: buy 10 @ 0.50 then sell 10 @ 0.60 → pnl = 1.00
    await collector.record_order(
        _order(order_id="o-1", side="buy", price="0.50", size="10"),
        _result(order_id="o-1", filled_price="0.50", filled_size="10", strategy="arb"),
    )
    await collector.record_order(
        _order(order_id="o-2", side="sell", price="0.60", size="10"),
        _result(order_id="o-2", filled_price="0.60", filled_size="10", strategy="arb"),
    )

    report = collector.get_performance_metrics()
    assert report.per_strategy["arb"].pnl == pytest.approx(1.0, rel=1e-6)


# ---------------------------------------------------------------------------
# FeedbackEngine
# ---------------------------------------------------------------------------


def _perf_report(per_strategy: dict[str, StrategyMetrics]) -> PerformanceReport:
    return PerformanceReport(
        start=datetime(2026, 1, 1, tzinfo=UTC),
        end=datetime(2026, 1, 1, 0, 5, tzinfo=UTC),
        per_strategy=per_strategy,
    )


def _metrics(
    name: str = "arb",
    win_rate: float = 0.5,
    avg_slippage: float = 0.01,
    pnl: float = 0.0,
    num_orders: int = 10,
    num_fills: int = 5,
) -> StrategyMetrics:
    return StrategyMetrics(
        strategy_name=name,
        num_orders=num_orders,
        num_fills=num_fills,
        win_rate=win_rate,
        avg_slippage=avg_slippage,
        avg_fill_latency_ms=0.0,
        pnl=pnl,
    )


def test_generate_feedback_empty_metrics() -> None:
    engine = FeedbackEngine()
    report = _perf_report({})

    fb = engine.generate_feedback(report)

    assert isinstance(fb, EvaluationFeedback)
    assert fb.strategy_adjustments == {}
    assert fb.connector_adjustments == {}
    assert fb.timestamp == report.end
    assert fb.period == (report.end - report.start)


def test_generate_feedback_low_win_rate_suggests_raise_min_spread() -> None:
    engine = FeedbackEngine(low_win_rate_threshold=0.4)
    report = _perf_report({"arb": _metrics(win_rate=0.3, avg_slippage=0.01)})

    fb = engine.generate_feedback(report)

    assert fb.strategy_adjustments["arb"].suggestion == "raise_min_spread"


def test_generate_feedback_high_slippage_suggests_reduce_aggression() -> None:
    engine = FeedbackEngine()
    report = _perf_report({"arb": _metrics(win_rate=0.9, avg_slippage=0.25)})

    fb = engine.generate_feedback(report)

    assert fb.strategy_adjustments["arb"].suggestion == "reduce_aggression"


def test_generate_feedback_normal_conditions_suggests_hold() -> None:
    engine = FeedbackEngine()
    report = _perf_report({"arb": _metrics(win_rate=0.8, avg_slippage=0.01)})

    fb = engine.generate_feedback(report)

    assert fb.strategy_adjustments["arb"].suggestion == "hold"


def test_generate_feedback_period_matches_report_window() -> None:
    engine = FeedbackEngine()
    report = _perf_report({"arb": _metrics()})

    fb = engine.generate_feedback(report)

    assert fb.period == timedelta(minutes=5)


def test_generate_feedback_bounds_are_enforced_on_single_strategy() -> None:
    engine = FeedbackEngine()
    # Deliberately out-of-bounds inputs
    report = _perf_report(
        {
            "bad": _metrics(
                win_rate=2.5,
                avg_slippage=-0.5,
                pnl=5_000_000.0,
            )
        }
    )

    fb = engine.generate_feedback(report)
    bad = fb.strategy_adjustments["bad"]

    assert 0.0 <= bad.win_rate <= 1.0
    assert 0.0 <= bad.avg_slippage <= 1.0
    assert (
        FEEDBACK_GUARDRAILS["strategy_pnl_min"]
        <= bad.pnl
        <= FEEDBACK_GUARDRAILS["strategy_pnl_max"]
    )


def test_generate_feedback_fuzz_guardrails() -> None:
    """100 random ``StrategyMetrics`` — no generated StrategyFeedback escapes bounds."""
    engine = FeedbackEngine()
    rng = random.Random(42)

    for i in range(100):
        # Generate metrics with extreme values: some in-range, some way outside,
        # including negative and very large values.
        metrics = _metrics(
            name=f"strat-{i}",
            win_rate=rng.uniform(-5.0, 5.0),
            avg_slippage=rng.uniform(-2.0, 2.0),
            pnl=rng.uniform(-5_000_000.0, 5_000_000.0),
        )
        report = _perf_report({metrics.strategy_name: metrics})
        fb = engine.generate_feedback(report)
        sf = fb.strategy_adjustments[metrics.strategy_name]

        assert 0.0 <= sf.win_rate <= 1.0, (
            f"iteration {i}: win_rate {sf.win_rate} out of bounds"
        )
        assert 0.0 <= sf.avg_slippage <= 1.0, (
            f"iteration {i}: avg_slippage {sf.avg_slippage} out of bounds"
        )
        assert (
            FEEDBACK_GUARDRAILS["strategy_pnl_min"]
            <= sf.pnl
            <= FEEDBACK_GUARDRAILS["strategy_pnl_max"]
        ), f"iteration {i}: pnl {sf.pnl} out of bounds"

        # RiskFeedback exposure must also be within bounds.
        assert (
            FEEDBACK_GUARDRAILS["risk_exposure_min"]
            <= fb.risk_adjustments.current_exposure
            <= FEEDBACK_GUARDRAILS["risk_exposure_max"]
        )


def test_feedback_guardrails_module_constant_has_required_keys() -> None:
    required = {
        "strategy_pnl_min",
        "strategy_pnl_max",
        "strategy_win_rate_min",
        "strategy_win_rate_max",
        "strategy_slippage_min",
        "strategy_slippage_max",
        "risk_exposure_min",
        "risk_exposure_max",
    }
    assert required.issubset(FEEDBACK_GUARDRAILS.keys())
