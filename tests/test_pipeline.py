"""Tests for the TradingPipeline orchestrator (CP06).

Covers every CP06 acceptance criterion:

1. ``TradingPipeline.__init__`` accepts all Protocol implementations via
   constructor injection (verified structurally — wiring happens in the
   happy-path test).
2. ``TradingPipeline.run_cycle`` executes a full sense → strategy → risk →
   execute → evaluate → feedback loop (`test_run_cycle_happy_path`).
3. When the strategy emits no orders, risk + executor are never called
   (`test_run_cycle_empty_strategy_skips_risk_and_execute`).
4. When the risk manager rejects every order, the executor is never called
   (`test_run_cycle_all_orders_rejected_skips_execute`).
5. When a connector raises ``ConnectionError``, the pipeline logs the error,
   records it in ``CycleReport.errors``, and does not crash
   (`test_run_cycle_connector_connection_error_is_handled`).
6. Config file (``config.yaml``) resolves module implementations
   (`test_load_config_parses_yaml_into_pipeline_config`).
7. ``ModuleRegistry`` resolves a class-path string to an instance
   (`test_module_registry_instantiates_class_by_path`,
    `test_module_registry_raises_on_bad_path`).

Spy mocks below (``MockConnector``, ``SpyExecutor``, etc.) are in-test
fakes. They are not production code and live only in this module so they
can stay close to the tests that use them and so ``ModuleRegistry`` can
resolve ``tests.test_pipeline.FakeRegistryTarget`` by full class path.

Note: all mocks only implement the Protocol methods actually exercised by
the pipeline cycle. Unused methods like ``stream_prices`` and
``get_historical_prices`` raise ``NotImplementedError`` on purpose so any
future pipeline code accidentally touching them fails loudly.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from pms.models import (
    ConnectorFeedback,
    CorrelationPair,
    EvaluationFeedback,
    Market,
    Order,
    OrderBook,
    OrderResult,
    Outcome,
    PerformanceReport,
    PnLReport,
    Position,
    PriceUpdate,
    RiskDecision,
    RiskFeedback,
    StrategyFeedback,
)
from pms.evaluation.metrics import MetricsCollector
from pms.orchestrator.config import ModuleSpec, PipelineConfig, load_config
from pms.orchestrator.pipeline import CycleReport, TradingPipeline
from pms.orchestrator.registry import ModuleRegistry
from tests._registry_target import FakeRegistryTarget

# ---------------------------------------------------------------------------
# Sample domain objects
# ---------------------------------------------------------------------------


def _sample_market(
    platform: str = "mock", market_id: str = "m-1"
) -> Market:
    return Market(
        platform=platform,
        market_id=market_id,
        title="Sample",
        description="Sample market",
        outcomes=[
            Outcome(outcome_id="yes", title="Yes", price=Decimal("0.55")),
            Outcome(outcome_id="no", title="No", price=Decimal("0.45")),
        ],
        volume=Decimal("1000"),
        end_date=datetime(2030, 1, 1, tzinfo=timezone.utc),
        category="test",
        url="https://example.com/m-1",
        status="open",
        raw={},
    )


def _sample_order(order_id: str = "o-1") -> Order:
    return Order(
        order_id=order_id,
        platform="mock",
        market_id="m-1",
        outcome_id="yes",
        side="buy",
        price=Decimal("0.55"),
        size=Decimal("10"),
        order_type="limit",
    )


def _sample_order_result(order_id: str, status: str = "filled") -> OrderResult:
    return OrderResult(
        order_id=order_id,
        status=status,  # type: ignore[arg-type]
        filled_size=Decimal("10"),
        filled_price=Decimal("0.55"),
        message="",
        raw={},
    )


def _sample_feedback() -> EvaluationFeedback:
    return EvaluationFeedback(
        timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
        period=timedelta(minutes=5),
        strategy_adjustments={
            "mock": StrategyFeedback(
                cash_flow=0.0,
                win_rate=0.0,
                avg_slippage=0.0,
                suggestion="hold",
            )
        },
        risk_adjustments=RiskFeedback(
            max_drawdown_hit=False,
            current_exposure=Decimal("0"),
            suggestion="hold",
        ),
        connector_adjustments={
            "mock": ConnectorFeedback(
                data_staleness_ms=0.0, api_error_rate=0.0, suggestion="hold"
            )
        },
    )


# ---------------------------------------------------------------------------
# Mock connectors
# ---------------------------------------------------------------------------


class MockConnector:
    """A connector that returns a single sample market and records calls."""

    platform = "mock"

    def __init__(self, markets: list[Market] | None = None) -> None:
        self._markets = markets if markets is not None else [_sample_market()]
        self.get_active_markets_calls = 0

    async def get_active_markets(self) -> list[Market]:
        self.get_active_markets_calls += 1
        return list(self._markets)

    async def get_orderbook(self, market_id: str) -> OrderBook:
        raise NotImplementedError("not used by pipeline cycle in CP06")

    def stream_prices(
        self, market_ids: list[str]
    ) -> AsyncIterator[PriceUpdate]:
        raise NotImplementedError

    async def get_historical_prices(
        self, market_id: str, since: datetime
    ) -> list[PriceUpdate]:
        raise NotImplementedError


class BrokenConnector:
    """A connector that always raises ConnectionError from get_active_markets."""

    platform = "broken"

    def __init__(self) -> None:
        self.get_active_markets_calls = 0

    async def get_active_markets(self) -> list[Market]:
        self.get_active_markets_calls += 1
        raise ConnectionError("Simulated network failure")

    async def get_orderbook(self, market_id: str) -> OrderBook:
        raise NotImplementedError

    def stream_prices(
        self, market_ids: list[str]
    ) -> AsyncIterator[PriceUpdate]:
        raise NotImplementedError

    async def get_historical_prices(
        self, market_id: str, since: datetime
    ) -> list[PriceUpdate]:
        raise NotImplementedError


# ---------------------------------------------------------------------------
# Mock strategies
# ---------------------------------------------------------------------------


class MockStrategy:
    """A strategy that emits a preset list of orders on every price update.

    ``on_feedback`` records that it was called so the happy-path test can
    assert feedback dispatch.
    """

    name = "mock"

    def __init__(self, orders_to_emit: list[Order] | None = None) -> None:
        self._orders = orders_to_emit if orders_to_emit is not None else []
        self.on_price_update_calls = 0
        self.on_feedback_calls = 0

    async def on_price_update(
        self, update: PriceUpdate
    ) -> list[Order] | None:
        self.on_price_update_calls += 1
        return list(self._orders) if self._orders else None

    async def on_correlation_found(
        self, pair: Any
    ) -> list[Order] | None:
        return None

    async def on_feedback(self, feedback: EvaluationFeedback) -> None:
        self.on_feedback_calls += 1


class EmptyStrategy:
    """A strategy that never emits any orders."""

    name = "empty"

    def __init__(self) -> None:
        self.on_price_update_calls = 0

    async def on_price_update(
        self, update: PriceUpdate
    ) -> list[Order] | None:
        self.on_price_update_calls += 1
        return None

    async def on_correlation_found(
        self, pair: Any
    ) -> list[Order] | None:
        return None

    async def on_feedback(self, feedback: EvaluationFeedback) -> None:
        return None


# ---------------------------------------------------------------------------
# Mock executor / risk / metrics / feedback engine
# ---------------------------------------------------------------------------


class SpyExecutor:
    """Records every call and returns ``filled`` order results."""

    def __init__(self) -> None:
        self.submitted_orders: list[Order] = []
        self.get_positions_calls = 0

    async def submit_order(self, order: Order) -> OrderResult:
        self.submitted_orders.append(order)
        return _sample_order_result(order.order_id, status="filled")

    async def cancel_order(self, order_id: str) -> bool:
        return True

    async def get_positions(self) -> list[Position]:
        self.get_positions_calls += 1
        return []


class ApproveAllRisk:
    def __init__(self) -> None:
        self.check_order_calls = 0
        self.update_limits_calls = 0

    def check_order(
        self, order: Order, positions: list[Position]
    ) -> RiskDecision:
        self.check_order_calls += 1
        return RiskDecision(approved=True, reason="ok", adjusted_size=None)

    def update_limits(self, feedback: EvaluationFeedback) -> None:
        self.update_limits_calls += 1


class RejectAllRisk:
    def __init__(self) -> None:
        self.check_order_calls = 0
        self.update_limits_calls = 0

    def check_order(
        self, order: Order, positions: list[Position]
    ) -> RiskDecision:
        self.check_order_calls += 1
        return RiskDecision(
            approved=False, reason="reject-all", adjusted_size=None
        )

    def update_limits(self, feedback: EvaluationFeedback) -> None:
        self.update_limits_calls += 1


class SizeAdjustRisk:
    """Approves every order but forces a smaller size."""

    def __init__(self, new_size: Decimal) -> None:
        self._size = new_size
        self.check_order_calls = 0

    def check_order(
        self, order: Order, positions: list[Position]
    ) -> RiskDecision:
        self.check_order_calls += 1
        return RiskDecision(
            approved=True, reason="scaled", adjusted_size=self._size
        )

    def update_limits(self, feedback: EvaluationFeedback) -> None:
        return None


class MockMetrics:
    def __init__(self) -> None:
        self.record_order_calls: list[tuple[Order, OrderResult]] = []

    async def record_order(
        self, order: Order, result: OrderResult
    ) -> None:
        self.record_order_calls.append((order, result))

    async def record_price_snapshot(
        self, updates: list[PriceUpdate]
    ) -> None:
        return None

    def get_pnl(self, since: datetime) -> PnLReport:
        return PnLReport(
            start=since,
            end=since,
            cash_flow=Decimal("0"),
            realized_pnl=Decimal("0"),
            unrealized_pnl=Decimal("0"),
            total=Decimal("0"),
            num_trades=0,
        )

    def get_performance_metrics(self) -> PerformanceReport:
        return PerformanceReport(
            start=datetime(2026, 1, 1, tzinfo=timezone.utc),
            end=datetime(2026, 1, 1, tzinfo=timezone.utc),
            per_strategy={},
        )


class MockFeedbackEngine:
    def __init__(self) -> None:
        self.generate_feedback_calls = 0

    def generate_feedback(self, metrics: PerformanceReport) -> EvaluationFeedback:
        self.generate_feedback_calls += 1
        return _sample_feedback()


# ---------------------------------------------------------------------------
# Pipeline acceptance criteria tests
# ---------------------------------------------------------------------------


async def test_run_cycle_happy_path_executes_full_loop() -> None:
    """AC: happy path — every stage runs, every count is correct."""

    connector = MockConnector(markets=[_sample_market()])
    strategy = MockStrategy(orders_to_emit=[_sample_order("o-happy")])
    executor = SpyExecutor()
    risk = ApproveAllRisk()
    metrics = MockMetrics()
    feedback_engine = MockFeedbackEngine()

    pipeline = TradingPipeline(
        connectors=[connector],
        strategies=[strategy],
        executor=executor,
        risk_manager=risk,
        metrics=metrics,
        feedback_engine=feedback_engine,
    )

    report = await pipeline.run_cycle()

    # Sense
    assert connector.get_active_markets_calls == 1
    assert report.markets_fetched == 1
    # Strategy saw at least one price update (2 outcomes → 2 updates)
    assert strategy.on_price_update_calls == 2
    # The strategy emits its preset list for every update it sees
    assert report.orders_proposed == 2
    # Risk approved both
    assert risk.check_order_calls == 2
    assert report.orders_approved == 2
    # Executor submitted both
    assert len(executor.submitted_orders) == 2
    assert report.orders_submitted == 2
    assert report.orders_filled == 2
    # Metrics recorded each submission
    assert len(metrics.record_order_calls) == 2
    # Feedback engine + feedback dispatch ran
    assert feedback_engine.generate_feedback_calls == 1
    assert strategy.on_feedback_calls == 1
    assert risk.update_limits_calls == 1
    # No errors, feedback generated
    assert report.connector_errors == 0
    assert report.errors == ()
    assert report.feedback_generated is True


async def test_run_cycle_empty_strategy_skips_risk_and_execute() -> None:
    """AC: empty strategy → risk manager and executor are never touched."""

    connector = MockConnector(markets=[_sample_market()])
    strategy = EmptyStrategy()
    executor = SpyExecutor()
    risk = ApproveAllRisk()
    metrics = MockMetrics()
    feedback_engine = MockFeedbackEngine()

    pipeline = TradingPipeline(
        connectors=[connector],
        strategies=[strategy],
        executor=executor,
        risk_manager=risk,
        metrics=metrics,
        feedback_engine=feedback_engine,
    )

    report = await pipeline.run_cycle()

    assert report.markets_fetched == 1
    # Two outcomes → two price updates → strategy called twice
    assert strategy.on_price_update_calls == 2
    assert report.orders_proposed == 0
    assert report.orders_approved == 0
    assert report.orders_submitted == 0
    assert report.orders_filled == 0
    assert report.feedback_generated is False

    # Crucial: risk and executor never called
    assert risk.check_order_calls == 0
    assert executor.submitted_orders == []
    assert executor.get_positions_calls == 0
    assert metrics.record_order_calls == []
    # Feedback engine is also not invoked when there is nothing to evaluate
    assert feedback_engine.generate_feedback_calls == 0
    assert risk.update_limits_calls == 0


async def test_run_cycle_all_orders_rejected_skips_execute() -> None:
    """AC: risk rejects all orders → executor never called."""

    connector = MockConnector(markets=[_sample_market()])
    strategy = MockStrategy(orders_to_emit=[_sample_order("o-rej")])
    executor = SpyExecutor()
    risk = RejectAllRisk()
    metrics = MockMetrics()
    feedback_engine = MockFeedbackEngine()

    pipeline = TradingPipeline(
        connectors=[connector],
        strategies=[strategy],
        executor=executor,
        risk_manager=risk,
        metrics=metrics,
        feedback_engine=feedback_engine,
    )

    report = await pipeline.run_cycle()

    assert report.orders_proposed == 2  # 1 order × 2 price updates
    assert risk.check_order_calls == 2
    assert report.orders_approved == 0
    assert report.orders_submitted == 0
    assert report.orders_filled == 0
    assert executor.submitted_orders == []
    # Positions fetched exactly once before iterating orders
    assert executor.get_positions_calls == 1
    assert metrics.record_order_calls == []
    assert feedback_engine.generate_feedback_calls == 0
    assert report.feedback_generated is False


async def test_run_cycle_connector_connection_error_is_handled(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """AC: connector ConnectionError is caught, logged, and the cycle completes."""

    broken = BrokenConnector()
    working = MockConnector(markets=[_sample_market("mock", "m-2")])
    strategy = MockStrategy()  # emits nothing
    executor = SpyExecutor()
    risk = ApproveAllRisk()
    metrics = MockMetrics()
    feedback_engine = MockFeedbackEngine()

    pipeline = TradingPipeline(
        connectors=[broken, working],
        strategies=[strategy],
        executor=executor,
        risk_manager=risk,
        metrics=metrics,
        feedback_engine=feedback_engine,
    )

    with caplog.at_level("WARNING", logger="pms.orchestrator.pipeline"):
        report = await pipeline.run_cycle()

    # Cycle completes
    assert isinstance(report, CycleReport)
    # Broken connector error recorded
    assert report.connector_errors == 1
    assert len(report.errors) == 1
    assert "ConnectionError" in report.errors[0]
    assert "broken" in report.errors[0]
    # Working connector still ran and fetched its market
    assert working.get_active_markets_calls == 1
    assert report.markets_fetched == 1
    # Log captured
    assert any(
        "ConnectionError" in record.getMessage() for record in caplog.records
    )


async def test_run_cycle_risk_size_adjustment_is_applied() -> None:
    """Risk manager's ``adjusted_size`` must be honored when the order is submitted."""

    connector = MockConnector(markets=[_sample_market()])
    strategy = MockStrategy(orders_to_emit=[_sample_order("o-adj")])
    executor = SpyExecutor()
    risk = SizeAdjustRisk(new_size=Decimal("3"))
    metrics = MockMetrics()
    feedback_engine = MockFeedbackEngine()

    pipeline = TradingPipeline(
        connectors=[connector],
        strategies=[strategy],
        executor=executor,
        risk_manager=risk,
        metrics=metrics,
        feedback_engine=feedback_engine,
    )

    report = await pipeline.run_cycle()

    assert report.orders_submitted == 2
    # Both submitted orders have the adjusted size
    for submitted in executor.submitted_orders:
        assert submitted.size == Decimal("3")


class _MockCorrelationDetector:
    """Returns a preset list of CorrelationPair from ``detect``."""

    def __init__(
        self,
        pairs: list[CorrelationPair] | None = None,
        raise_on_detect: bool = False,
    ) -> None:
        self._pairs = pairs or []
        self._raise = raise_on_detect
        self.detect_calls = 0

    async def detect(self, markets: list[Market]) -> list[CorrelationPair]:
        self.detect_calls += 1
        if self._raise:
            raise RuntimeError("simulated detector failure")
        return list(self._pairs)


class _CorrelationOrderStrategy:
    """Returns a fixed order from ``on_correlation_found`` (and nothing else)."""

    name = "corr-mock"

    def __init__(self, order: Order) -> None:
        self._order = order
        self.on_correlation_found_calls = 0
        self.on_price_update_calls = 0

    async def on_price_update(
        self, update: PriceUpdate
    ) -> list[Order] | None:
        self.on_price_update_calls += 1
        return None

    async def on_correlation_found(
        self, pair: CorrelationPair
    ) -> list[Order] | None:
        self.on_correlation_found_calls += 1
        return [self._order]

    async def on_feedback(self, feedback: EvaluationFeedback) -> None:
        return None


def _correlation_pair_for_markets() -> CorrelationPair:
    a = _sample_market(platform="polymarket", market_id="m-pm")
    b = _sample_market(platform="kalshi", market_id="m-kalshi")
    return CorrelationPair(
        market_a=a,
        market_b=b,
        similarity_score=0.9,
        relation_type="overlapping",
        relation_detail="",
        arbitrage_opportunity=None,
    )


async def test_pipeline_invokes_correlation_detector_when_configured() -> None:
    """Review-loop fix f2 (round 2): the pipeline must run the
    CorrelationDetector after sense and dispatch each pair to every
    strategy via ``on_correlation_found``. Resulting orders must reach
    the executor and be attributed to the emitting strategy.
    """
    connector = MockConnector(markets=[_sample_market()])
    corr_pair = _correlation_pair_for_markets()
    detector = _MockCorrelationDetector(pairs=[corr_pair])
    corr_order = Order(
        order_id="corr-o-1",
        platform="kalshi",
        market_id="m-kalshi",
        outcome_id="yes",
        side="buy",
        price=Decimal("0.40"),
        size=Decimal("5"),
        order_type="limit",
    )
    strategy = _CorrelationOrderStrategy(order=corr_order)
    executor = SpyExecutor()
    risk = ApproveAllRisk()
    metrics = MetricsCollector()
    feedback_engine = MockFeedbackEngine()

    pipeline = TradingPipeline(
        connectors=[connector],
        strategies=[strategy],
        executor=executor,
        risk_manager=risk,
        metrics=metrics,
        feedback_engine=feedback_engine,
        correlation_detector=detector,
    )

    report = await pipeline.run_cycle()

    assert detector.detect_calls == 1
    assert strategy.on_correlation_found_calls == 1
    # Order from on_correlation_found made it through risk + execute.
    assert any(
        o.order_id == "corr-o-1" for o in executor.submitted_orders
    )
    assert report.orders_proposed >= 1
    assert report.orders_submitted >= 1
    # Strategy attribution must reach the metrics collector.
    perf = metrics.get_performance_metrics()
    assert "corr-mock" in perf.per_strategy
    assert "unknown" not in perf.per_strategy


async def test_pipeline_without_correlation_detector_still_works() -> None:
    """Existing pipelines (no detector configured) must keep working unchanged."""
    connector = MockConnector(markets=[_sample_market()])
    strategy = MockStrategy(orders_to_emit=[_sample_order("o-no-detector")])
    executor = SpyExecutor()
    risk = ApproveAllRisk()
    metrics = MockMetrics()
    feedback_engine = MockFeedbackEngine()

    pipeline = TradingPipeline(
        connectors=[connector],
        strategies=[strategy],
        executor=executor,
        risk_manager=risk,
        metrics=metrics,
        feedback_engine=feedback_engine,
        # correlation_detector intentionally omitted
    )

    report = await pipeline.run_cycle()

    assert report.orders_submitted == 2  # 2 outcomes -> 2 orders
    assert report.errors == ()


async def test_correlation_detector_failure_does_not_crash_cycle() -> None:
    """A detector that raises must be caught and recorded, not propagated."""
    connector = MockConnector(markets=[_sample_market()])
    detector = _MockCorrelationDetector(raise_on_detect=True)
    strategy = MockStrategy()  # emits nothing
    executor = SpyExecutor()
    risk = ApproveAllRisk()
    metrics = MockMetrics()
    feedback_engine = MockFeedbackEngine()

    pipeline = TradingPipeline(
        connectors=[connector],
        strategies=[strategy],
        executor=executor,
        risk_manager=risk,
        metrics=metrics,
        feedback_engine=feedback_engine,
        correlation_detector=detector,
    )

    report = await pipeline.run_cycle()

    assert isinstance(report, CycleReport)
    assert detector.detect_calls == 1
    assert any("CorrelationDetector" in e for e in report.errors)


async def test_correlation_detector_strategy_failure_is_isolated() -> None:
    """A strategy that raises in ``on_correlation_found`` must not crash
    the cycle — it should be recorded in ``errors`` and the cycle should
    continue (no orders, but no crash)."""

    class _RaisingCorrStrategy:
        name = "raises"

        async def on_price_update(
            self, update: PriceUpdate
        ) -> list[Order] | None:
            return None

        async def on_correlation_found(
            self, pair: CorrelationPair
        ) -> list[Order] | None:
            raise RuntimeError("kaboom")

        async def on_feedback(self, feedback: EvaluationFeedback) -> None:
            return None

    connector = MockConnector(markets=[_sample_market()])
    detector = _MockCorrelationDetector(pairs=[_correlation_pair_for_markets()])
    strategy = _RaisingCorrStrategy()
    executor = SpyExecutor()
    risk = ApproveAllRisk()
    metrics = MockMetrics()
    feedback_engine = MockFeedbackEngine()

    pipeline = TradingPipeline(
        connectors=[connector],
        strategies=[strategy],  # type: ignore[list-item]
        executor=executor,
        risk_manager=risk,
        metrics=metrics,
        feedback_engine=feedback_engine,
        correlation_detector=detector,
    )

    report = await pipeline.run_cycle()
    assert any("on_correlation_found" in e for e in report.errors)
    # No orders submitted because the strategy raised before producing any.
    assert report.orders_submitted == 0


async def test_run_cycle_stamps_strategy_attribution_on_metrics() -> None:
    """The pipeline must tag every recorded order with the emitting strategy.

    Without attribution, ``MetricsCollector._compute_strategy_metrics``
    buckets every order under ``"unknown"`` (because ``result.raw`` does
    not carry a ``"strategy"`` key), which silently severs the feedback
    loop — ``FeedbackEngine`` emits ``strategy_adjustments["unknown"]``
    but ``ArbitrageStrategy.on_feedback`` reads ``.get("arbitrage")``.

    This test drives the fix: pipeline stamps ``result.raw["strategy"]``
    with the name of the strategy that emitted the order before
    recording the order on the metrics collector. We use the REAL
    ``MetricsCollector`` (not the in-test ``MockMetrics``) because the
    bug is in the metrics strategy-attribution path and we want the
    integration to fail loudly if the pipeline regresses.
    """

    connector = MockConnector(markets=[_sample_market()])
    strategy = MockStrategy(orders_to_emit=[_sample_order("o-attr")])
    executor = SpyExecutor()
    risk = ApproveAllRisk()
    metrics = MetricsCollector()
    feedback_engine = MockFeedbackEngine()

    pipeline = TradingPipeline(
        connectors=[connector],
        strategies=[strategy],
        executor=executor,
        risk_manager=risk,
        metrics=metrics,
        feedback_engine=feedback_engine,
    )

    report = await pipeline.run_cycle()

    # Sanity: cycle ran and at least one order was recorded.
    assert report.orders_submitted >= 1

    performance = metrics.get_performance_metrics()
    # Every recorded order must live under the strategy's ``name`` bucket,
    # not the ``"unknown"`` fallback.
    assert "mock" in performance.per_strategy
    assert "unknown" not in performance.per_strategy
    assert performance.per_strategy["mock"].num_orders == report.orders_submitted


# ---------------------------------------------------------------------------
# Config loader tests
# ---------------------------------------------------------------------------


def test_load_config_parses_yaml_into_pipeline_config(
    tmp_path: Path,
) -> None:
    """AC: a YAML file resolves to ``PipelineConfig`` with ``ModuleSpec`` fields."""

    cfg_yaml = """\
connectors:
  - class: pms.connectors.polymarket.PolymarketConnector
    kwargs:
      base_url: https://example.com
  - class: pms.connectors.kalshi.KalshiConnector
    kwargs: {}
strategies: []
executor:
  class: tests.test_pipeline.SpyExecutor
  kwargs: {}
risk_manager:
  class: tests.test_pipeline.ApproveAllRisk
  kwargs: {}
metrics:
  class: tests.test_pipeline.MockMetrics
  kwargs: {}
feedback_engine:
  class: tests.test_pipeline.MockFeedbackEngine
  kwargs: {}
"""
    path = tmp_path / "config.yaml"
    path.write_text(cfg_yaml, encoding="utf-8")

    config = load_config(path)

    assert isinstance(config, PipelineConfig)
    assert len(config.connectors) == 2
    assert (
        config.connectors[0].class_path
        == "pms.connectors.polymarket.PolymarketConnector"
    )
    assert config.connectors[0].kwargs == {"base_url": "https://example.com"}
    assert (
        config.connectors[1].class_path
        == "pms.connectors.kalshi.KalshiConnector"
    )
    assert config.connectors[1].kwargs == {}
    assert config.strategies == []
    assert config.executor.class_path == "tests.test_pipeline.SpyExecutor"
    assert (
        config.risk_manager.class_path == "tests.test_pipeline.ApproveAllRisk"
    )
    assert config.metrics.class_path == "tests.test_pipeline.MockMetrics"
    assert (
        config.feedback_engine.class_path
        == "tests.test_pipeline.MockFeedbackEngine"
    )


def test_load_config_defaults_empty_kwargs(tmp_path: Path) -> None:
    cfg_yaml = """\
connectors: []
strategies: []
executor:
  class: tests.test_pipeline.SpyExecutor
risk_manager:
  class: tests.test_pipeline.ApproveAllRisk
metrics:
  class: tests.test_pipeline.MockMetrics
feedback_engine:
  class: tests.test_pipeline.MockFeedbackEngine
"""
    path = tmp_path / "config.yaml"
    path.write_text(cfg_yaml, encoding="utf-8")

    config = load_config(path)

    assert config.executor.kwargs == {}
    assert config.risk_manager.kwargs == {}


# ---------------------------------------------------------------------------
# ModuleRegistry tests
# ---------------------------------------------------------------------------


def test_module_registry_instantiates_class_by_path() -> None:
    """AC: ModuleRegistry resolves a dotted class path to a fresh instance.

    The target lives in ``tests/_registry_target.py`` (not inside this
    test file) so the registry resolution and the pytest import land on
    the same cached module. See that file's docstring for the pytest-
    rootdir-import rationale.
    """
    registry = ModuleRegistry()
    spec = ModuleSpec(
        class_path="tests._registry_target.FakeRegistryTarget",
        kwargs={"x": 42, "label": "custom"},
    )

    instance = registry.instantiate(spec)

    assert isinstance(instance, FakeRegistryTarget)
    assert instance.x == 42
    assert instance.label == "custom"


def test_module_registry_raises_on_missing_module() -> None:
    registry = ModuleRegistry()
    spec = ModuleSpec(
        class_path="pms.does_not_exist.NopeClass", kwargs={}
    )

    with pytest.raises(ImportError):
        registry.instantiate(spec)


def test_module_registry_raises_on_missing_class() -> None:
    registry = ModuleRegistry()
    spec = ModuleSpec(
        class_path="tests._registry_target.NoSuchClassAtAll", kwargs={}
    )

    with pytest.raises(AttributeError):
        registry.instantiate(spec)


def test_module_registry_raises_on_bare_class_name() -> None:
    registry = ModuleRegistry()
    spec = ModuleSpec(class_path="NoModuleHere", kwargs={})

    with pytest.raises(ValueError):
        registry.instantiate(spec)
