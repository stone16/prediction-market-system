"""TradingPipeline — the main sense → strategy → risk → execute → evaluate → feedback loop.

The pipeline is deliberately boring: it iterates over injected module
instances, forwards events between them, and records a
:class:`CycleReport` describing what happened. It does not own any
business logic — strategy behavior, risk rules, execution semantics,
metric formulas, and feedback generation all live behind the respective
Protocol implementations injected at construction time.

Error-handling contract
-----------------------

The pipeline treats every module call as potentially fallible. Any
exception raised by a module is **caught, logged, and recorded** in the
returned ``CycleReport.errors`` tuple. A cycle always runs to completion
— a single bad connector, strategy, or risk check must not abort the
rest of the cycle or crash the process.

The only exception to this rule is the pipeline's own programming
errors (e.g. a malformed Protocol implementation that violates the
method signatures). Those should bubble up to the test suite so they are
not silently masked.

The CP06 scope is deliberately narrow:

- Market data is fetched once per cycle from every connector.
- Each market's known last-tick prices are synthesized into a
  ``PriceUpdate`` per outcome; real streaming/tick data is a CP07/CP08
  concern.
- A cycle ends with a single feedback dispatch — the feedback loop runs
  in lock-step with the sensing loop for CP06. Decoupling them into
  separate cadences is a CP09 concern.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from decimal import Decimal

from pms.models import (
    Market,
    Order,
    OrderResult,
    Position,
    PriceUpdate,
)
from pms.protocols import (
    ConnectorProtocol,
    CorrelationDetectorProtocol,
    ExecutorProtocol,
    FeedbackEngineProtocol,
    MetricsCollectorProtocol,
    RiskManagerProtocol,
    StrategyProtocol,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CycleReport:
    """Immutable summary of a single :meth:`TradingPipeline.run_cycle`.

    Every field is a count or flag so callers can log, assert, or
    aggregate the report without holding references to the mutable
    domain objects the cycle operated on.
    """

    markets_fetched: int
    price_updates_generated: int
    orders_proposed: int
    orders_approved: int
    orders_submitted: int
    orders_filled: int
    connector_errors: int
    errors: tuple[str, ...]
    feedback_generated: bool


class TradingPipeline:
    """Main trading loop wiring Protocol implementations together.

    The pipeline accepts every pluggable module through the constructor
    so it is trivial to unit-test with in-memory fakes and to reconfigure
    for different platforms/strategies via YAML (see
    :func:`pms.orchestrator.config.load_config`).
    """

    def __init__(
        self,
        connectors: Sequence[ConnectorProtocol],
        strategies: Sequence[StrategyProtocol],
        executor: ExecutorProtocol,
        risk_manager: RiskManagerProtocol,
        metrics: MetricsCollectorProtocol,
        feedback_engine: FeedbackEngineProtocol,
        correlation_detector: CorrelationDetectorProtocol | None = None,
    ) -> None:
        self._connectors: list[ConnectorProtocol] = list(connectors)
        self._strategies: list[StrategyProtocol] = list(strategies)
        self._executor = executor
        self._risk_manager = risk_manager
        self._metrics = metrics
        self._feedback_engine = feedback_engine
        # Optional CP10 correlation detector. When set, the pipeline runs
        # detection after sense and dispatches each ``CorrelationPair``
        # to every strategy via ``on_correlation_found`` (review-loop fix
        # f2 round 2). When None, behaviour is identical to the
        # detector-less pipeline.
        self._correlation_detector: CorrelationDetectorProtocol | None = (
            correlation_detector
        )

    # ------------------------------------------------------------------
    # Cycle entry point
    # ------------------------------------------------------------------

    async def run_cycle(self) -> CycleReport:
        """Execute one full sense → strategy → risk → execute → feedback loop.

        Returns a :class:`CycleReport` summarizing the stages reached.
        The cycle exits early — but still returns a report — at two
        places:

        1. If no strategy emitted any orders, risk and execute are
           skipped (and so is feedback generation).
        2. If the risk manager rejected every proposed order, the
           executor is skipped (and so is feedback generation).

        All non-fatal errors are collected into ``CycleReport.errors``.
        """
        errors: list[str] = []

        # 1. SENSE
        markets, connector_errors, sense_errors = await self._sense(
            self._connectors
        )
        errors.extend(sense_errors)

        # 2. Synthesize price updates from fetched markets
        price_updates = self._synthesize_price_updates(markets)

        # 2.5. CORRELATION DETECTION (optional, review-loop fix f2 r2)
        # When a CorrelationDetector is wired, run it on the fetched
        # markets and dispatch each pair to every strategy. The orders
        # returned from ``on_correlation_found`` are merged into the
        # proposed-orders pool below. Detector failures and strategy
        # failures are isolated like the rest of the pipeline.
        (
            correlation_orders,
            correlation_strategy_map,
            correlation_errors,
        ) = await self._run_correlation_detection(markets)
        errors.extend(correlation_errors)

        # 3. STRATEGY
        (
            proposed_orders,
            order_to_strategy,
            strategy_errors,
        ) = await self._run_strategies(self._strategies, price_updates)
        errors.extend(strategy_errors)

        # Merge correlation-derived orders into the price-update pool.
        # Correlation orders are appended (not pre-pended) so the existing
        # per-update emission order is preserved for the legacy tests.
        proposed_orders.extend(correlation_orders)
        order_to_strategy.update(correlation_strategy_map)

        if not proposed_orders:
            logger.info(
                "Pipeline cycle: no orders proposed, skipping risk/execute"
            )
            return CycleReport(
                markets_fetched=len(markets),
                price_updates_generated=len(price_updates),
                orders_proposed=0,
                orders_approved=0,
                orders_submitted=0,
                orders_filled=0,
                connector_errors=connector_errors,
                errors=tuple(errors),
                feedback_generated=False,
            )

        # 4. RISK
        positions, positions_error = await self._load_positions()
        if positions_error is not None:
            errors.append(positions_error)

        approved_orders, risk_errors = self._run_risk(
            proposed_orders, positions
        )
        errors.extend(risk_errors)

        if not approved_orders:
            logger.info(
                "Pipeline cycle: all orders rejected, skipping executor"
            )
            return CycleReport(
                markets_fetched=len(markets),
                price_updates_generated=len(price_updates),
                orders_proposed=len(proposed_orders),
                orders_approved=0,
                orders_submitted=0,
                orders_filled=0,
                connector_errors=connector_errors,
                errors=tuple(errors),
                feedback_generated=False,
            )

        # 5. EXECUTE
        submitted_count, filled_count, execute_errors = await self._execute(
            approved_orders, order_to_strategy
        )
        errors.extend(execute_errors)

        # 6. EVALUATE + FEEDBACK
        feedback_generated, feedback_errors = await self._dispatch_feedback()
        errors.extend(feedback_errors)

        return CycleReport(
            markets_fetched=len(markets),
            price_updates_generated=len(price_updates),
            orders_proposed=len(proposed_orders),
            orders_approved=len(approved_orders),
            orders_submitted=submitted_count,
            orders_filled=filled_count,
            connector_errors=connector_errors,
            errors=tuple(errors),
            feedback_generated=feedback_generated,
        )

    # ------------------------------------------------------------------
    # Private helpers — one per pipeline stage
    # ------------------------------------------------------------------

    async def _sense(
        self, connectors: Sequence[ConnectorProtocol]
    ) -> tuple[list[Market], int, list[str]]:
        """Fetch active markets from every connector.

        Returns the union of markets, a count of connector failures,
        and the list of error messages to record on the cycle report.
        """
        all_markets: list[Market] = []
        errors: list[str] = []
        connector_errors = 0

        for connector in connectors:
            try:
                markets = await connector.get_active_markets()
                all_markets.extend(markets)
            except ConnectionError as exc:
                connector_errors += 1
                message = (
                    f"Connector {connector.platform} ConnectionError: {exc}"
                )
                logger.warning(message)
                errors.append(message)
            except Exception as exc:  # noqa: BLE001 — pipeline contract
                connector_errors += 1
                message = (
                    f"Connector {connector.platform} unexpected error: {exc}"
                )
                logger.exception(message)
                errors.append(message)

        return all_markets, connector_errors, errors

    def _synthesize_price_updates(
        self, markets: list[Market]
    ) -> list[PriceUpdate]:
        """Derive a ``PriceUpdate`` per outcome from each market's last prices.

        The pipeline needs at least one event per market to drive the
        strategies. In a production pipeline those events would come
        from ``stream_prices()`` or a fresh order-book snapshot; for
        CP06's scope we synthesize them from the outcomes the connector
        already returned so the strategy layer has something to react
        to. The synthesized update uses the outcome's current price for
        ``bid``, ``ask``, and ``last`` because we do not yet have bid/
        ask telemetry on the ``Outcome`` model — this is a deliberate
        simplification that can be refined when a real order book is
        threaded through the pipeline.
        """
        updates: list[PriceUpdate] = []
        now = datetime.now(timezone.utc)
        for market in markets:
            for outcome in market.outcomes:
                price: Decimal = outcome.price
                updates.append(
                    PriceUpdate(
                        platform=market.platform,
                        market_id=market.market_id,
                        outcome_id=outcome.outcome_id,
                        bid=price,
                        ask=price,
                        last=price,
                        timestamp=now,
                    )
                )
        return updates

    async def _run_correlation_detection(
        self, markets: list[Market]
    ) -> tuple[list[Order], dict[str, str], list[str]]:
        """Detect correlations and dispatch them to every strategy.

        Review-loop fix f2 round 2: the CP10 ``CorrelationDetector`` was
        previously unwired, leaving ``ArbitrageStrategy.on_correlation_found``
        unreachable in production (the only path that fired in tests was
        the cross-platform ``outcome_id`` equality check, which never
        matches Polymarket vs. Kalshi IDs). Wiring the detector here is
        the production seam.

        Returns the union of orders emitted by every strategy, a
        ``order_id -> strategy.name`` map for attribution, and any
        captured error messages.
        """
        if self._correlation_detector is None:
            return [], {}, []

        errors: list[str] = []

        try:
            pairs = await self._correlation_detector.detect(markets)
        except Exception as exc:  # noqa: BLE001 — pipeline contract
            message = f"CorrelationDetector failed: {exc}"
            logger.exception(message)
            return [], {}, [message]

        produced: list[Order] = []
        attribution: dict[str, str] = {}

        for pair in pairs:
            for strategy in self._strategies:
                try:
                    orders = await strategy.on_correlation_found(pair)
                except Exception as exc:  # noqa: BLE001 — pipeline contract
                    message = (
                        f"Strategy {strategy.name} on_correlation_found "
                        f"failed: {exc}"
                    )
                    logger.exception(message)
                    errors.append(message)
                    continue

                if not orders:
                    continue

                for order in orders:
                    produced.append(order)
                    attribution[order.order_id] = strategy.name

        return produced, attribution, errors

    async def _run_strategies(
        self,
        strategies: Sequence[StrategyProtocol],
        price_updates: list[PriceUpdate],
    ) -> tuple[list[Order], dict[str, str], list[str]]:
        """Feed every price update to every strategy; collect proposed orders.

        Also returns a ``order_id -> strategy.name`` map so the execute
        stage can stamp strategy attribution onto the ``OrderResult.raw``
        dict before recording the result on the metrics collector. This
        is the only place where the pipeline has first-hand knowledge of
        which strategy emitted which order — ``Order`` itself has no
        strategy field (and adding one would spill into every connector's
        order payload).
        """
        proposed: list[Order] = []
        order_to_strategy: dict[str, str] = {}
        errors: list[str] = []

        for update in price_updates:
            for strategy in strategies:
                try:
                    result = await strategy.on_price_update(update)
                except Exception as exc:  # noqa: BLE001 — pipeline contract
                    message = (
                        f"Strategy {strategy.name} failed on price update: "
                        f"{exc}"
                    )
                    logger.exception(message)
                    errors.append(message)
                    continue

                if result:
                    proposed.extend(result)
                    for order in result:
                        order_to_strategy[order.order_id] = strategy.name

        return proposed, order_to_strategy, errors

    async def _load_positions(self) -> tuple[list[Position], str | None]:
        """Fetch current positions from the executor for risk evaluation."""
        try:
            positions = await self._executor.get_positions()
        except Exception as exc:  # noqa: BLE001 — pipeline contract
            message = f"Executor.get_positions failed: {exc}"
            logger.exception(message)
            return [], message
        return list(positions), None

    def _run_risk(
        self, proposed_orders: list[Order], positions: list[Position]
    ) -> tuple[list[Order], list[str]]:
        """Run every proposed order through the risk manager."""
        approved: list[Order] = []
        errors: list[str] = []

        for order in proposed_orders:
            try:
                decision = self._risk_manager.check_order(order, positions)
            except Exception as exc:  # noqa: BLE001 — pipeline contract
                message = (
                    f"Risk check failed for order {order.order_id}: {exc}"
                )
                logger.exception(message)
                errors.append(message)
                continue

            if not decision.approved:
                logger.info(
                    "Order %s rejected by risk manager: %s",
                    order.order_id,
                    decision.reason,
                )
                continue

            if decision.adjusted_size is not None:
                approved.append(
                    replace(order, size=decision.adjusted_size)
                )
            else:
                approved.append(order)

        return approved, errors

    async def _execute(
        self,
        approved_orders: list[Order],
        order_to_strategy: dict[str, str],
    ) -> tuple[int, int, list[str]]:
        """Submit approved orders to the executor and record metrics.

        Before recording an order on the metrics collector, the result's
        ``raw`` payload is augmented with a ``"strategy"`` key pointing
        to the emitting strategy's ``name``. This is what allows
        :meth:`MetricsCollector.get_performance_metrics` to bucket
        orders per strategy — without it, every order falls into the
        ``"unknown"`` bucket and the feedback loop silently breaks
        (``ArbitrageStrategy.on_feedback`` reads
        ``strategy_adjustments["arbitrage"]``, which never exists).

        If the risk manager rewrote the order via
        :class:`dataclasses.replace`, the ``order_id`` is preserved, so
        the attribution lookup still succeeds. Orders with no known
        origin fall through to ``"unknown"`` for backward compatibility.

        Review-loop fix f13 (opportunity lifecycle cleanup)
        ---------------------------------------------------

        After ``metrics.record_order`` succeeds (or fails — the lifecycle
        is terminal either way), the pipeline notifies the producing
        strategy via ``clear_opportunity(order_id)`` if the strategy
        exposes that method. This is the seam that lets
        ``ArbitrageStrategy`` release its outstanding-opportunity tracker
        so the same logical opportunity can be re-emitted on a later
        cycle. Without this hook, once an opportunity is emitted it is
        suppressed forever (the outstanding-opportunity set never
        shrinks). ``clear_opportunity`` is intentionally duck-typed with
        ``hasattr`` because ``StrategyProtocol`` does not (yet) require
        it; strategies that do not implement it keep their old behaviour.
        Exceptions from ``clear_opportunity`` are recorded in ``errors``
        so a buggy strategy cannot abort the cycle.

        Review-loop fix f14 (round 4 — exception path cleanup)
        ------------------------------------------------------

        ``clear_opportunity`` MUST also fire when ``submit_order`` raises.
        Previously the except branch ``continue``d before the cleanup
        block, so a flaky executor would leak the opportunity key
        forever — every subsequent cycle would silently suppress the same
        logical opportunity because the strategy still believed it was
        in flight. The submission loop now wraps the per-order work in a
        ``try/finally`` so cleanup runs on both the success and failure
        paths.
        """
        submitted = 0
        filled = 0
        errors: list[str] = []

        # O(1) strategy lookup by name so every order that makes it
        # through the execute phase can find its producing strategy
        # instance without re-scanning ``self._strategies``.
        strategies_by_name: dict[str, StrategyProtocol] = {
            strategy.name: strategy for strategy in self._strategies
        }

        for order in approved_orders:
            try:
                try:
                    result = await self._executor.submit_order(order)
                except Exception as exc:  # noqa: BLE001 — pipeline contract
                    message = (
                        f"Executor failed on order {order.order_id}: {exc}"
                    )
                    logger.exception(message)
                    errors.append(message)
                    # Skip the success-path bookkeeping but DO NOT skip
                    # the ``finally`` cleanup below — the opportunity
                    # lifecycle is terminal regardless of submit outcome.
                    continue

                submitted += 1
                if result.status == "filled":
                    filled += 1

                strategy_name = order_to_strategy.get(
                    order.order_id, "unknown"
                )
                stamped_result = OrderResult(
                    order_id=result.order_id,
                    status=result.status,
                    filled_size=result.filled_size,
                    filled_price=result.filled_price,
                    message=result.message,
                    raw={**result.raw, "strategy": strategy_name},
                )

                try:
                    await self._metrics.record_order(order, stamped_result)
                except Exception as exc:  # noqa: BLE001 — pipeline contract
                    message = (
                        f"Metrics.record_order failed for {order.order_id}: "
                        f"{exc}"
                    )
                    logger.exception(message)
                    errors.append(message)
            finally:
                # Release the producing strategy's opportunity tracker
                # for this order. The lifecycle is terminal at this
                # point — every terminal status (filled, partial,
                # rejected, error, AND executor exception) releases the
                # opportunity so a later cycle can retry if appropriate.
                # Strategies that do not implement the optional method
                # are skipped silently. The lookup uses the attribution
                # map directly (rather than the in-loop ``strategy_name``
                # variable) so the cleanup still runs even when control
                # left the try block via the executor-failure ``continue``
                # before ``strategy_name`` was assigned.
                attributed_name = order_to_strategy.get(order.order_id)
                if attributed_name is not None:
                    producing_strategy = strategies_by_name.get(
                        attributed_name
                    )
                    if producing_strategy is not None and hasattr(
                        producing_strategy, "clear_opportunity"
                    ):
                        try:
                            producing_strategy.clear_opportunity(
                                order.order_id
                            )
                        except Exception as exc:  # noqa: BLE001 — pipeline contract
                            message = (
                                f"Strategy {attributed_name} "
                                f"clear_opportunity failed for "
                                f"{order.order_id}: {exc}"
                            )
                            logger.exception(message)
                            errors.append(message)

        return submitted, filled, errors

    async def _dispatch_feedback(self) -> tuple[bool, list[str]]:
        """Generate feedback, dispatch to strategies, and update risk limits."""
        errors: list[str] = []

        try:
            performance = self._metrics.get_performance_metrics()
            feedback = self._feedback_engine.generate_feedback(performance)
        except Exception as exc:  # noqa: BLE001 — pipeline contract
            message = f"Feedback generation failed: {exc}"
            logger.exception(message)
            errors.append(message)
            return False, errors

        for strategy in self._strategies:
            try:
                await strategy.on_feedback(feedback)
            except Exception as exc:  # noqa: BLE001 — pipeline contract
                message = (
                    f"Strategy {strategy.name} on_feedback failed: {exc}"
                )
                logger.exception(message)
                errors.append(message)

        try:
            self._risk_manager.update_limits(feedback)
        except Exception as exc:  # noqa: BLE001 — pipeline contract
            message = f"RiskManager.update_limits failed: {exc}"
            logger.exception(message)
            errors.append(message)

        return True, errors
