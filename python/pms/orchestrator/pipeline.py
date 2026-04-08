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
    Position,
    PriceUpdate,
)
from pms.protocols import (
    ConnectorProtocol,
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
    ) -> None:
        self._connectors: list[ConnectorProtocol] = list(connectors)
        self._strategies: list[StrategyProtocol] = list(strategies)
        self._executor = executor
        self._risk_manager = risk_manager
        self._metrics = metrics
        self._feedback_engine = feedback_engine

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

        # 3. STRATEGY
        proposed_orders, strategy_errors = await self._run_strategies(
            self._strategies, price_updates
        )
        errors.extend(strategy_errors)

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
            approved_orders
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

    async def _run_strategies(
        self,
        strategies: Sequence[StrategyProtocol],
        price_updates: list[PriceUpdate],
    ) -> tuple[list[Order], list[str]]:
        """Feed every price update to every strategy; collect proposed orders."""
        proposed: list[Order] = []
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

        return proposed, errors

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
        self, approved_orders: list[Order]
    ) -> tuple[int, int, list[str]]:
        """Submit approved orders to the executor and record metrics."""
        submitted = 0
        filled = 0
        errors: list[str] = []

        for order in approved_orders:
            try:
                result = await self._executor.submit_order(order)
            except Exception as exc:  # noqa: BLE001 — pipeline contract
                message = (
                    f"Executor failed on order {order.order_id}: {exc}"
                )
                logger.exception(message)
                errors.append(message)
                continue

            submitted += 1
            if result.status == "filled":
                filled += 1

            try:
                await self._metrics.record_order(order, result)
            except Exception as exc:  # noqa: BLE001 — pipeline contract
                message = (
                    f"Metrics.record_order failed for {order.order_id}: "
                    f"{exc}"
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
