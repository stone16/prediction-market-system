"""StrategyProtocol — pluggable trading strategy interface."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from pms.models import CorrelationPair, EvaluationFeedback, Order, PriceUpdate


@runtime_checkable
class StrategyProtocol(Protocol):
    """A trading strategy that reacts to price updates and correlation events."""

    name: str

    async def on_price_update(self, update: PriceUpdate) -> list[Order] | None:
        """Handle a price update; return any orders to submit, or ``None``."""
        ...

    async def on_correlation_found(
        self, pair: CorrelationPair
    ) -> list[Order] | None:
        """Handle a newly detected correlation; return any orders to submit."""
        ...

    async def on_feedback(self, feedback: EvaluationFeedback) -> None:
        """Apply tuning feedback from the evaluation layer."""
        ...
