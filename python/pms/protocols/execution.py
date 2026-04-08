"""Execution-layer protocols — order executor and risk manager."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from pms.models import (
    EvaluationFeedback,
    Order,
    OrderResult,
    Position,
    RiskDecision,
)


@runtime_checkable
class ExecutorProtocol(Protocol):
    """Routes orders to the correct platform connector and tracks positions."""

    async def submit_order(self, order: Order) -> OrderResult:
        """Submit ``order`` to its target platform and return the result."""
        ...

    async def cancel_order(self, order_id: str) -> bool:
        """Cancel an in-flight order; return True on success."""
        ...

    async def get_positions(self) -> list[Position]:
        """Return the executor's current positions across all platforms."""
        ...


@runtime_checkable
class RiskManagerProtocol(Protocol):
    """Pre-trade risk gate with feedback-driven limit adjustments."""

    def check_order(
        self, order: Order, positions: list[Position]
    ) -> RiskDecision:
        """Approve, reject, or size-adjust ``order`` given current positions."""
        ...

    def update_limits(self, feedback: EvaluationFeedback) -> None:
        """Apply guardrail-bounded adjustments derived from feedback."""
        ...
