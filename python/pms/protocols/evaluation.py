"""Evaluation-layer protocols — metrics collection and feedback generation."""

from __future__ import annotations

from datetime import datetime
from typing import Protocol, runtime_checkable

from pms.models import (
    EvaluationFeedback,
    Order,
    OrderResult,
    PerformanceReport,
    PnLReport,
    PriceUpdate,
)


@runtime_checkable
class MetricsCollectorProtocol(Protocol):
    """Records orders, results, and price snapshots; computes P&L and metrics."""

    async def record_order(self, order: Order, result: OrderResult) -> None:
        """Record a submitted order and its execution result."""
        ...

    async def record_price_snapshot(self, updates: list[PriceUpdate]) -> None:
        """Record a batch of price updates for later P&L computation."""
        ...

    def get_pnl(self, since: datetime) -> PnLReport:
        """Return realized + unrealized P&L since ``since``."""
        ...

    def get_performance_metrics(self) -> PerformanceReport:
        """Return per-strategy performance metrics."""
        ...


@runtime_checkable
class FeedbackEngineProtocol(Protocol):
    """Generates feedback packets from performance metrics."""

    def generate_feedback(self, metrics: PerformanceReport) -> EvaluationFeedback:
        """Produce a feedback packet bounded by guardrails."""
        ...
