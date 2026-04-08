"""Execution layer — risk management and order routing (CP08).

This package houses the concrete implementations of
:class:`~pms.protocols.execution.RiskManagerProtocol` and
:class:`~pms.protocols.execution.ExecutorProtocol`. Sub-modules are exposed
here so callers can ``from pms.execution import RiskManager, OrderExecutor``.
"""

from .executor import OrderExecutor
from .guardrails import GUARDRAILS, GuardrailBounds, apply_guardrail
from .risk import RiskManager

__all__ = [
    "GUARDRAILS",
    "GuardrailBounds",
    "OrderExecutor",
    "RiskManager",
    "apply_guardrail",
]
