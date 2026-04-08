"""RiskManager — pre-trade gate with guardrail-bounded limit updates (CP08).

This module implements :class:`~pms.protocols.execution.RiskManagerProtocol`.
It enforces two independent caps on every candidate order:

1. A **per-market** notional cap (``max_position_per_market``), which may
   approve the order with a smaller ``adjusted_size`` if partial room
   remains.
2. A **total exposure** notional cap (``max_total_exposure``), which is a
   hard reject when breached.

``update_limits`` reacts to :class:`pms.models.EvaluationFeedback` from the
feedback engine: drawdown hits tighten limits, ``"relax"`` suggestions
loosen them. Every mutation flows through :func:`apply_guardrail` so the
floor/ceiling bounds defined in :data:`GUARDRAILS` are never crossed.
"""

from __future__ import annotations

from collections.abc import Sequence
from decimal import Decimal

from pms.execution.guardrails import apply_guardrail
from pms.models import EvaluationFeedback, Order, Position, RiskDecision


class RiskManager:
    """Enforces position + exposure caps with feedback-driven tuning.

    Defaults are chosen to sit well inside the guardrail bounds so a bare
    ``RiskManager()`` is always safe to instantiate.
    """

    def __init__(
        self,
        max_position_per_market: Decimal = Decimal("500"),
        max_total_exposure: Decimal = Decimal("5000"),
        max_drawdown_pct: Decimal = Decimal("0.20"),
    ) -> None:
        self._max_position_per_market = apply_guardrail(
            "max_position_per_market", max_position_per_market
        )
        self._max_total_exposure = apply_guardrail(
            "max_total_exposure", max_total_exposure
        )
        self._max_drawdown_pct = apply_guardrail(
            "max_drawdown_pct", max_drawdown_pct
        )

    # ------------------------------------------------------------------
    # Pre-trade gate
    # ------------------------------------------------------------------
    def check_order(
        self, order: Order, positions: Sequence[Position]
    ) -> RiskDecision:
        """Approve, reject, or size-adjust ``order`` given current positions."""
        order_notional = order.price * order.size

        # Rule 1 — per-market cap, keyed on (platform, market_id).
        market_key = (order.platform, order.market_id)
        current_market_notional = sum(
            (
                p.size * p.avg_entry_price
                for p in positions
                if (p.platform, p.market_id) == market_key
            ),
            start=Decimal("0"),
        )
        if current_market_notional + order_notional > self._max_position_per_market:
            remaining = self._max_position_per_market - current_market_notional
            if remaining <= Decimal("0"):
                return RiskDecision(
                    approved=False,
                    reason=(
                        f"Market position cap {self._max_position_per_market} "
                        "reached"
                    ),
                    adjusted_size=None,
                )
            # Partial room against the per-market cap: offer a reduced size
            # that exactly fits.
            adjusted_size = remaining / order.price

            # Re-validate the reduced order against the total exposure cap.
            # Otherwise a per-market shrink could still push total notional
            # over ``max_total_exposure`` (CP08 iter-2 fix).
            adjusted_notional = adjusted_size * order.price
            current_total_notional = sum(
                (p.size * p.avg_entry_price for p in positions),
                start=Decimal("0"),
            )
            if (
                current_total_notional + adjusted_notional
                > self._max_total_exposure
            ):
                exposure_room = (
                    self._max_total_exposure - current_total_notional
                )
                if exposure_room <= Decimal("0"):
                    return RiskDecision(
                        approved=False,
                        reason=(
                            f"Total exposure cap {self._max_total_exposure} "
                            "reached"
                        ),
                        adjusted_size=None,
                    )
                adjusted_size = exposure_room / order.price
                return RiskDecision(
                    approved=True,
                    reason=(
                        "Size reduced to fit both per-market and total caps"
                    ),
                    adjusted_size=adjusted_size,
                )

            return RiskDecision(
                approved=True,
                reason="Size reduced to fit per-market cap",
                adjusted_size=adjusted_size,
            )

        # Rule 2 — total exposure cap, summed across every position.
        current_total_notional = sum(
            (p.size * p.avg_entry_price for p in positions),
            start=Decimal("0"),
        )
        if current_total_notional + order_notional > self._max_total_exposure:
            return RiskDecision(
                approved=False,
                reason=(
                    f"Total exposure cap {self._max_total_exposure} reached"
                ),
                adjusted_size=None,
            )

        return RiskDecision(
            approved=True,
            reason="within_limits",
            adjusted_size=None,
        )

    # ------------------------------------------------------------------
    # Feedback-driven limit updates
    # ------------------------------------------------------------------
    def update_limits(self, feedback: EvaluationFeedback) -> None:
        """Apply guardrail-bounded adjustments derived from ``feedback``.

        Behaviour:

        - ``risk_adjustments.max_drawdown_hit`` → multiply limits by 0.7
          (30% tighter).
        - ``risk_adjustments.suggestion == "relax"`` → multiply limits by
          1.2 (20% looser).
        - Otherwise a no-op.

        All resulting values are clamped to the :data:`GUARDRAILS` bounds.
        """
        risk_fb = feedback.risk_adjustments

        if risk_fb.max_drawdown_hit:
            new_max_pos = self._max_position_per_market * Decimal("0.7")
            new_max_exp = self._max_total_exposure * Decimal("0.7")
        elif risk_fb.suggestion == "relax":
            new_max_pos = self._max_position_per_market * Decimal("1.2")
            new_max_exp = self._max_total_exposure * Decimal("1.2")
        else:
            return

        self._max_position_per_market = apply_guardrail(
            "max_position_per_market", new_max_pos
        )
        self._max_total_exposure = apply_guardrail(
            "max_total_exposure", new_max_exp
        )

    # ------------------------------------------------------------------
    # Observability
    # ------------------------------------------------------------------
    def current_limits(self) -> dict[str, Decimal]:
        """Return a snapshot of the currently active limits."""
        return {
            "max_position_per_market": self._max_position_per_market,
            "max_total_exposure": self._max_total_exposure,
            "max_drawdown_pct": self._max_drawdown_pct,
        }
