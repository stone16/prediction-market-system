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

Sell-side exposure model (review-loop fix f10, round 2)
-------------------------------------------------------

The first attempt at fixing f1 unconditionally subtracted the sell
notional from current exposure and floored at zero. That was wrong in
two ways:

1. A naked sell with no held inventory was approved as zero-risk even
   though it opens a fresh short position.
2. A partial sell of an over-limit long was rejected as if it were
   adding new exposure, even though it strictly reduces risk.

The correct, inventory-aware model splits a sell into a covered portion
(closes part of the held long, reduces exposure by the proportional cost
basis) and a remainder (opens a new short, adds exposure at order
price). On top of that, any order whose post-trade per-market notional
is *strictly less than* the current notional is always approved — risk
reductions never need a cap check.

Buys keep the original behaviour: ``order.size * order.price`` is added
to per-market and total notionals.

Piecewise partial-fit sizing for sells (review-loop fix f12, round 3)
---------------------------------------------------------------------

The round-2 :meth:`_compute_exposure_delta` correctly models sells that
span both covered inventory and a short remainder, but the partial-fit
branch in :meth:`check_order` shrank over-cap orders using a **linear**
formula ``remaining / order.price``. That formula is only correct for
buys and for pure-short sells; it fundamentally ignores the covered-
portion reduction for mixed sells and under-sizes the order.

Concrete Codex repro:

- held: ``100 @ 0.20`` → current market notional = ``20``
- cap: ``40``
- sell: ``150 @ 0.90``

Full-order delta = ``-100 * 0.20 + 50 * 0.90 = -20 + 45 = +25``, so
``new_market_notional = 20 + 25 = 45 > 40``. The linear path then
computes ``adjusted_size = (40 - 20) / 0.90 ≈ 22.22``, which is
~6.5× smaller than the true piecewise maximum of ``~144.44``.

The correct math for sells is to model exposure as a piecewise function
of the sell size ``s``:

    if s <= held_size:
        exposure(s) = current - s * avg_basis     # strictly decreasing
    else:
        exposure(s) = (current - held_size * avg_basis) + (s - held_size) * order.price

At ``s = held_size`` the function reaches ``exposure_after_cover``.
Beyond that, every extra share is pure short exposure at ``order.price``
and the constraint ``exposure(s) <= cap`` linearises to

    short_room = cap - exposure_after_cover
    max_short  = short_room / order.price      (capped at 0)
    max_s      = held_size + max_short

The same shape applies to the total-exposure cap — both caps are walked
together and the binding one wins. :meth:`_max_fillable_sell_size`
implements this. Buys keep their original linear partial-fit path.
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
        """Approve, reject, or size-adjust ``order`` given current positions.

        See the module docstring for the inventory-aware sell model
        (review-loop fix f10, round 2). The pseudo-code below summarises
        the algorithm:

        1. Compute per-market notional and the matching held size/value
           on the *exact* outcome the order targets (a sell only closes
           inventory it actually owns).
        2. Compute ``exposure_delta`` for the order:
              buy:  +order.size * order.price
              sell: covered = min(held_size, order.size)
                    avg_basis = held_value / held_size  (if any)
                    -covered * avg_basis
                    + (order.size - covered) * order.price
        3. ``new_market_notional = current_market_notional + market_delta``
           (where ``market_delta`` is the part of ``exposure_delta`` that
           lives on this market, i.e. the same value).
        4. If ``new_market_notional <= current_market_notional``, the
           order strictly reduces risk on this market — approve
           unconditionally (no cap check).
        5. Otherwise, run the per-market cap check (rejecting or partial-
           fitting), then re-validate against total exposure.
        """
        market_key = (order.platform, order.market_id)
        current_market_notional = self._sum_notional_for_key(
            positions, market_key
        )
        current_total_notional = sum(
            (p.size * p.avg_entry_price for p in positions),
            start=Decimal("0"),
        )

        exposure_delta = self._compute_exposure_delta(order, positions)
        new_market_notional = current_market_notional + exposure_delta
        new_total_notional = current_total_notional + exposure_delta

        # Strictly-reducing orders are always approved, even if the
        # current per-market notional is above the cap (e.g. because the
        # cap was tightened after the position was opened).
        if new_market_notional < current_market_notional:
            return RiskDecision(
                approved=True,
                reason="strictly_reducing",
                adjusted_size=None,
            )

        # Per-market cap check (only fires when the order INCREASES
        # exposure on this market — sells with covered_only<order.size
        # can also land here when the short remainder pushes new notional
        # above current).
        if new_market_notional > self._max_position_per_market:
            if order.side == "sell":
                # Review-loop fix f12: piecewise sizing for sells. The
                # linear ``remaining / price`` path under-sizes any sell
                # that spans both covered inventory and a short
                # remainder because it ignores the exposure reduction
                # from the covered portion. ``_max_fillable_sell_size``
                # walks both caps together and returns the largest ``s``
                # such that post-trade exposure stays under both.
                adjusted_size = self._max_fillable_sell_size(
                    order,
                    positions,
                    current_market_notional,
                    current_total_notional,
                )
                if adjusted_size <= Decimal("0"):
                    return RiskDecision(
                        approved=False,
                        reason=(
                            f"Market position cap {self._max_position_per_market} "
                            "reached"
                        ),
                        adjusted_size=None,
                    )
                return RiskDecision(
                    approved=True,
                    reason=(
                        "Sell size reduced to fit caps (piecewise)"
                    ),
                    adjusted_size=adjusted_size,
                )

            # Buy partial-fit: linear math against ``order.price``.
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
            adjusted_size = remaining / order.price
            adjusted_notional = adjusted_size * order.price

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

        # Per-market cap is fine — re-check total exposure.
        if new_total_notional > self._max_total_exposure:
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
    # Inventory-aware exposure helpers (review-loop fix f10 round 2)
    # ------------------------------------------------------------------

    @staticmethod
    def _sum_notional_for_key(
        positions: Sequence[Position], market_key: tuple[str, str]
    ) -> Decimal:
        return sum(
            (
                p.size * p.avg_entry_price
                for p in positions
                if (p.platform, p.market_id) == market_key
            ),
            start=Decimal("0"),
        )

    def _max_fillable_sell_size(
        self,
        order: Order,
        positions: Sequence[Position],
        current_market_notional: Decimal,
        current_total_notional: Decimal,
    ) -> Decimal:
        """Return the largest sell size whose post-trade exposure fits.

        Review-loop fix f12 (round 3): sells with a mixed covered +
        short shape need piecewise sizing. The linear formula
        ``remaining / order.price`` under-sizes the order because it
        ignores the exposure reduction from the covered portion. This
        helper walks both the per-market and total-exposure caps
        together and returns the binding limit (min) — or ``0`` if
        there is no room at all.

        Pseudocode::

            exposure_after_cover = current - held_size * avg_basis
            market_room = per_market_cap - exposure_after_cover
            total_room  = total_cap - exposure_after_cover
            if market_room < 0 or total_room < 0:
                return 0       # even zero-short fit breaks a cap
            max_short = min(market_room, total_room) / order.price
            return min(order.size, held_size + max_short)

        Notes:
          * The helper is only called for **sell** orders that have
            already been classified as over-cap by the caller; the
            strictly-reducing fast-path in :meth:`check_order` filters
            out pure-reduction sells before we get here.
          * ``held_size`` and ``held_value`` are walked against the
            EXACT (platform, market_id, outcome_id) the order targets,
            same as :meth:`_compute_exposure_delta`, so a sell never
            "covers itself" with inventory on a sibling outcome.
        """
        held_size, held_value = self._held_size_and_value_for_order(
            order, positions
        )

        if held_size <= Decimal("0"):
            # Naked short: pure linear. Both caps apply at ``order.price``.
            market_room = (
                self._max_position_per_market - current_market_notional
            )
            total_room = (
                self._max_total_exposure - current_total_notional
            )
            room = min(market_room, total_room)
            if room <= Decimal("0"):
                return Decimal("0")
            max_short = room / order.price
            return min(order.size, max_short)

        avg_basis = held_value / held_size

        # Exposure AFTER the covered portion is fully sold. For a
        # fully-covered sell (order.size <= held_size) the exposure
        # continues to decrease beyond this point, but the caller
        # already ensured this sell is over-cap so by definition it has
        # a short remainder.
        covered_for_reduction = min(order.size, held_size)
        reduction = covered_for_reduction * avg_basis
        market_after_cover = current_market_notional - reduction
        total_after_cover = current_total_notional - reduction

        market_room = self._max_position_per_market - market_after_cover
        total_room = self._max_total_exposure - total_after_cover

        if market_room <= Decimal("0") or total_room <= Decimal("0"):
            # Even fully clearing the held inventory doesn't open any
            # room for a short remainder. The best we can do is sell
            # the covered portion — which is strictly-reducing anyway,
            # so it wouldn't have landed in the partial-fit branch.
            # Return the covered size so the caller approves it instead
            # of rejecting outright.
            return covered_for_reduction

        room = min(market_room, total_room)
        max_short = room / order.price
        return min(order.size, held_size + max_short)

    @staticmethod
    def _held_size_and_value_for_order(
        order: Order, positions: Sequence[Position]
    ) -> tuple[Decimal, Decimal]:
        """Walk positions on the order's exact outcome; return (size, value)."""
        held_size = Decimal("0")
        held_value = Decimal("0")
        for p in positions:
            if (
                p.platform == order.platform
                and p.market_id == order.market_id
                and p.outcome_id == order.outcome_id
            ):
                held_size += p.size
                held_value += p.size * p.avg_entry_price
        return held_size, held_value

    @classmethod
    def _compute_exposure_delta(
        cls, order: Order, positions: Sequence[Position]
    ) -> Decimal:
        """Return the change in exposure notional if ``order`` fills.

        Buys add ``order.size * order.price``. Sells split into a
        covered portion (closes part of the long on the targeted
        outcome at proportional cost basis) and a short remainder
        (opens new short exposure at order price).
        """
        if order.side == "buy":
            return order.price * order.size

        # Sell: walk only positions on the EXACT outcome being sold.
        held_size, held_value = cls._held_size_and_value_for_order(
            order, positions
        )

        if held_size <= Decimal("0"):
            # Naked short: every share is fresh exposure at order price.
            return order.price * order.size

        if held_size >= order.size:
            # Fully covered: reduce exposure by the proportional cost basis.
            avg_basis = held_value / held_size
            return -(order.size * avg_basis)

        # Partially covered: close the held portion, open a short for the
        # remainder.
        avg_basis = held_value / held_size
        reduction = -(held_size * avg_basis)
        new_short = (order.size - held_size) * order.price
        return reduction + new_short

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
