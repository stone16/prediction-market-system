"""Tests for RiskManager (CP08).

Covers every CP08 risk acceptance criterion:

1. ``RiskManager.check_order()`` returns ``RiskDecision`` (approve/reject).
2. Orders that would breach ``max_position_per_market`` are rejected
   (or size-adjusted when partial room remains).
3. Orders exceeding ``max_total_exposure`` are rejected.
4. ``RiskManager.update_limits()`` reacts to feedback — tightens on a
   drawdown hit and relaxes on a "relax" suggestion.
5. Both ``__init__`` and ``update_limits`` clamp values to the
   ``GUARDRAILS`` floor/ceiling bounds.

All financial math uses ``Decimal`` per CP01.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Literal

import pytest

from pms.execution.guardrails import GUARDRAILS, apply_guardrail
from pms.execution.risk import RiskManager
from pms.models import (
    ConnectorFeedback,
    EvaluationFeedback,
    Order,
    Position,
    RiskDecision,
    RiskFeedback,
)


# ---------------------------------------------------------------------------
# Factories
# ---------------------------------------------------------------------------


def _order(
    price: Decimal,
    size: Decimal,
    platform: str = "polymarket",
    market_id: str = "m-1",
    outcome_id: str = "yes",
    order_id: str = "",
    side: Literal["buy", "sell"] = "buy",
) -> Order:
    return Order(
        order_id=order_id,
        platform=platform,
        market_id=market_id,
        outcome_id=outcome_id,
        side=side,
        price=price,
        size=size,
        order_type="limit",
    )


def _position(
    size: Decimal,
    avg_entry_price: Decimal,
    platform: str = "polymarket",
    market_id: str = "m-1",
    outcome_id: str = "yes",
) -> Position:
    return Position(
        platform=platform,
        market_id=market_id,
        outcome_id=outcome_id,
        size=size,
        avg_entry_price=avg_entry_price,
        unrealized_pnl=Decimal("0"),
    )


def _feedback(
    *,
    max_drawdown_hit: bool = False,
    suggestion: str = "hold",
    current_exposure: Decimal = Decimal("0"),
) -> EvaluationFeedback:
    return EvaluationFeedback(
        timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
        period=timedelta(minutes=5),
        strategy_adjustments={},
        risk_adjustments=RiskFeedback(
            max_drawdown_hit=max_drawdown_hit,
            current_exposure=current_exposure,
            suggestion=suggestion,
        ),
        connector_adjustments={},
    )


# ---------------------------------------------------------------------------
# Construction + guardrails
# ---------------------------------------------------------------------------


def test_default_limits_are_guardrail_bounded() -> None:
    """Defaults fall inside the guardrail floor/ceiling ranges."""
    rm = RiskManager()
    limits = rm.current_limits()

    for name in ("max_position_per_market", "max_total_exposure", "max_drawdown_pct"):
        bounds = GUARDRAILS[name]
        assert bounds["floor"] <= limits[name] <= bounds["ceiling"], (
            f"{name}={limits[name]} escaped guardrails {bounds}"
        )


def test_init_clamps_value_below_floor_up_to_floor() -> None:
    """Constructing with a value beneath the guardrail floor clamps up."""
    rm = RiskManager(
        max_position_per_market=Decimal("0.001"),  # far below floor of 10
        max_total_exposure=Decimal("0.001"),  # far below floor of 100
        max_drawdown_pct=Decimal("0.0001"),  # far below floor of 0.01
    )
    limits = rm.current_limits()
    assert limits["max_position_per_market"] == GUARDRAILS["max_position_per_market"]["floor"]
    assert limits["max_total_exposure"] == GUARDRAILS["max_total_exposure"]["floor"]
    assert limits["max_drawdown_pct"] == GUARDRAILS["max_drawdown_pct"]["floor"]


def test_init_clamps_value_above_ceiling_down_to_ceiling() -> None:
    """Constructing with a value above the guardrail ceiling clamps down."""
    rm = RiskManager(
        max_position_per_market=Decimal("1000000"),
        max_total_exposure=Decimal("1000000"),
        max_drawdown_pct=Decimal("0.99"),
    )
    limits = rm.current_limits()
    assert limits["max_position_per_market"] == GUARDRAILS["max_position_per_market"]["ceiling"]
    assert limits["max_total_exposure"] == GUARDRAILS["max_total_exposure"]["ceiling"]
    assert limits["max_drawdown_pct"] == GUARDRAILS["max_drawdown_pct"]["ceiling"]


def test_apply_guardrail_unknown_name_passes_through() -> None:
    """Unknown guardrail names are not clamped."""
    value = Decimal("123.456")
    assert apply_guardrail("not-a-real-parameter", value) == value


# ---------------------------------------------------------------------------
# check_order — approvals, rejections, adjustments
# ---------------------------------------------------------------------------


def test_approve_order_within_per_market_cap_empty_positions() -> None:
    """Small order against empty portfolio is approved without adjustment."""
    rm = RiskManager(
        max_position_per_market=Decimal("500"),
        max_total_exposure=Decimal("5000"),
    )
    order = _order(price=Decimal("0.50"), size=Decimal("100"))  # notional 50
    decision = rm.check_order(order, positions=[])

    assert isinstance(decision, RiskDecision)
    assert decision.approved is True
    assert decision.adjusted_size is None


def test_reject_order_when_per_market_cap_already_full() -> None:
    """Existing position already at the per-market cap — new order is rejected."""
    rm = RiskManager(
        max_position_per_market=Decimal("500"),
        max_total_exposure=Decimal("5000"),
    )
    # existing position notional = 1000 * 0.50 = 500 (exactly the cap)
    existing = _position(size=Decimal("1000"), avg_entry_price=Decimal("0.50"))
    order = _order(price=Decimal("0.50"), size=Decimal("10"))  # +5 notional
    decision = rm.check_order(order, positions=[existing])

    assert decision.approved is False
    assert "cap" in decision.reason.lower()
    assert decision.adjusted_size is None


def test_adjust_order_size_when_partially_fits_per_market_cap() -> None:
    """Partial room remaining → approved with a reduced size that fits."""
    rm = RiskManager(
        max_position_per_market=Decimal("500"),
        max_total_exposure=Decimal("5000"),
    )
    # existing notional = 800 * 0.50 = 400 → 100 room left
    existing = _position(size=Decimal("800"), avg_entry_price=Decimal("0.50"))
    # requested notional = 200 * 0.50 = 100 (fits exactly); go larger to force a shrink
    order = _order(price=Decimal("0.50"), size=Decimal("400"))  # notional 200, only 100 fits
    decision = rm.check_order(order, positions=[existing])

    assert decision.approved is True
    assert decision.adjusted_size is not None
    # remaining room 100 / price 0.50 = 200
    assert decision.adjusted_size == Decimal("200")


def test_partial_fit_also_respects_total_exposure_cap() -> None:
    """CP08 iter-2 fix: partial-fit path must re-validate against total exposure.

    Regression test for a bug where ``check_order`` would shrink an order to
    fit ``max_position_per_market`` but forget to re-check the reduced order
    against ``max_total_exposure``. The reduced order could therefore push
    the portfolio over the total-exposure cap.
    """
    rm = RiskManager(
        max_position_per_market=Decimal("500"),
        max_total_exposure=Decimal("1000"),
    )
    # Existing notional in the target market "M1": 400
    p_same_market = _position(
        size=Decimal("400"),
        avg_entry_price=Decimal("1.0"),
        market_id="M1",
        outcome_id="O1",
    )
    # Existing notional in a different market: 580 → total exposure = 980
    p_other_market = _position(
        size=Decimal("580"),
        avg_entry_price=Decimal("1.0"),
        market_id="M2",
        outcome_id="O1",
    )
    # New order on market M1: notional 200. Per-market room is 100 (500-400),
    # so the naive fix would shrink to 100. But total room is only 20
    # (1000-980), so the order must be shrunk further or rejected.
    new_order = _order(
        price=Decimal("1.0"),
        size=Decimal("200"),
        market_id="M1",
        outcome_id="O2",
    )
    decision = rm.check_order(new_order, positions=[p_same_market, p_other_market])

    if decision.approved:
        adjusted = decision.adjusted_size or new_order.size
        total_after = Decimal("980") + adjusted * new_order.price
        assert total_after <= Decimal("1000"), (
            f"Total exposure {total_after} exceeds cap 1000 after partial-fit "
            "adjustment"
        )


def test_reject_order_on_total_exposure_cap() -> None:
    """Multiple positions push the portfolio to the total exposure cap."""
    rm = RiskManager(
        max_position_per_market=Decimal("5000"),  # per-market cap well above
        max_total_exposure=Decimal("1000"),
    )
    p1 = _position(
        size=Decimal("1000"),
        avg_entry_price=Decimal("0.50"),
        market_id="m-1",
    )  # 500 notional
    p2 = _position(
        size=Decimal("1000"),
        avg_entry_price=Decimal("0.40"),
        market_id="m-2",
    )  # 400 notional → total 900
    # new order on a third market: notional 200 → 900 + 200 > 1000 cap
    order = _order(price=Decimal("0.40"), size=Decimal("500"), market_id="m-3")
    decision = rm.check_order(order, positions=[p1, p2])

    assert decision.approved is False
    assert "exposure" in decision.reason.lower()
    assert decision.adjusted_size is None


def test_approve_order_when_under_total_exposure_cap() -> None:
    """Portfolio nowhere near limits → order is approved."""
    rm = RiskManager(
        max_position_per_market=Decimal("500"),
        max_total_exposure=Decimal("5000"),
    )
    p1 = _position(
        size=Decimal("200"), avg_entry_price=Decimal("0.50"), market_id="m-other"
    )  # 100 notional
    order = _order(price=Decimal("0.50"), size=Decimal("100"))  # 50 notional
    decision = rm.check_order(order, positions=[p1])

    assert decision.approved is True
    assert decision.adjusted_size is None


# ---------------------------------------------------------------------------
# check_order — sell-side regression (review-loop fix A: f1)
# ---------------------------------------------------------------------------


def test_sell_order_reduces_exposure_not_adds() -> None:
    """A sell order against an existing long position must NOT count as
    new exposure. Regression for review-loop f1: prior to the fix,
    ``check_order`` computed ``order.price * order.size`` regardless of
    ``order.side``, so a closing sell against a position already at the cap
    was rejected as if it were doubling the exposure.
    """
    rm = RiskManager(
        max_position_per_market=Decimal("500"),
        max_total_exposure=Decimal("5000"),
    )
    # Existing long position sitting EXACTLY at the per-market cap.
    # 1000 * 0.50 = 500 notional == max_position_per_market.
    existing = _position(size=Decimal("1000"), avg_entry_price=Decimal("0.50"))
    sell_order = _order(
        price=Decimal("0.50"),
        size=Decimal("100"),
        side="sell",
    )

    decision = rm.check_order(sell_order, positions=[existing])

    # Selling reduces (not adds to) exposure, so it must be approved
    # even though the buy-side cap is already reached.
    assert decision.approved is True
    assert decision.adjusted_size is None


def test_sell_order_below_cap_approved() -> None:
    """Selling part of a long position well within the cap is approved."""
    rm = RiskManager(
        max_position_per_market=Decimal("500"),
        max_total_exposure=Decimal("5000"),
    )
    # 600 * 0.50 = 300 notional, comfortably under the 500 cap.
    existing = _position(size=Decimal("600"), avg_entry_price=Decimal("0.50"))
    sell_order = _order(
        price=Decimal("0.50"),
        size=Decimal("100"),
        side="sell",
    )

    decision = rm.check_order(sell_order, positions=[existing])

    assert decision.approved is True
    assert decision.adjusted_size is None


def test_sell_order_with_no_existing_position_counts_as_short_exposure() -> None:
    """Review-loop f10 (round 2): a naked sell with no held position must
    be treated as **new short exposure**, not as a no-op.

    The round-1 fix unconditionally floored sell exposure at zero, which
    silently approved naked shorts as "zero risk". The proper inventory-
    aware model counts the un-covered remainder of any sell as new short
    exposure, so a 100 @ 0.5 naked sell must consume 50 units of the
    per-market and total caps.
    """
    rm = RiskManager(
        max_position_per_market=Decimal("500"),
        max_total_exposure=Decimal("5000"),
    )
    sell_order = _order(
        price=Decimal("0.50"),
        size=Decimal("100"),
        side="sell",
    )
    # Approved (50 < 500 and 50 < 5000), but the new short exposure was
    # taken into account — see ``test_naked_sell_consumes_per_market_cap``
    # below for the cap-consumption assertion.
    decision = rm.check_order(sell_order, positions=[])
    assert decision.approved is True
    assert decision.adjusted_size is None


def test_naked_sell_consumes_per_market_cap() -> None:
    """A naked sell that would push notional past the per-market cap
    must be sized down or rejected — it is NOT zero-risk.

    Construction:
    - per-market cap = 500
    - existing position on a DIFFERENT market = 0
    - naked sell on m-1: 1500 @ 1.0 → 1500 of new short exposure
    - cap is exceeded → must reject (no covering inventory at all)
    """
    rm = RiskManager(
        max_position_per_market=Decimal("500"),
        max_total_exposure=Decimal("5000"),
    )
    # No held positions on m-1, so the entire sell is new short exposure.
    sell_order = _order(
        price=Decimal("1.0"),
        size=Decimal("1500"),
        side="sell",
        market_id="m-1",
    )
    decision = rm.check_order(sell_order, positions=[])
    # 1500 > 500, but partial-fit allows 500/1.0 = 500 shares.
    assert decision.approved is True
    assert decision.adjusted_size == Decimal("500")


def test_naked_sell_rejected_when_cap_already_full_from_other_short() -> None:
    """Once short exposure on a market is at the cap, more naked shorts
    on the SAME outcome must be rejected — not silently approved.

    We simulate "existing short" by parking a long position on a sibling
    outcome of the same market that fills the per-market cap, then we
    fire a naked sell on a third outcome of the same market.
    """
    rm = RiskManager(
        max_position_per_market=Decimal("500"),
        max_total_exposure=Decimal("5000"),
    )
    # Existing long fills the per-market cap entirely.
    full_market = _position(
        size=Decimal("1000"),
        avg_entry_price=Decimal("0.50"),
        market_id="m-1",
        outcome_id="other-outcome",
    )
    # New naked sell on a different outcome of the same market.
    sell_order = _order(
        price=Decimal("0.50"),
        size=Decimal("200"),
        side="sell",
        market_id="m-1",
        outcome_id="naked-outcome",
    )
    decision = rm.check_order(sell_order, positions=[full_market])
    # Cap already full → naked sell on another outcome must be rejected.
    assert decision.approved is False
    assert decision.adjusted_size is None


def test_partial_sell_of_over_limit_position_strictly_reducing_is_approved() -> None:
    """Review-loop f10 (round 2): a strictly-reducing sell against a
    position that is *already* over the cap must be approved.

    Construction:
    - per-market cap = 500
    - existing long position on m-1 = 800 @ 1.0 → 800 notional (already
      over cap because the cap was tightened after entry)
    - sell 200 @ 1.0 → reduces market notional by 200 (cost basis-aware)
    - new market notional after sell = 600
    - 600 is still > 500, but the order strictly reduces risk so it must
      be approved unconditionally.
    """
    rm = RiskManager(
        max_position_per_market=Decimal("500"),
        max_total_exposure=Decimal("5000"),
    )
    over_limit = _position(
        size=Decimal("800"),
        avg_entry_price=Decimal("1.0"),
        market_id="m-1",
        outcome_id="yes",
    )
    sell_order = _order(
        price=Decimal("1.0"),
        size=Decimal("200"),
        side="sell",
        market_id="m-1",
        outcome_id="yes",
    )
    decision = rm.check_order(sell_order, positions=[over_limit])
    # Strictly reducing → always approved, even though new notional is
    # still above the cap.
    assert decision.approved is True
    assert decision.adjusted_size is None


def test_sell_partial_fit_piecewise_calculation() -> None:
    """Review-loop fix f12: a sell that crosses from covered to short
    must be sized using piecewise math, not ``remaining / price``.

    Construction (Codex repro):
    - per-market cap = 40
    - held: 100 @ 0.20 → current market notional = 20
    - sell 150 @ 0.90

    Exposure delta for a full 150-share sell:
      covered portion (100): -100 * 0.20 = -20
      short portion (50):    +50  * 0.90 = +45
      total delta:           +25 → new notional 45 > cap 40

    Correct piecewise max-fillable sell:
      after clearing 100 covered shares, exposure is 0 (20 - 20).
      remaining market room = 40 - 0 = 40
      short portion at 0.90: 40 / 0.90 ≈ 44.444...
      max s = 100 (covered) + 44.444... = 144.444...

    The buggy pre-fix path returned ``remaining_before_sell / price``
    = 20 / 0.90 ≈ 22.22, which is off by ~6.5x. Any post-trade exposure
    strictly under 40 + epsilon is considered valid.
    """
    rm = RiskManager(
        max_position_per_market=Decimal("40"),
        max_total_exposure=Decimal("5000"),
    )
    held = _position(
        size=Decimal("100"),
        avg_entry_price=Decimal("0.20"),
        market_id="m-1",
        outcome_id="yes",
    )
    sell_order = _order(
        price=Decimal("0.90"),
        size=Decimal("150"),
        side="sell",
        market_id="m-1",
        outcome_id="yes",
    )

    decision = rm.check_order(sell_order, positions=[held])

    assert decision.approved is True
    assert decision.adjusted_size is not None
    # Correct answer: 100 (covered) + 40/0.90 ≈ 144.444...
    expected = Decimal("100") + Decimal("40") / Decimal("0.90")
    # Allow a small tolerance for Decimal precision on the division.
    diff = abs(decision.adjusted_size - expected)
    assert diff < Decimal("0.0001"), (
        f"Expected adjusted_size ~ {expected}, got {decision.adjusted_size}"
    )
    # And the buggy linear answer must NOT be returned.
    assert decision.adjusted_size > Decimal("30"), (
        f"adjusted_size {decision.adjusted_size} looks like the buggy "
        "linear path (remaining/price ≈ 22.22)"
    )

    # Post-trade exposure must actually fit under the cap.
    covered = min(sell_order.size, held.size)
    short_remainder = decision.adjusted_size - covered
    # held has avg_basis 0.20 → reduction = covered * 0.20
    reduction = covered * Decimal("0.20")
    new_short = short_remainder * sell_order.price
    delta = -reduction + new_short
    current_notional = held.size * held.avg_entry_price  # 20
    post_trade = current_notional + delta
    assert post_trade <= Decimal("40") + Decimal("0.0001"), (
        f"Post-trade exposure {post_trade} exceeds cap 40 after "
        "partial-fit sizing"
    )


def test_sell_fully_covered_no_partial_fit_needed() -> None:
    """A fully-covered sell must be approved at full size without
    invoking the partial-fit branch.

    Construction:
    - per-market cap = 50
    - held: 100 @ 0.50 → current market notional = 50 (at cap)
    - sell 80 @ 0.50 → covered portion 80 * 0.50 = -40 delta
      → new notional = 50 - 40 = 10, strictly below cap.
    """
    rm = RiskManager(
        max_position_per_market=Decimal("50"),
        max_total_exposure=Decimal("5000"),
    )
    held = _position(
        size=Decimal("100"),
        avg_entry_price=Decimal("0.50"),
        market_id="m-1",
        outcome_id="yes",
    )
    sell_order = _order(
        price=Decimal("0.50"),
        size=Decimal("80"),
        side="sell",
        market_id="m-1",
        outcome_id="yes",
    )
    decision = rm.check_order(sell_order, positions=[held])

    assert decision.approved is True
    assert decision.adjusted_size is None  # no adjustment needed


def test_sell_with_no_held_position_uses_linear_math() -> None:
    """Naked short with no held inventory must use the linear
    ``remaining / price`` calculation — there is no covered portion to
    add on top.

    Construction:
    - per-market cap = 40
    - no held positions
    - sell 100 @ 0.50 → naked short would add 50 exposure > 40
    - max fillable = 40 / 0.50 = 80 shares
    """
    rm = RiskManager(
        max_position_per_market=Decimal("40"),
        max_total_exposure=Decimal("5000"),
    )
    sell_order = _order(
        price=Decimal("0.50"),
        size=Decimal("100"),
        side="sell",
        market_id="m-1",
        outcome_id="yes",
    )
    decision = rm.check_order(sell_order, positions=[])

    assert decision.approved is True
    assert decision.adjusted_size == Decimal("80")


def test_fully_covered_sell_reduces_per_market_exposure() -> None:
    """A sell that is fully covered by inventory must reduce per-market
    notional by the proportional cost basis, not by the order price."""
    rm = RiskManager(
        max_position_per_market=Decimal("500"),
        max_total_exposure=Decimal("5000"),
    )
    held = _position(
        size=Decimal("400"),
        avg_entry_price=Decimal("0.50"),
        market_id="m-1",
        outcome_id="yes",
    )
    sell_order = _order(
        price=Decimal("0.50"),
        size=Decimal("200"),
        side="sell",
        market_id="m-1",
        outcome_id="yes",
    )
    decision = rm.check_order(sell_order, positions=[held])
    assert decision.approved is True
    assert decision.adjusted_size is None


def test_sell_flip_to_overcap_short_is_sized_down() -> None:
    """Codex final consensus regression (review-loop fix f15): a sell that
    flips a long into an over-cap short must NOT bypass the cap check via
    the strictly_reducing shortcut.

    Construction:
    - per-market cap = 500
    - held: 800 @ 1.0 → current market notional = 800 (above cap)
    - sell 1400 @ 1.0

    Exposure delta for the full 1400-share sell:
      covered portion (800): -800 * 1.0 = -800
      short portion  (600):  +600 * 1.0 = +600
      total delta:           -200 → new market notional = 600

    Pre-fix bug: ``new_market_notional (600) < current_market_notional
    (800)`` → strictly_reducing fast-path → approved at full 1400 size,
    leaving the post-trade short (600) STILL above the per-market cap
    (500).

    Correct behaviour: fall through to the partial-fit branch and use
    ``_max_fillable_sell_size`` to size the order down so that
    post-trade exposure ≤ cap.
    """
    rm = RiskManager(
        max_position_per_market=Decimal("500"),
        max_total_exposure=Decimal("10000"),
    )
    held = _position(
        size=Decimal("800"),
        avg_entry_price=Decimal("1.0"),
        market_id="m-1",
        outcome_id="yes",
    )
    sell_order = _order(
        price=Decimal("1.0"),
        size=Decimal("1400"),
        side="sell",
        market_id="m-1",
        outcome_id="yes",
    )

    decision = rm.check_order(sell_order, positions=[held])

    # The shortcut must NOT have approved this at full size with no
    # adjustment: that would leave a 600 short above the 500 cap.
    assert not (
        decision.approved
        and decision.adjusted_size is None
        and decision.reason == "strictly_reducing"
    ), (
        "strictly_reducing shortcut bypassed cap check: approved full "
        f"1400 size leaving 600 short above 500 cap (reason={decision.reason!r})"
    )

    # If approved (with size-down) the post-trade exposure must fit the cap.
    if decision.approved:
        adjusted = decision.adjusted_size or sell_order.size
        # Piecewise post-trade math: covered portion clears at avg_basis,
        # short remainder opens at order.price.
        covered = min(adjusted, held.size)
        short_remainder = adjusted - covered
        avg_basis = held.avg_entry_price  # held_value / held_size = 1.0
        reduction = covered * avg_basis
        new_short_notional = short_remainder * sell_order.price
        current_notional = held.size * held.avg_entry_price  # 800
        post_notional = current_notional - reduction + new_short_notional
        assert post_notional <= Decimal("500") + Decimal("0.0001"), (
            f"Post-trade per-market notional {post_notional} exceeds cap "
            f"500 after approval (adjusted_size={adjusted})"
        )


def test_sell_strictly_reducing_within_cap_approved() -> None:
    """Sanity: a fully-covered sell whose post-trade notional fits the
    cap must still take the strictly_reducing fast-path (no adjustment).

    Construction:
    - per-market cap = 500
    - held: 600 @ 1.0 → current market notional = 600 (above cap)
    - sell 200 @ 1.0 → fully covered, new notional = 400 (fits cap)
    """
    rm = RiskManager(
        max_position_per_market=Decimal("500"),
        max_total_exposure=Decimal("10000"),
    )
    held = _position(
        size=Decimal("600"),
        avg_entry_price=Decimal("1.0"),
        market_id="m-1",
        outcome_id="yes",
    )
    sell_order = _order(
        price=Decimal("1.0"),
        size=Decimal("200"),
        side="sell",
        market_id="m-1",
        outcome_id="yes",
    )
    decision = rm.check_order(sell_order, positions=[held])
    assert decision.approved is True
    assert decision.adjusted_size is None
    assert decision.reason == "strictly_reducing"


# ---------------------------------------------------------------------------
# update_limits — tightening / relaxing, with guardrail clamping
# ---------------------------------------------------------------------------


def test_update_limits_tightens_on_drawdown_hit() -> None:
    """A drawdown hit reduces limits by 30%."""
    rm = RiskManager(
        max_position_per_market=Decimal("500"),
        max_total_exposure=Decimal("5000"),
    )
    before = rm.current_limits()
    rm.update_limits(_feedback(max_drawdown_hit=True))
    after = rm.current_limits()

    assert after["max_position_per_market"] == before["max_position_per_market"] * Decimal("0.7")
    assert after["max_total_exposure"] == before["max_total_exposure"] * Decimal("0.7")


def test_update_limits_relaxes_on_relax_suggestion() -> None:
    """A "relax" suggestion raises limits by 20%."""
    rm = RiskManager(
        max_position_per_market=Decimal("500"),
        max_total_exposure=Decimal("5000"),
    )
    before = rm.current_limits()
    rm.update_limits(_feedback(suggestion="relax"))
    after = rm.current_limits()

    assert after["max_position_per_market"] == before["max_position_per_market"] * Decimal("1.2")
    assert after["max_total_exposure"] == before["max_total_exposure"] * Decimal("1.2")


def test_update_limits_no_op_without_signal() -> None:
    """Neutral feedback leaves limits untouched."""
    rm = RiskManager(
        max_position_per_market=Decimal("500"),
        max_total_exposure=Decimal("5000"),
    )
    before = rm.current_limits()
    rm.update_limits(_feedback())  # neutral: no drawdown, suggestion="hold"
    assert rm.current_limits() == before


def test_update_limits_clamps_to_ceiling_on_relax() -> None:
    """Relax that would exceed the ceiling is clamped down to the ceiling."""
    # Start just under the ceiling so a 1.2x multiplier blows past it
    rm = RiskManager(
        max_position_per_market=Decimal("4500"),  # ceiling 5000
        max_total_exposure=Decimal("45000"),  # ceiling 50000
    )
    rm.update_limits(_feedback(suggestion="relax"))
    after = rm.current_limits()

    assert after["max_position_per_market"] == GUARDRAILS["max_position_per_market"]["ceiling"]
    assert after["max_total_exposure"] == GUARDRAILS["max_total_exposure"]["ceiling"]


def test_update_limits_clamps_to_floor_on_drawdown() -> None:
    """Tightening that would fall below the floor is clamped up to the floor."""
    # Start just above the floor so a 0.7x multiplier would drop below
    rm = RiskManager(
        max_position_per_market=Decimal("12"),  # floor 10 → 12*0.7 = 8.4, must clamp to 10
        max_total_exposure=Decimal("120"),  # floor 100 → 120*0.7 = 84, must clamp to 100
    )
    rm.update_limits(_feedback(max_drawdown_hit=True))
    after = rm.current_limits()

    assert after["max_position_per_market"] == GUARDRAILS["max_position_per_market"]["floor"]
    assert after["max_total_exposure"] == GUARDRAILS["max_total_exposure"]["floor"]
