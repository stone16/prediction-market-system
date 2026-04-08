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
) -> Order:
    return Order(
        order_id=order_id,
        platform=platform,
        market_id=market_id,
        outcome_id=outcome_id,
        side="buy",
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
