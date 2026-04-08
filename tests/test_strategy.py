"""Tests for ArbitrageStrategy (CP07).

Covers every CP07 acceptance criterion:

1. Cross-platform spread detection (>= configurable threshold) → Order list
   (``test_cross_platform_arb_detection``,
    ``test_cross_platform_paired_correlation_id``).
2. No orders emitted when the cross-platform spread is below the threshold
   (``test_cross_platform_no_arb_when_spread_too_small``).
3. Subset pricing violations → orders that buy the cheap superset and sell
   the overpriced subset (``test_subset_violation_detection``,
    ``test_subset_paired_correlation_id``).
4. No action when subset prices are logically consistent
   (``test_subset_no_action_when_prices_consistent``).
5. No action for non-"subset" relation types
   (``test_subset_no_action_for_independent_relation``).
6. Feedback adjustments:
   - Poor win_rate + negative pnl raises ``min_spread`` by 50%
     (``test_feedback_raises_spread_on_poor_performance``).
   - High observed slippage bumps ``min_spread`` by the slippage amount
     (``test_feedback_raises_spread_on_high_slippage``).
   - Guardrails clamp ``min_spread`` to the 0.005–0.20 band
     (``test_feedback_respects_ceiling_guardrail``).
   - Feedback for an unrelated strategy name is a no-op
     (``test_feedback_no_op_for_irrelevant_strategy_name``).
7. ``max_position_size`` is the size on every emitted order
   (``test_max_position_size_respected``).
8. Strategy satisfies ``StrategyProtocol`` at runtime
   (``test_protocol_compat``).

All tests use ``Decimal`` for financial math and construct immutable,
frozen model instances per CP01.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from pms.models import (
    CorrelationPair,
    EvaluationFeedback,
    Market,
    Outcome,
    PriceUpdate,
    RiskFeedback,
    StrategyFeedback,
)
from pms.protocols import StrategyProtocol
from pms.strategy import ArbitragePairOrders, ArbitrageStrategy


# ---------------------------------------------------------------------------
# Sample object factories — frozen instances per CP01
# ---------------------------------------------------------------------------


def _price_update(
    platform: str,
    bid: Decimal,
    ask: Decimal,
    market_id: str = "m-1",
    outcome_id: str = "yes",
) -> PriceUpdate:
    return PriceUpdate(
        platform=platform,
        market_id=market_id,
        outcome_id=outcome_id,
        bid=bid,
        ask=ask,
        last=(bid + ask) / Decimal("2"),
        timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )


def _market(
    platform: str,
    market_id: str,
    yes_price: Decimal,
) -> Market:
    return Market(
        platform=platform,
        market_id=market_id,
        title=f"Sample {market_id}",
        description=f"Sample market {market_id}",
        outcomes=[
            Outcome(outcome_id="yes", title="Yes", price=yes_price),
            Outcome(
                outcome_id="no",
                title="No",
                price=Decimal("1") - yes_price,
            ),
        ],
        volume=Decimal("1000"),
        end_date=datetime(2030, 1, 1, tzinfo=timezone.utc),
        category="test",
        url=f"https://example.com/{market_id}",
        status="open",
        raw={},
    )


def _empty_risk_feedback() -> RiskFeedback:
    return RiskFeedback(
        max_drawdown_hit=False,
        current_exposure=Decimal("0"),
        suggestion="hold",
    )


def _feedback_with_strategy(
    name: str,
    *,
    cash_flow: float = 0.0,
    win_rate: float = 1.0,
    avg_slippage: float = 0.0,
    suggestion: str = "",
) -> EvaluationFeedback:
    return EvaluationFeedback(
        timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
        period=timedelta(minutes=5),
        strategy_adjustments={
            name: StrategyFeedback(
                cash_flow=cash_flow,
                win_rate=win_rate,
                avg_slippage=avg_slippage,
                suggestion=suggestion,
            ),
        },
        risk_adjustments=_empty_risk_feedback(),
        connector_adjustments={},
    )


# ---------------------------------------------------------------------------
# 1. Cross-platform arbitrage detection
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cross_platform_arb_detection() -> None:
    """PM ask 0.52, Kalshi bid 0.58 on the same outcome => arb (>2% spread)."""
    strategy = ArbitrageStrategy(
        min_spread=Decimal("0.02"),
        max_position_size=Decimal("100"),
    )

    pm = _price_update("polymarket", bid=Decimal("0.50"), ask=Decimal("0.52"))
    kalshi = _price_update("kalshi", bid=Decimal("0.58"), ask=Decimal("0.60"))

    assert await strategy.on_price_update(pm) is None
    orders = await strategy.on_price_update(kalshi)

    assert orders is not None
    assert len(orders) == 2

    sides = {(o.platform, o.side, o.price) for o in orders}
    assert ("polymarket", "buy", Decimal("0.52")) in sides
    assert ("kalshi", "sell", Decimal("0.58")) in sides


@pytest.mark.asyncio
async def test_cross_platform_no_arb_when_spread_too_small() -> None:
    """PM ask 0.52, Kalshi bid 0.53 ~= 1.9% normalized spread < 2% threshold."""
    strategy = ArbitrageStrategy(
        min_spread=Decimal("0.02"),
        max_position_size=Decimal("100"),
    )

    pm = _price_update("polymarket", bid=Decimal("0.51"), ask=Decimal("0.52"))
    kalshi = _price_update("kalshi", bid=Decimal("0.53"), ask=Decimal("0.54"))

    assert await strategy.on_price_update(pm) is None
    assert await strategy.on_price_update(kalshi) is None


@pytest.mark.asyncio
async def test_cross_platform_paired_correlation_id() -> None:
    """Both emitted orders share a correlation_id recorded in paired_orders."""
    strategy = ArbitrageStrategy(
        min_spread=Decimal("0.02"),
        max_position_size=Decimal("100"),
    )

    pm = _price_update("polymarket", bid=Decimal("0.50"), ask=Decimal("0.52"))
    kalshi = _price_update("kalshi", bid=Decimal("0.58"), ask=Decimal("0.60"))

    await strategy.on_price_update(pm)
    orders = await strategy.on_price_update(kalshi)
    assert orders is not None

    paired = strategy.get_paired_orders()
    assert len(paired) == 1
    pair = paired[0]

    assert isinstance(pair, ArbitragePairOrders)
    assert len(pair.orders) == 2
    # Both orders carry the same correlation_id prefix.
    assert all(o.order_id.startswith(pair.correlation_id) for o in pair.orders)
    # And the orders returned from the call are exactly the tracked pair.
    assert set(orders) == set(pair.orders)


# ---------------------------------------------------------------------------
# 2. Subset pricing violations (on_correlation_found)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_subset_violation_detection() -> None:
    """Subset priced above superset => buy superset, sell subset."""
    strategy = ArbitrageStrategy(
        min_spread=Decimal("0.02"),
        max_position_size=Decimal("100"),
    )

    market_a = _market("polymarket", "subset-market", yes_price=Decimal("0.70"))
    market_b = _market("kalshi", "superset-market", yes_price=Decimal("0.50"))

    pair = CorrelationPair(
        market_a=market_a,
        market_b=market_b,
        similarity_score=0.9,
        relation_type="subset",
        relation_detail="A is a subset of B",
        arbitrage_opportunity=Decimal("0.20"),
    )

    orders = await strategy.on_correlation_found(pair)
    assert orders is not None
    assert len(orders) == 2

    by_side = {(o.platform, o.market_id, o.side, o.price) for o in orders}
    # Buy superset B (cheap) @ 0.50 on kalshi
    assert (
        "kalshi",
        "superset-market",
        "buy",
        Decimal("0.50"),
    ) in by_side
    # Sell subset A (expensive) @ 0.70 on polymarket
    assert (
        "polymarket",
        "subset-market",
        "sell",
        Decimal("0.70"),
    ) in by_side


@pytest.mark.asyncio
async def test_subset_no_action_when_prices_consistent() -> None:
    """A=0.50, B=0.60 is logically consistent; no orders."""
    strategy = ArbitrageStrategy(min_spread=Decimal("0.02"))

    market_a = _market("polymarket", "subset-market", yes_price=Decimal("0.50"))
    market_b = _market("kalshi", "superset-market", yes_price=Decimal("0.60"))

    pair = CorrelationPair(
        market_a=market_a,
        market_b=market_b,
        similarity_score=0.9,
        relation_type="subset",
        relation_detail="A is a subset of B",
        arbitrage_opportunity=None,
    )

    assert await strategy.on_correlation_found(pair) is None


@pytest.mark.asyncio
async def test_subset_no_action_for_independent_relation() -> None:
    """Non-subset relation types short-circuit, regardless of prices."""
    strategy = ArbitrageStrategy()

    market_a = _market("polymarket", "a", yes_price=Decimal("0.90"))
    market_b = _market("kalshi", "b", yes_price=Decimal("0.10"))

    pair = CorrelationPair(
        market_a=market_a,
        market_b=market_b,
        similarity_score=0.5,
        relation_type="independent",
        relation_detail="unrelated",
        arbitrage_opportunity=None,
    )

    assert await strategy.on_correlation_found(pair) is None


@pytest.mark.asyncio
async def test_superset_relation_normalized_to_subset() -> None:
    """A ``superset`` pair is equivalent to its swapped ``subset``.

    CP10's detector picks ``relation_type`` based on the lexicographic order
    of market IDs, so the same logical subset/superset violation may arrive
    either way. The strategy must normalize so it emits the SAME orders
    regardless of which side the detector tagged as the subset.

    Construction:

    * ``market_a`` (on kalshi) is the SUPERSET, priced at 0.50.
    * ``market_b`` (on polymarket) is the SUBSET, priced at 0.70.
    * ``relation_type="superset"`` — meaning "A is a superset of B".

    After normalization, we expect:

    * Buy A (superset, cheap) @ 0.50 on kalshi
    * Sell B (subset, expensive) @ 0.70 on polymarket
    """
    strategy = ArbitrageStrategy(
        min_spread=Decimal("0.02"),
        max_position_size=Decimal("100"),
    )

    # Note: market_a is tagged as the SUPERSET here.
    market_a = _market(
        "kalshi", "superset-market", yes_price=Decimal("0.50")
    )
    market_b = _market(
        "polymarket", "subset-market", yes_price=Decimal("0.70")
    )

    pair = CorrelationPair(
        market_a=market_a,
        market_b=market_b,
        similarity_score=0.9,
        relation_type="superset",
        relation_detail="A is a superset of B",
        arbitrage_opportunity=Decimal("0.20"),
    )

    orders = await strategy.on_correlation_found(pair)
    assert orders is not None
    assert len(orders) == 2

    by_side = {(o.platform, o.market_id, o.side, o.price) for o in orders}
    # Buy the cheap superset A @ 0.50 on kalshi
    assert (
        "kalshi",
        "superset-market",
        "buy",
        Decimal("0.50"),
    ) in by_side
    # Sell the expensive subset B @ 0.70 on polymarket
    assert (
        "polymarket",
        "subset-market",
        "sell",
        Decimal("0.70"),
    ) in by_side


@pytest.mark.asyncio
async def test_subset_paired_correlation_id() -> None:
    """Subset arbitrage also records a paired order entry with a shared id."""
    strategy = ArbitrageStrategy(min_spread=Decimal("0.02"))

    market_a = _market("polymarket", "subset-market", yes_price=Decimal("0.70"))
    market_b = _market("kalshi", "superset-market", yes_price=Decimal("0.50"))

    pair = CorrelationPair(
        market_a=market_a,
        market_b=market_b,
        similarity_score=0.9,
        relation_type="subset",
        relation_detail="A is a subset of B",
        arbitrage_opportunity=Decimal("0.20"),
    )

    orders = await strategy.on_correlation_found(pair)
    assert orders is not None

    paired = strategy.get_paired_orders()
    assert len(paired) == 1
    assert all(
        o.order_id.startswith(paired[0].correlation_id) for o in paired[0].orders
    )
    assert set(paired[0].orders) == set(orders)


# ---------------------------------------------------------------------------
# 3. Feedback-driven threshold adjustment
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_feedback_raises_spread_on_poor_performance() -> None:
    """win_rate < 0.4 AND cash_flow < 0 => min_spread *= 1.5."""
    strategy = ArbitrageStrategy(min_spread=Decimal("0.02"))
    feedback = _feedback_with_strategy(
        strategy.name,
        cash_flow=-100.0,
        win_rate=0.2,
        avg_slippage=0.0,
    )

    await strategy.on_feedback(feedback)

    assert strategy.current_min_spread() == Decimal("0.03")


@pytest.mark.asyncio
async def test_feedback_raises_spread_on_high_slippage() -> None:
    """avg_slippage > min_spread / 2 => min_spread += avg_slippage."""
    strategy = ArbitrageStrategy(min_spread=Decimal("0.02"))
    feedback = _feedback_with_strategy(
        strategy.name,
        cash_flow=50.0,
        win_rate=0.8,
        avg_slippage=0.02,
    )

    await strategy.on_feedback(feedback)

    assert strategy.current_min_spread() == Decimal("0.04")


@pytest.mark.asyncio
async def test_feedback_respects_ceiling_guardrail() -> None:
    """A huge poor-performance bump is clamped to the 0.20 ceiling."""
    strategy = ArbitrageStrategy(min_spread=Decimal("0.15"))
    feedback = _feedback_with_strategy(
        strategy.name,
        cash_flow=-500.0,
        win_rate=0.1,
        avg_slippage=0.0,
    )

    await strategy.on_feedback(feedback)

    # 0.15 * 1.5 = 0.225 -> clamped to 0.20
    assert strategy.current_min_spread() == Decimal("0.20")


@pytest.mark.asyncio
async def test_feedback_no_op_for_irrelevant_strategy_name() -> None:
    """Feedback that does not mention ``arbitrage`` leaves state untouched."""
    strategy = ArbitrageStrategy(min_spread=Decimal("0.02"))
    feedback = _feedback_with_strategy(
        "other_strategy",
        cash_flow=-100.0,
        win_rate=0.1,
        avg_slippage=0.5,
    )

    await strategy.on_feedback(feedback)

    assert strategy.current_min_spread() == Decimal("0.02")


# ---------------------------------------------------------------------------
# 4. Configurable max_position_size + Protocol compatibility
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_max_position_size_respected() -> None:
    """Every emitted order — cross-platform or subset — uses max_position_size."""
    strategy = ArbitrageStrategy(
        min_spread=Decimal("0.02"),
        max_position_size=Decimal("42"),
    )

    pm = _price_update("polymarket", bid=Decimal("0.50"), ask=Decimal("0.52"))
    kalshi = _price_update("kalshi", bid=Decimal("0.58"), ask=Decimal("0.60"))

    await strategy.on_price_update(pm)
    cross = await strategy.on_price_update(kalshi)
    assert cross is not None
    assert all(o.size == Decimal("42") for o in cross)

    market_a = _market("polymarket", "a", yes_price=Decimal("0.70"))
    market_b = _market("kalshi", "b", yes_price=Decimal("0.50"))
    subset_pair = CorrelationPair(
        market_a=market_a,
        market_b=market_b,
        similarity_score=0.9,
        relation_type="subset",
        relation_detail="",
        arbitrage_opportunity=None,
    )
    subset_orders = await strategy.on_correlation_found(subset_pair)
    assert subset_orders is not None
    assert all(o.size == Decimal("42") for o in subset_orders)


def test_protocol_compat() -> None:
    """ArbitrageStrategy satisfies StrategyProtocol at runtime."""
    strategy = ArbitrageStrategy()
    assert isinstance(strategy, StrategyProtocol)


# ---------------------------------------------------------------------------
# 5. Opportunity deduplication (review-loop f9 round 2)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_arbitrage_does_not_re_emit_same_cross_platform_opportunity() -> None:
    """Review-loop fix f9 (round 2): a cross-platform opportunity must
    only emit one paired order while it remains outstanding.

    Construction: feed the same PM/Kalshi pair into the strategy twice
    in a row. The first call must emit the order pair; the second call
    must return ``None`` (or an empty list) because the opportunity is
    still tracked.
    """
    strategy = ArbitrageStrategy(
        min_spread=Decimal("0.02"),
        max_position_size=Decimal("100"),
    )

    pm = _price_update("polymarket", bid=Decimal("0.50"), ask=Decimal("0.52"))
    kalshi = _price_update("kalshi", bid=Decimal("0.58"), ask=Decimal("0.60"))

    # Seed both caches with one tick each.
    await strategy.on_price_update(pm)
    first = await strategy.on_price_update(kalshi)
    assert first is not None
    assert len(first) == 2

    # Same opportunity, fresh tick — must NOT re-emit.
    second = await strategy.on_price_update(kalshi)
    assert second is None or len(second) == 0
    # Paired-orders ledger only contains the first emission.
    assert len(strategy.get_paired_orders()) == 1


@pytest.mark.asyncio
async def test_arbitrage_does_not_re_emit_same_correlation_pair() -> None:
    """Calling ``on_correlation_found`` twice with the same pair must
    only emit orders once."""
    strategy = ArbitrageStrategy(min_spread=Decimal("0.02"))

    market_a = _market("polymarket", "subset-market", yes_price=Decimal("0.70"))
    market_b = _market("kalshi", "superset-market", yes_price=Decimal("0.50"))

    pair = CorrelationPair(
        market_a=market_a,
        market_b=market_b,
        similarity_score=0.9,
        relation_type="subset",
        relation_detail="A is a subset of B",
        arbitrage_opportunity=Decimal("0.20"),
    )

    first = await strategy.on_correlation_found(pair)
    assert first is not None and len(first) == 2

    second = await strategy.on_correlation_found(pair)
    assert second is None or len(second) == 0
    assert len(strategy.get_paired_orders()) == 1


@pytest.mark.asyncio
async def test_arbitrage_different_opportunities_are_tracked_separately() -> None:
    """Two distinct cross-platform opportunities must each emit one
    paired order — deduplication is per-opportunity, not global."""
    strategy = ArbitrageStrategy(
        min_spread=Decimal("0.02"),
        max_position_size=Decimal("100"),
    )

    pm_a = _price_update(
        "polymarket",
        bid=Decimal("0.50"),
        ask=Decimal("0.52"),
        market_id="m-a",
        outcome_id="yes-a",
    )
    kalshi_a = _price_update(
        "kalshi",
        bid=Decimal("0.58"),
        ask=Decimal("0.60"),
        market_id="m-a",
        outcome_id="yes-a",
    )
    pm_b = _price_update(
        "polymarket",
        bid=Decimal("0.30"),
        ask=Decimal("0.32"),
        market_id="m-b",
        outcome_id="yes-b",
    )
    kalshi_b = _price_update(
        "kalshi",
        bid=Decimal("0.40"),
        ask=Decimal("0.42"),
        market_id="m-b",
        outcome_id="yes-b",
    )

    await strategy.on_price_update(pm_a)
    first = await strategy.on_price_update(kalshi_a)
    assert first is not None
    assert len(first) == 2

    await strategy.on_price_update(pm_b)
    second = await strategy.on_price_update(kalshi_b)
    assert second is not None
    assert len(second) == 2

    # Both opportunities tracked, neither de-duplicated.
    assert len(strategy.get_paired_orders()) == 2
