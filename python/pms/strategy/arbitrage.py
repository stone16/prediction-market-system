"""Arbitrage strategy — the first concrete ``StrategyProtocol`` (CP07).

Two detection paths:

1. **Cross-platform**: when the same outcome is priced differently on two
   connected platforms (Polymarket vs. Kalshi), buy the underpriced side and
   sell the overpriced side. A normalized (relative) spread gate keeps the
   strategy away from noise.
2. **Correlation-based (subset)**: when a ``CorrelationPair`` with
   ``relation_type == "subset"`` prices the subset market HIGHER than its
   superset — violating the ``P(A⊂B) ≤ P(B)`` invariant — buy the cheap
   superset and sell the expensive subset.

Both code paths emit pairs of orders sharing a generated ``correlation_id``
so that the CP08 executor can coordinate atomic-ish execution and roll back
if one side fails.

Thresholds are self-tuning via ``on_feedback``: persistent losses tighten
the spread gate, and observed slippage bumps it upward. Guardrails clamp
the gate to the ``[0.005, 0.20]`` band so pathological feedback cannot
silently disable (or over-trigger) the strategy.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from decimal import Decimal
from typing import Sequence

from pms.models import (
    CorrelationPair,
    EvaluationFeedback,
    Market,
    Order,
    PriceUpdate,
)

logger = logging.getLogger(__name__)

# Guardrails for the adaptive ``min_spread`` threshold. Chosen to keep the
# strategy in a usable band even if feedback is pathological — 50 bps floor
# (avoids firing on noise) and 20 % ceiling (avoids permanently muting it).
_MIN_SPREAD_FLOOR = Decimal("0.005")
_MIN_SPREAD_CEILING = Decimal("0.20")


@dataclass(frozen=True)
class ArbitragePairOrders:
    """A pair of orders forming one atomic arbitrage position.

    Both orders share ``correlation_id``. CP08's executor will use this to
    coordinate atomic-ish execution (submit both; if either fails, cancel
    the other).
    """

    correlation_id: str
    orders: tuple[Order, ...]


class ArbitrageStrategy:
    """Detects cross-market and intra-market arbitrage opportunities.

    For CP07, cross-platform matching is intentionally simple: two price
    updates from different platforms with the same ``outcome_id`` are
    treated as the same logical event. Real semantic matching across
    platforms is deferred to CP10's correlation detector.
    """

    name: str = "arbitrage"

    def __init__(
        self,
        min_spread: Decimal = Decimal("0.02"),
        max_position_size: Decimal = Decimal("100"),
        platforms: Sequence[str] = ("polymarket", "kalshi"),
    ) -> None:
        self._min_spread: Decimal = min_spread
        self._max_position_size: Decimal = max_position_size
        self._platforms: tuple[str, ...] = tuple(platforms)
        # Cache of the latest price update per (platform, market_id, outcome_id)
        # so we can look across platforms for the same outcome on every tick.
        self._latest_prices: dict[tuple[str, str, str], PriceUpdate] = {}
        # Paired-orders ledger for observability / downstream atomic execution.
        self._paired_orders: list[ArbitragePairOrders] = []
        # Outstanding-opportunity tracker (review-loop fix f9 round 2).
        # Each opportunity has a stable canonical key derived from the
        # (platform, market_id, outcome_id) of both legs (or both
        # markets, for correlation pairs). The strategy refuses to emit
        # a fresh order pair while an opportunity key is in this set.
        # ``clear_opportunity`` is the public API for callers (executor,
        # post-fill cleanup) to release a tracked opportunity once the
        # paired orders have resolved.
        self._outstanding_opportunities: set[str] = set()
        self._correlation_id_to_key: dict[str, str] = {}

    # ------------------------------------------------------------------
    # StrategyProtocol hooks
    # ------------------------------------------------------------------

    async def on_price_update(self, update: PriceUpdate) -> list[Order] | None:
        """Detect cross-platform spread on the same logical outcome.

        Any other-platform cache entry with the same ``outcome_id`` is a
        candidate. For each candidate, compute a normalized spread and emit
        a buy/sell pair if it exceeds ``min_spread``.
        """
        key = (update.platform, update.market_id, update.outcome_id)
        self._latest_prices[key] = update

        # Cross-platform ``outcome_id`` equality is the **test-only** path:
        # Polymarket emits ERC-1155 token IDs and Kalshi emits
        # "<ticker>-YES/NO", so real updates from the two platforms never
        # share an ``outcome_id`` and this path is a no-op in production.
        # The production cross-platform arbitrage path is the CP10
        # ``CorrelationDetector`` → :meth:`on_correlation_found` hook,
        # which is now wired into ``TradingPipeline`` (review-loop fix f2
        # round 2). Removing the direct path is a post-v1 cleanup; the
        # unit tests still pin the existing behaviour and the path is
        # harmless because it is unreachable on real data.
        candidates: list[PriceUpdate] = [
            other
            for other_key, other in self._latest_prices.items()
            if other_key[0] != update.platform
            and other_key[2] == update.outcome_id
        ]

        # Review-loop fix f9 (round 2): de-duplicate by canonical
        # opportunity key. The same logical opportunity (e.g. PM YES vs
        # Kalshi YES on the same outcome) must only emit one paired
        # order pair while it remains outstanding. Callers release the
        # opportunity via :meth:`clear_opportunity` when the paired
        # orders have resolved (filled/cancelled).
        orders: list[Order] = []
        for other in candidates:
            spread = _compute_spread(update, other)
            if spread is None or spread < self._min_spread:
                continue

            # Pick the cheap/expensive sides. We buy the leg with the lower
            # ask (cheap) and sell the leg with the higher bid (expensive).
            if update.ask < other.bid:
                cheap, expensive = update, other
            else:
                cheap, expensive = other, update

            opportunity_key = self._cross_platform_opportunity_key(
                cheap, expensive
            )
            if opportunity_key in self._outstanding_opportunities:
                continue

            pair = self._make_cross_platform_pair(cheap, expensive)
            self._outstanding_opportunities.add(opportunity_key)
            self._correlation_id_to_key[pair.correlation_id] = opportunity_key
            self._paired_orders.append(pair)
            orders.extend(pair.orders)

        return orders if orders else None

    async def on_correlation_found(
        self, pair: CorrelationPair
    ) -> list[Order] | None:
        """Detect subset pricing violations (P(A⊂B) > P(B)).

        Market A is treated as the subset, B as the superset. If A's
        YES-leg price is materially higher than B's YES-leg price (by more
        than ``min_spread``), we buy B and sell A to capture the edge.

        CP10's correlation detector assigns ``relation_type`` based on the
        lexicographic order of the two market IDs, so the same logical
        subset/superset pair may arrive with ``relation_type="superset"``
        (meaning "A is a superset of B") depending on which ID sorted
        first. We normalize by swapping ``market_a`` / ``market_b`` so the
        rest of this method can assume the canonical "A is the subset"
        orientation. Without this normalization, the strategy would
        silently miss half of its opportunities in a way driven purely by
        market ID ordering.
        """
        if pair.relation_type == "superset":
            pair = CorrelationPair(
                market_a=pair.market_b,
                market_b=pair.market_a,
                similarity_score=pair.similarity_score,
                relation_type="subset",
                relation_detail=pair.relation_detail,
                arbitrage_opportunity=pair.arbitrage_opportunity,
            )

        if pair.relation_type != "subset":
            return None

        a_yes = _first_outcome_price(pair.market_a)
        b_yes = _first_outcome_price(pair.market_b)
        if a_yes is None or b_yes is None:
            return None

        # Need A priced meaningfully higher than B to trade; otherwise the
        # spread is within noise and would likely be eaten by fees.
        if a_yes <= b_yes + self._min_spread:
            return None

        # Review-loop fix f9 round 2: de-duplicate identical correlation
        # pairs. ``CorrelationDetector`` can re-emit the same pair on
        # every cycle, so without tracking we would flood the executor
        # with stale order pairs.
        opportunity_key = self._correlation_opportunity_key(pair)
        if opportunity_key in self._outstanding_opportunities:
            return None

        correlation_id = str(uuid.uuid4())
        buy_b = Order(
            order_id=f"{correlation_id}-buy-b",
            platform=pair.market_b.platform,
            market_id=pair.market_b.market_id,
            outcome_id=pair.market_b.outcomes[0].outcome_id,
            side="buy",
            price=b_yes,
            size=self._max_position_size,
            order_type="limit",
        )
        sell_a = Order(
            order_id=f"{correlation_id}-sell-a",
            platform=pair.market_a.platform,
            market_id=pair.market_a.market_id,
            outcome_id=pair.market_a.outcomes[0].outcome_id,
            side="sell",
            price=a_yes,
            size=self._max_position_size,
            order_type="limit",
        )

        arb_pair = ArbitragePairOrders(
            correlation_id=correlation_id,
            orders=(buy_b, sell_a),
        )
        self._outstanding_opportunities.add(opportunity_key)
        self._correlation_id_to_key[correlation_id] = opportunity_key
        self._paired_orders.append(arb_pair)
        return list(arb_pair.orders)

    async def on_feedback(self, feedback: EvaluationFeedback) -> None:
        """Adjust ``min_spread`` based on the latest performance summary.

        Rules (applied in priority order — the first matching rule wins):

        1. If ``win_rate < 0.4`` and ``cash_flow < 0``: multiply
           ``min_spread`` by 1.5 (become more selective to cut losing
           trades). ``cash_flow`` is the v1 signed-cash-flow proxy from
           the metrics collector — see :class:`pms.models.PnLReport` for
           why it's not labelled "pnl" in v1 (review-loop fix f11).
        2. Else if observed ``avg_slippage`` exceeds ``min_spread / 2``:
           raise ``min_spread`` by the observed slippage (restore edge
           after costs).

        The result is clamped to ``[_MIN_SPREAD_FLOOR, _MIN_SPREAD_CEILING]``
        so runaway feedback cannot disable or over-trigger the strategy.
        """
        my_feedback = feedback.strategy_adjustments.get(self.name)
        if my_feedback is None:
            return

        new_spread: Decimal = self._min_spread

        if my_feedback.win_rate < 0.4 and my_feedback.cash_flow < 0:
            new_spread = self._min_spread * Decimal("1.5")
        else:
            # ``avg_slippage`` is a float on the wire; convert via ``str``
            # to preserve the authored precision and avoid binary artifacts.
            slippage = Decimal(str(my_feedback.avg_slippage))
            if slippage > self._min_spread / Decimal("2"):
                new_spread = self._min_spread + slippage

        clamped = max(_MIN_SPREAD_FLOOR, min(_MIN_SPREAD_CEILING, new_spread))
        if clamped != self._min_spread:
            logger.info(
                "arbitrage min_spread adjusted: %s -> %s",
                self._min_spread,
                clamped,
            )
            self._min_spread = clamped

    # ------------------------------------------------------------------
    # Observability (not in Protocol — used by tests / downstream tooling)
    # ------------------------------------------------------------------

    def get_paired_orders(self) -> list[ArbitragePairOrders]:
        """Return a snapshot of every paired arbitrage order emitted so far."""
        return list(self._paired_orders)

    def current_min_spread(self) -> Decimal:
        """Return the current adaptive ``min_spread`` threshold."""
        return self._min_spread

    def outstanding_opportunity_keys(self) -> set[str]:
        """Return a snapshot of currently tracked opportunity keys.

        Useful for tests and observability tooling. The strategy refuses
        to re-emit a paired order while its opportunity key is in this
        set (review-loop fix f9 round 2).
        """
        return set(self._outstanding_opportunities)

    def clear_opportunity(self, correlation_id: str) -> None:
        """Release the opportunity associated with ``correlation_id``.

        Callers should invoke this once the paired order with the given
        ``correlation_id`` has resolved (filled, cancelled, expired).
        After clearing, the strategy is free to emit a fresh pair if
        the underlying spread still meets ``min_spread``.

        Unknown ``correlation_id`` values are silently ignored so the
        method is safe to call from cleanup handlers without extra
        bookkeeping on the caller side.
        """
        opportunity_key = self._correlation_id_to_key.pop(correlation_id, None)
        if opportunity_key is not None:
            self._outstanding_opportunities.discard(opportunity_key)

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    @staticmethod
    def _cross_platform_opportunity_key(
        cheap: PriceUpdate, expensive: PriceUpdate
    ) -> str:
        """Build a canonical key for a cross-platform opportunity.

        The key is independent of which leg is currently the cheap side
        — sorting by ``(platform, market_id, outcome_id)`` ensures the
        same key surfaces if the spread later flips.
        """
        a = (cheap.platform, cheap.market_id, cheap.outcome_id)
        b = (expensive.platform, expensive.market_id, expensive.outcome_id)
        ordered = sorted([a, b])
        return f"cross::{ordered[0]}::{ordered[1]}"

    @staticmethod
    def _correlation_opportunity_key(pair: CorrelationPair) -> str:
        """Build a canonical key for a correlation-based opportunity.

        Sorted by ``(platform, market_id)`` of both legs so the key is
        the same regardless of which side the detector tagged as
        ``market_a``.
        """
        a = (pair.market_a.platform, pair.market_a.market_id)
        b = (pair.market_b.platform, pair.market_b.market_id)
        ordered = sorted([a, b])
        return f"correlation::{ordered[0]}::{ordered[1]}"

    def _make_cross_platform_pair(
        self, cheap: PriceUpdate, expensive: PriceUpdate
    ) -> ArbitragePairOrders:
        correlation_id = str(uuid.uuid4())
        buy_cheap = Order(
            order_id=f"{correlation_id}-buy",
            platform=cheap.platform,
            market_id=cheap.market_id,
            outcome_id=cheap.outcome_id,
            side="buy",
            price=cheap.ask,
            size=self._max_position_size,
            order_type="limit",
        )
        sell_expensive = Order(
            order_id=f"{correlation_id}-sell",
            platform=expensive.platform,
            market_id=expensive.market_id,
            outcome_id=expensive.outcome_id,
            side="sell",
            price=expensive.bid,
            size=self._max_position_size,
            order_type="limit",
        )
        return ArbitragePairOrders(
            correlation_id=correlation_id,
            orders=(buy_cheap, sell_expensive),
        )


def _compute_spread(
    update_a: PriceUpdate, update_b: PriceUpdate
) -> Decimal | None:
    """Compute a normalized (relative) spread between two price updates.

    We take the best executable prices: the lower of the two asks (cheapest
    place to buy) and the higher of the two bids (richest place to sell).
    Their absolute difference is the gross edge; we normalize by the average
    mid-price so ``min_spread`` can be expressed as a percentage regardless
    of absolute market price.

    Returns ``None`` if the prices cross unfavorably (no edge) or if the
    average mid-price is non-positive (degenerate input).
    """
    low = min(update_a.ask, update_b.ask)
    high = max(update_a.bid, update_b.bid)
    gross = high - low
    if gross <= Decimal("0"):
        return None

    mid_a = (update_a.bid + update_a.ask) / Decimal("2")
    mid_b = (update_b.bid + update_b.ask) / Decimal("2")
    avg_mid = (mid_a + mid_b) / Decimal("2")
    if avg_mid <= Decimal("0"):
        return None

    return gross / avg_mid


def _first_outcome_price(market: Market) -> Decimal | None:
    """Return the first outcome's price, or ``None`` if the market has no outcomes."""
    if not market.outcomes:
        return None
    return market.outcomes[0].price
