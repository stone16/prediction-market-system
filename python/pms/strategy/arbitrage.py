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

        candidates: list[PriceUpdate] = [
            other
            for other_key, other in self._latest_prices.items()
            if other_key[0] != update.platform
            and other_key[2] == update.outcome_id
        ]

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

            pair = self._make_cross_platform_pair(cheap, expensive)
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
        """
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
        self._paired_orders.append(arb_pair)
        return list(arb_pair.orders)

    async def on_feedback(self, feedback: EvaluationFeedback) -> None:
        """Adjust ``min_spread`` based on the latest performance summary.

        Rules (applied in priority order — the first matching rule wins):

        1. If ``win_rate < 0.4`` and ``pnl < 0``: multiply ``min_spread`` by
           1.5 (become more selective to cut losing trades).
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

        if my_feedback.win_rate < 0.4 and my_feedback.pnl < 0:
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

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

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
