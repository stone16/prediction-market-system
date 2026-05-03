"""Observation source for the H1 favorite-longshot bias strategy."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from pms.core.enums import TimeInForce
from pms.core.models import BookSide, Outcome, Portfolio, Venue
from pms.strategies.flb.evaluator import DEFAULT_MIN_EXPECTED_EDGE
from pms.strategies.intents import StrategyContext, StrategyObservation


FLB_RESEARCH_REF = "research:h1-flb-strategy#h1"
LIVE_FLB_SOURCE = "live_flb_market_source"
LONGSHOT_YES_THRESHOLD = 0.10
FAVORITE_YES_THRESHOLD = 0.90
DEFAULT_FLB_CONFIDENCE = 0.65


class FlbMarketSnapshotReader(Protocol):
    async def latest(
        self,
        market_id: str,
        *,
        as_of: datetime,
    ) -> FlbMarketSnapshot | None: ...


class FlbPositionSizer(Protocol):
    def size(
        self,
        *,
        prob: float,
        market_price: float,
        portfolio: Portfolio,
    ) -> float: ...


@dataclass(frozen=True, slots=True)
class FlbMarketSnapshot:
    market_id: str
    title: str
    yes_token_id: str
    no_token_id: str
    yes_price: float
    observed_at: datetime
    yes_best_ask: float | None = None
    no_best_ask: float | None = None
    resolves_at: datetime | None = None
    venue: Venue = "polymarket"

    def __post_init__(self) -> None:
        _require_non_empty(self.market_id, "market_id")
        _require_non_empty(self.title, "title")
        _require_non_empty(self.yes_token_id, "yes_token_id")
        _require_non_empty(self.no_token_id, "no_token_id")
        _require_open_probability(self.yes_price, "yes_price")
        if self.yes_best_ask is not None:
            _require_open_probability(self.yes_best_ask, "yes_best_ask")
        if self.no_best_ask is not None:
            _require_open_probability(self.no_best_ask, "no_best_ask")


@dataclass(frozen=True, slots=True)
class LiveFlbSource:
    """Market-price source for H1 FLB signals.

    This source implements only H1 bucket semantics from the research brief. The
    edge estimate is a paper-soak placeholder until warehouse decile estimates
    replace it. H2 anchoring-lag/news replay remains out of scope until the H1
    historical data spine is proven viable.
    """

    market_ids: Sequence[str]
    market_reader: FlbMarketSnapshotReader
    position_sizer: FlbPositionSizer
    portfolio: Portfolio
    min_expected_edge: float = DEFAULT_MIN_EXPECTED_EDGE
    longshot_yes_threshold: float = LONGSHOT_YES_THRESHOLD
    favorite_yes_threshold: float = FAVORITE_YES_THRESHOLD
    confidence: float = DEFAULT_FLB_CONFIDENCE
    max_slippage_bps: int = 50
    time_in_force: TimeInForce = TimeInForce.GTC

    def __post_init__(self) -> None:
        if not self.market_ids:
            msg = "market_ids must not be empty"
            raise ValueError(msg)
        for market_id in self.market_ids:
            _require_non_empty(market_id, "market_id")
        if self.min_expected_edge <= 0.0:
            msg = "min_expected_edge must be > 0.0"
            raise ValueError(msg)
        _require_open_probability(self.longshot_yes_threshold, "longshot_yes_threshold")
        _require_open_probability(self.favorite_yes_threshold, "favorite_yes_threshold")
        if self.longshot_yes_threshold >= self.favorite_yes_threshold:
            msg = "longshot_yes_threshold must be below favorite_yes_threshold"
            raise ValueError(msg)
        if not 0.0 <= self.confidence <= 1.0:
            msg = "confidence must satisfy 0.0 <= confidence <= 1.0"
            raise ValueError(msg)
        if self.max_slippage_bps < 0:
            msg = "max_slippage_bps must be >= 0"
            raise ValueError(msg)

    async def observe(self, context: StrategyContext) -> Sequence[StrategyObservation]:
        observations: list[StrategyObservation] = []
        for market_id in self.market_ids:
            market = await self.market_reader.latest(market_id, as_of=context.as_of)
            if market is None or _is_resolved(context.as_of, market.resolves_at):
                continue
            observation = _observation_from_market(
                context=context,
                market=market,
                position_sizer=self.position_sizer,
                portfolio=self.portfolio,
                min_expected_edge=self.min_expected_edge,
                longshot_yes_threshold=self.longshot_yes_threshold,
                favorite_yes_threshold=self.favorite_yes_threshold,
                confidence=self.confidence,
                max_slippage_bps=self.max_slippage_bps,
                time_in_force=self.time_in_force,
            )
            if observation is not None:
                observations.append(observation)
        return tuple(observations)


@dataclass(frozen=True, slots=True)
class _FlbSignal:
    signal_name: str
    thesis: str
    token_id: str
    outcome: Outcome
    side: BookSide
    limit_price: float
    probability_estimate: float


def _observation_from_market(
    *,
    context: StrategyContext,
    market: FlbMarketSnapshot,
    position_sizer: FlbPositionSizer,
    portfolio: Portfolio,
    min_expected_edge: float,
    longshot_yes_threshold: float,
    favorite_yes_threshold: float,
    confidence: float,
    max_slippage_bps: int,
    time_in_force: TimeInForce,
) -> StrategyObservation | None:
    signal = _classify_market(
        market=market,
        min_expected_edge=min_expected_edge,
        longshot_yes_threshold=longshot_yes_threshold,
        favorite_yes_threshold=favorite_yes_threshold,
    )
    if signal is None:
        return None

    expected_edge = signal.probability_estimate - signal.limit_price
    notional_usdc = position_sizer.size(
        prob=signal.probability_estimate,
        market_price=signal.limit_price,
        portfolio=portfolio,
    )
    if notional_usdc <= 0.0:
        return None

    evidence_refs = _evidence_refs(market)
    metadata = {
        "source": LIVE_FLB_SOURCE,
        "h1_signal": signal.signal_name,
        "yes_price": market.yes_price,
        "yes_best_ask": market.yes_best_ask,
        "no_best_ask": market.no_best_ask,
        "observed_at": market.observed_at.isoformat(),
        "resolves_at": market.resolves_at.isoformat() if market.resolves_at else None,
        "longshot_yes_threshold": longshot_yes_threshold,
        "favorite_yes_threshold": favorite_yes_threshold,
        "min_expected_edge": min_expected_edge,
    }
    payload = {
        "market_id": market.market_id,
        "title": market.title,
        "thesis": signal.thesis,
        "probability_estimate": signal.probability_estimate,
        "expected_edge": expected_edge,
        "confidence": confidence,
        "token_id": signal.token_id,
        "venue": market.venue,
        "side": signal.side,
        "outcome": signal.outcome,
        "limit_price": signal.limit_price,
        "notional_usdc": notional_usdc,
        "expected_price": signal.probability_estimate,
        "max_slippage_bps": max_slippage_bps,
        "time_in_force": time_in_force,
        "contradiction_refs": (),
        "metadata": metadata,
    }
    return StrategyObservation(
        observation_id=f"live-flb-{market.market_id}-{context.as_of.isoformat()}",
        strategy_id=context.strategy_id,
        strategy_version_id=context.strategy_version_id,
        source=LIVE_FLB_SOURCE,
        observed_at=context.as_of,
        summary=signal.thesis,
        payload=payload,
        evidence_refs=evidence_refs,
    )


def _classify_market(
    *,
    market: FlbMarketSnapshot,
    min_expected_edge: float,
    longshot_yes_threshold: float,
    favorite_yes_threshold: float,
) -> _FlbSignal | None:
    if market.yes_price < longshot_yes_threshold:
        limit_price = _bounded_probability(
            market.no_best_ask if market.no_best_ask is not None else 1.0 - market.yes_price,
            "no_limit_price",
        )
        probability_estimate = _bounded_probability(
            limit_price + min_expected_edge,
            "no_probability_estimate",
        )
        return _FlbSignal(
            signal_name="longshot_yes_overpriced_buy_no",
            thesis=(
                "H1 FLB: low-YES longshot bucket is treated as overpriced; "
                "buy NO exposure."
            ),
            token_id=market.no_token_id,
            outcome="NO",
            side="BUY",
            limit_price=limit_price,
            probability_estimate=probability_estimate,
        )
    if market.yes_price > favorite_yes_threshold:
        limit_price = _bounded_probability(
            market.yes_best_ask if market.yes_best_ask is not None else market.yes_price,
            "yes_limit_price",
        )
        probability_estimate = _bounded_probability(
            limit_price + min_expected_edge,
            "yes_probability_estimate",
        )
        return _FlbSignal(
            signal_name="favorite_yes_underpriced_buy_yes",
            thesis=(
                "H1 FLB: high-YES favorite bucket is treated as underpriced; "
                "buy YES exposure."
            ),
            token_id=market.yes_token_id,
            outcome="YES",
            side="BUY",
            limit_price=limit_price,
            probability_estimate=probability_estimate,
        )
    return None


def _is_resolved(as_of: datetime, resolves_at: datetime | None) -> bool:
    return resolves_at is not None and resolves_at <= as_of


def _evidence_refs(market: FlbMarketSnapshot) -> tuple[str, ...]:
    market_ref = f"market_snapshot:{market.market_id}:{market.observed_at.isoformat()}"
    return (FLB_RESEARCH_REF, market_ref)


def _require_non_empty(value: str, field_name: str) -> None:
    if not value:
        msg = f"{field_name} must be non-empty"
        raise ValueError(msg)


def _require_open_probability(value: float, field_name: str) -> None:
    if value <= 0.0 or value >= 1.0 or value != value:
        msg = f"{field_name} must satisfy 0.0 < value < 1.0"
        raise ValueError(msg)


def _bounded_probability(value: float, field_name: str) -> float:
    if value != value:
        msg = f"{field_name} must not be NaN"
        raise ValueError(msg)
    return min(max(float(value), 0.0001), 0.9999)
