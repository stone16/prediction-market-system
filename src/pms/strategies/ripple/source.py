"""Fixture-backed observation source for the ripple strategy."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from pms.core.enums import TimeInForce
from pms.core.models import BookSide, Outcome, Venue
from pms.strategies.intents import StrategyContext, StrategyObservation


@dataclass(frozen=True, slots=True)
class RippleObservationFixture:
    observation_id: str
    market_id: str
    title: str
    thesis: str
    probability_estimate: float
    expected_edge: float
    confidence: float
    token_id: str
    limit_price: float
    notional_usdc: float
    expected_price: float
    max_slippage_bps: int
    evidence_refs: tuple[str, ...]
    contradiction_refs: tuple[str, ...] = ()
    venue: Venue = "polymarket"
    side: BookSide = "BUY"
    outcome: Outcome = "YES"
    time_in_force: TimeInForce = TimeInForce.GTC
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _require_non_empty(self.observation_id, "observation_id")
        _require_non_empty(self.market_id, "market_id")
        _require_non_empty(self.title, "title")
        _require_non_empty(self.thesis, "thesis")
        _require_non_empty(self.token_id, "token_id")
        if not 0.0 <= self.probability_estimate <= 1.0:
            msg = "probability_estimate must satisfy 0.0 <= value <= 1.0"
            raise ValueError(msg)
        if not 0.0 <= self.confidence <= 1.0:
            msg = "confidence must satisfy 0.0 <= value <= 1.0"
            raise ValueError(msg)
        if self.notional_usdc <= 0.0:
            msg = "notional_usdc must be > 0.0"
            raise ValueError(msg)
        if self.max_slippage_bps < 0:
            msg = "max_slippage_bps must be >= 0"
            raise ValueError(msg)

    def payload(self) -> Mapping[str, Any]:
        return {
            "market_id": self.market_id,
            "title": self.title,
            "thesis": self.thesis,
            "probability_estimate": self.probability_estimate,
            "expected_edge": self.expected_edge,
            "confidence": self.confidence,
            "token_id": self.token_id,
            "venue": self.venue,
            "side": self.side,
            "outcome": self.outcome,
            "limit_price": self.limit_price,
            "notional_usdc": self.notional_usdc,
            "expected_price": self.expected_price,
            "max_slippage_bps": self.max_slippage_bps,
            "time_in_force": self.time_in_force,
            "contradiction_refs": self.contradiction_refs,
            "metadata": dict(self.metadata),
        }


@dataclass(frozen=True, slots=True)
class RippleObservationSource:
    fixtures: Sequence[RippleObservationFixture]

    async def observe(self, context: StrategyContext) -> Sequence[StrategyObservation]:
        return tuple(
            StrategyObservation(
                observation_id=fixture.observation_id,
                strategy_id=context.strategy_id,
                strategy_version_id=context.strategy_version_id,
                source="ripple-fixture",
                observed_at=context.as_of,
                summary=fixture.thesis,
                payload=fixture.payload(),
                evidence_refs=fixture.evidence_refs,
            )
            for fixture in self.fixtures
        )


def _require_non_empty(value: str, field_name: str) -> None:
    if not value:
        msg = f"{field_name} must be non-empty"
        raise ValueError(msg)
