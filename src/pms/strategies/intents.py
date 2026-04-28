"""Typed strategy-plugin intent value objects, not actuator commands."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal, TypeAlias

from pms.core.enums import TimeInForce
from pms.core.models import BookSide, Outcome, Venue


BasketExecutionPolicy: TypeAlias = Literal[
    "manual_review", "all_or_none", "sequential_with_hedge", "single_leg_use_trade_intent"
]

SUPPORTED_BASKET_POLICIES = frozenset({"manual_review", "all_or_none", "sequential_with_hedge"})


def _require_non_empty(value: str, field_name: str) -> None:
    if not value:
        msg = f"{field_name} must be non-empty"
        raise ValueError(msg)


def _require_strategy_identity(strategy_id: str, strategy_version_id: str) -> None:
    _require_non_empty(strategy_id, "strategy_id")
    _require_non_empty(strategy_version_id, "strategy_version_id")


def _require_probability(value: float, field_name: str) -> None:
    if value < 0.0 or value > 1.0:
        msg = f"{field_name} must satisfy 0.0 <= {field_name} <= 1.0"
        raise ValueError(msg)


def _require_open_probability(value: float, field_name: str) -> None:
    if value <= 0.0 or value >= 1.0:
        msg = f"{field_name} must satisfy 0.0 < {field_name} < 1.0"
        raise ValueError(msg)


@dataclass(frozen=True, slots=True)
class StrategyContext:
    strategy_id: str
    strategy_version_id: str
    as_of: datetime
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _require_strategy_identity(self.strategy_id, self.strategy_version_id)


@dataclass(frozen=True, slots=True)
class StrategyObservation:
    observation_id: str
    strategy_id: str
    strategy_version_id: str
    source: str
    observed_at: datetime
    summary: str
    payload: Mapping[str, Any] = field(default_factory=dict)
    evidence_refs: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        _require_non_empty(self.observation_id, "observation_id")
        _require_strategy_identity(self.strategy_id, self.strategy_version_id)
        _require_non_empty(self.source, "source")


@dataclass(frozen=True, slots=True)
class StrategyCandidate:
    candidate_id: str
    strategy_id: str
    strategy_version_id: str
    market_id: str
    title: str
    thesis: str
    probability_estimate: float
    expected_edge: float
    evidence_refs: tuple[str, ...]
    created_at: datetime
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _require_non_empty(self.candidate_id, "candidate_id")
        _require_strategy_identity(self.strategy_id, self.strategy_version_id)
        _require_non_empty(self.market_id, "market_id")
        _require_probability(self.probability_estimate, "probability_estimate")


@dataclass(frozen=True, slots=True)
class StrategyJudgement:
    judgement_id: str
    candidate_id: str
    strategy_id: str
    strategy_version_id: str
    approved: bool
    confidence: float
    rationale: str
    evidence_refs: tuple[str, ...]
    failure_reasons: tuple[str, ...]
    created_at: datetime

    def __post_init__(self) -> None:
        _require_non_empty(self.judgement_id, "judgement_id")
        _require_non_empty(self.candidate_id, "candidate_id")
        _require_strategy_identity(self.strategy_id, self.strategy_version_id)
        _require_probability(self.confidence, "confidence")
        if self.approved and self.failure_reasons:
            msg = "approved judgements must not include failure_reasons"
            raise ValueError(msg)
        if not self.approved and not self.failure_reasons:
            msg = "rejected judgements require failure_reasons"
            raise ValueError(msg)


@dataclass(frozen=True, slots=True)
class TradeIntent:
    intent_id: str
    strategy_id: str
    strategy_version_id: str
    candidate_id: str
    market_id: str
    token_id: str
    venue: Venue
    side: BookSide
    outcome: Outcome
    limit_price: float
    notional_usdc: float
    expected_price: float
    expected_edge: float
    max_slippage_bps: int
    time_in_force: TimeInForce
    evidence_refs: tuple[str, ...]
    created_at: datetime

    def __post_init__(self) -> None:
        _require_non_empty(self.intent_id, "intent_id")
        _require_strategy_identity(self.strategy_id, self.strategy_version_id)
        _require_non_empty(self.candidate_id, "candidate_id")
        _require_non_empty(self.market_id, "market_id")
        _require_non_empty(self.token_id, "token_id")
        _require_open_probability(self.limit_price, "limit_price")
        _require_open_probability(self.expected_price, "expected_price")
        if self.notional_usdc <= 0.0:
            msg = "notional_usdc must be > 0.0"
            raise ValueError(msg)
        if self.max_slippage_bps < 0:
            msg = "max_slippage_bps must be >= 0"
            raise ValueError(msg)


@dataclass(frozen=True, slots=True)
class BasketIntent:
    basket_id: str
    strategy_id: str
    strategy_version_id: str
    legs: tuple[TradeIntent, ...]
    execution_policy: BasketExecutionPolicy
    evidence_refs: tuple[str, ...]
    created_at: datetime

    def __post_init__(self) -> None:
        _require_non_empty(self.basket_id, "basket_id")
        _require_strategy_identity(self.strategy_id, self.strategy_version_id)
        if not self.legs:
            msg = "BasketIntent legs must not be empty"
            raise ValueError(msg)
        if len(self.legs) == 1:
            msg = "single_leg_use_trade_intent: single-leg baskets must use TradeIntent"
            raise ValueError(msg)
        if self.execution_policy not in SUPPORTED_BASKET_POLICIES:
            msg = f"execution_policy is unsupported: {self.execution_policy}"
            raise ValueError(msg)
        for leg in self.legs:
            if (
                leg.strategy_id != self.strategy_id
                or leg.strategy_version_id != self.strategy_version_id
            ):
                msg = "BasketIntent rejects mixed strategy identity"
                raise ValueError(msg)
