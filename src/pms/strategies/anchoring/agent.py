"""Deterministic H2 anchoring-lag agent that builds trade intents."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, cast

from pms.core.enums import TimeInForce
from pms.core.models import BookSide, Outcome, Venue
from pms.strategies.anchoring.evaluator import AnchoringEvidenceEvaluator
from pms.strategies.intents import (
    BasketIntent,
    StrategyCandidate,
    StrategyContext,
    StrategyJudgement,
    TradeIntent,
)


@dataclass(frozen=True, slots=True)
class AnchoringAgent:
    min_evidence_refs: int = 2
    min_confidence: float = 0.6
    min_expected_edge: float = 0.02
    min_divergence: float = 0.15
    _evaluator: AnchoringEvidenceEvaluator = field(init=False, repr=False)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "_evaluator",
            AnchoringEvidenceEvaluator(
                min_evidence_refs=self.min_evidence_refs,
                min_confidence=self.min_confidence,
                min_expected_edge=self.min_expected_edge,
                min_divergence=self.min_divergence,
            ),
        )

    async def judge(
        self,
        context: StrategyContext,
        candidate: StrategyCandidate,
    ) -> StrategyJudgement:
        _require_same_strategy(context, candidate)
        assessment = self._evaluator.assess(candidate)
        return StrategyJudgement(
            judgement_id=f"judgement-{candidate.candidate_id}",
            candidate_id=candidate.candidate_id,
            strategy_id=context.strategy_id,
            strategy_version_id=context.strategy_version_id,
            approved=assessment.approved,
            confidence=assessment.confidence,
            rationale=assessment.rationale,
            evidence_refs=assessment.evidence_refs,
            failure_reasons=assessment.failure_reasons,
            created_at=context.as_of,
        )

    async def build_intents(
        self,
        context: StrategyContext,
        candidate: StrategyCandidate,
        judgement: StrategyJudgement,
    ) -> Sequence[TradeIntent | BasketIntent]:
        _require_same_strategy(context, candidate)
        if judgement.candidate_id != candidate.candidate_id:
            msg = "judgement candidate_id must match candidate"
            raise ValueError(msg)
        if not judgement.approved:
            return ()

        metadata = candidate.metadata
        intent = TradeIntent(
            intent_id=f"intent-{candidate.candidate_id}",
            strategy_id=context.strategy_id,
            strategy_version_id=context.strategy_version_id,
            candidate_id=candidate.candidate_id,
            market_id=candidate.market_id,
            token_id=_metadata_str(metadata, "token_id"),
            venue=cast(Venue, _metadata_str(metadata, "venue")),
            side=cast(BookSide, _metadata_str(metadata, "side")),
            outcome=cast(Outcome, _metadata_str(metadata, "outcome")),
            limit_price=_metadata_float(metadata, "limit_price"),
            notional_usdc=_metadata_float(metadata, "notional_usdc"),
            expected_price=_metadata_float(metadata, "expected_price"),
            expected_edge=candidate.expected_edge,
            max_slippage_bps=_metadata_int(metadata, "max_slippage_bps"),
            time_in_force=_metadata_time_in_force(metadata),
            evidence_refs=(judgement.judgement_id, *judgement.evidence_refs),
            created_at=context.as_of,
        )
        return (intent,)


def _require_same_strategy(
    context: StrategyContext,
    candidate: StrategyCandidate,
) -> None:
    if (
        candidate.strategy_id != context.strategy_id
        or candidate.strategy_version_id != context.strategy_version_id
    ):
        msg = "candidate strategy identity must match context"
        raise ValueError(msg)


def _metadata_str(metadata: Mapping[str, Any], field_name: str) -> str:
    value = metadata[field_name]
    if not isinstance(value, str):
        msg = f"{field_name} must be a string"
        raise TypeError(msg)
    return value


def _metadata_float(metadata: Mapping[str, Any], field_name: str) -> float:
    value = metadata[field_name]
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        msg = f"{field_name} must be numeric"
        raise TypeError(msg)
    return float(value)


def _metadata_int(metadata: Mapping[str, Any], field_name: str) -> int:
    value = metadata[field_name]
    if isinstance(value, bool) or not isinstance(value, int):
        msg = f"{field_name} must be an integer"
        raise TypeError(msg)
    return cast(int, value)


def _metadata_time_in_force(metadata: Mapping[str, Any]) -> TimeInForce:
    value = metadata["time_in_force"]
    if isinstance(value, TimeInForce):
        return value
    if isinstance(value, str):
        return TimeInForce(value)
    msg = "time_in_force must be a TimeInForce or string"
    raise TypeError(msg)
