"""Deterministic evidence gate for the H1 FLB strategy."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, cast

from pms.strategies.intents import StrategyCandidate


DEFAULT_MIN_EXPECTED_EDGE = 0.02
DEFAULT_MIN_CONFIDENCE = 0.60


@dataclass(frozen=True, slots=True)
class FlbEvidenceAssessment:
    approved: bool
    expected_edge: float
    confidence: float
    rationale: str
    evidence_refs: tuple[str, ...]
    failure_reasons: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class FlbEvidenceEvaluator:
    min_evidence_refs: int = 2
    min_confidence: float = DEFAULT_MIN_CONFIDENCE
    min_expected_edge: float = DEFAULT_MIN_EXPECTED_EDGE

    def assess(self, candidate: StrategyCandidate) -> FlbEvidenceAssessment:
        confidence = _metadata_float(candidate.metadata, "confidence")
        contradiction_refs = _metadata_tuple(candidate.metadata, "contradiction_refs")
        if len(candidate.evidence_refs) < self.min_evidence_refs:
            return FlbEvidenceAssessment(
                approved=False,
                expected_edge=candidate.expected_edge,
                confidence=min(confidence, self.min_confidence - 0.01),
                rationale="rejected: insufficient FLB evidence",
                evidence_refs=candidate.evidence_refs,
                failure_reasons=("insufficient_evidence",),
            )
        if contradiction_refs:
            return FlbEvidenceAssessment(
                approved=False,
                expected_edge=candidate.expected_edge,
                confidence=min(confidence, self.min_confidence - 0.01),
                rationale="rejected: FLB evidence contains a contradiction",
                evidence_refs=(*candidate.evidence_refs, *contradiction_refs),
                failure_reasons=("contradiction",),
            )
        if confidence < self.min_confidence:
            return FlbEvidenceAssessment(
                approved=False,
                expected_edge=candidate.expected_edge,
                confidence=confidence,
                rationale="rejected: FLB confidence below threshold",
                evidence_refs=candidate.evidence_refs,
                failure_reasons=("low_confidence",),
            )
        if candidate.expected_edge < self.min_expected_edge:
            return FlbEvidenceAssessment(
                approved=False,
                expected_edge=candidate.expected_edge,
                confidence=confidence,
                rationale="rejected: FLB expected edge below threshold",
                evidence_refs=candidate.evidence_refs,
                failure_reasons=("insufficient_expected_edge",),
            )
        return FlbEvidenceAssessment(
            approved=True,
            expected_edge=candidate.expected_edge,
            confidence=confidence,
            rationale=(
                "approved: H1 favorite-longshot bias bucket gives "
                f"{candidate.expected_edge:.4f} expected edge"
            ),
            evidence_refs=candidate.evidence_refs,
            failure_reasons=(),
        )


def _metadata_float(metadata: Mapping[str, Any], field_name: str) -> float:
    value = metadata[field_name]
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        msg = f"{field_name} must be numeric"
        raise TypeError(msg)
    return float(value)


def _metadata_tuple(metadata: Mapping[str, Any], field_name: str) -> tuple[str, ...]:
    value = metadata[field_name]
    if not isinstance(value, tuple):
        msg = f"{field_name} must be a tuple"
        raise TypeError(msg)
    if any(not isinstance(item, str) for item in value):
        msg = f"{field_name} must contain strings"
        raise TypeError(msg)
    return cast(tuple[str, ...], value)
