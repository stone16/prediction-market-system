"""Deterministic evidence gate for H2 anchoring-lag signals."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, cast

from pms.strategies.intents import StrategyCandidate


DEFAULT_MIN_DIVERGENCE = 0.15
DEFAULT_MIN_CONFIDENCE = 0.60
DEFAULT_MIN_EXPECTED_EDGE = 0.02


@dataclass(frozen=True, slots=True)
class AnchoringEvidenceAssessment:
    approved: bool
    expected_edge: float
    confidence: float
    rationale: str
    evidence_refs: tuple[str, ...]
    failure_reasons: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class AnchoringEvidenceEvaluator:
    min_evidence_refs: int = 2
    min_confidence: float = DEFAULT_MIN_CONFIDENCE
    min_expected_edge: float = DEFAULT_MIN_EXPECTED_EDGE
    min_divergence: float = DEFAULT_MIN_DIVERGENCE

    def assess(self, candidate: StrategyCandidate) -> AnchoringEvidenceAssessment:
        confidence = _metadata_float(candidate.metadata, "confidence")
        contradiction_refs = _metadata_tuple(candidate.metadata, "contradiction_refs")
        delta_effective = abs(_metadata_float(candidate.metadata, "delta_effective"))
        if len(candidate.evidence_refs) < self.min_evidence_refs:
            return AnchoringEvidenceAssessment(
                approved=False,
                expected_edge=candidate.expected_edge,
                confidence=min(confidence, self.min_confidence - 0.01),
                rationale="rejected: insufficient H2 anchoring-lag evidence",
                evidence_refs=candidate.evidence_refs,
                failure_reasons=("insufficient_evidence",),
            )
        if contradiction_refs:
            return AnchoringEvidenceAssessment(
                approved=False,
                expected_edge=candidate.expected_edge,
                confidence=min(confidence, self.min_confidence - 0.01),
                rationale="rejected: H2 anchoring-lag evidence contains a contradiction",
                evidence_refs=(*candidate.evidence_refs, *contradiction_refs),
                failure_reasons=("contradiction",),
            )
        if confidence < self.min_confidence:
            return AnchoringEvidenceAssessment(
                approved=False,
                expected_edge=candidate.expected_edge,
                confidence=confidence,
                rationale="rejected: H2 anchoring-lag confidence below threshold",
                evidence_refs=candidate.evidence_refs,
                failure_reasons=("low_confidence",),
            )
        if delta_effective <= self.min_divergence:
            return AnchoringEvidenceAssessment(
                approved=False,
                expected_edge=candidate.expected_edge,
                confidence=confidence,
                rationale="rejected: H2 anchoring-lag divergence below threshold",
                evidence_refs=candidate.evidence_refs,
                failure_reasons=("insufficient_divergence",),
            )
        if candidate.expected_edge < self.min_expected_edge:
            return AnchoringEvidenceAssessment(
                approved=False,
                expected_edge=candidate.expected_edge,
                confidence=confidence,
                rationale="rejected: H2 anchoring-lag expected edge below threshold",
                evidence_refs=candidate.evidence_refs,
                failure_reasons=("insufficient_expected_edge",),
            )
        return AnchoringEvidenceAssessment(
            approved=True,
            expected_edge=candidate.expected_edge,
            confidence=confidence,
            rationale=(
                "approved: H2 anchoring-lag signal gives "
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
