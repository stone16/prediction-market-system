"""Deterministic evidence assessment for ripple candidates."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, cast

from pms.strategies.intents import StrategyCandidate


@dataclass(frozen=True, slots=True)
class RippleEvidenceAssessment:
    approved: bool
    confidence: float
    rationale: str
    evidence_refs: tuple[str, ...]
    failure_reasons: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class RippleEvidenceEvaluator:
    min_evidence_refs: int = 2
    min_confidence: float = 0.6

    def assess(self, candidate: StrategyCandidate) -> RippleEvidenceAssessment:
        confidence = _metadata_float(candidate.metadata, "confidence")
        contradiction_refs = _metadata_tuple(candidate.metadata, "contradiction_refs")
        if len(candidate.evidence_refs) < self.min_evidence_refs:
            return RippleEvidenceAssessment(
                approved=False,
                confidence=min(confidence, self.min_confidence - 0.01),
                rationale="rejected: insufficient fixture evidence",
                evidence_refs=candidate.evidence_refs,
                failure_reasons=("insufficient_evidence",),
            )
        if contradiction_refs:
            return RippleEvidenceAssessment(
                approved=False,
                confidence=min(confidence, self.min_confidence - 0.01),
                rationale="rejected: fixture evidence contains a contradiction",
                evidence_refs=(*candidate.evidence_refs, *contradiction_refs),
                failure_reasons=("contradiction",),
            )
        if confidence < self.min_confidence:
            return RippleEvidenceAssessment(
                approved=False,
                confidence=confidence,
                rationale="rejected: confidence below deterministic threshold",
                evidence_refs=candidate.evidence_refs,
                failure_reasons=("low_confidence",),
            )
        return RippleEvidenceAssessment(
            approved=True,
            confidence=confidence,
            rationale="approved: fixture evidence is sufficient and non-contradictory",
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
