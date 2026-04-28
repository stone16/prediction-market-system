"""Frozen strategy artifact value objects."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
import re
from typing import Any, Final, Literal, TypeAlias, cast


MAX_REASONING_SUMMARY_CHARS: Final = 4000

JudgementArtifactType: TypeAlias = Literal["approved_intent", "rejected_candidate"]
ExecutionArtifactType: TypeAlias = Literal[
    "accepted_execution_plan", "rejected_execution_plan"
]

JUDGEMENT_ARTIFACT_TYPES: Final[tuple[JudgementArtifactType, ...]] = (
    "approved_intent",
    "rejected_candidate",
)
EXECUTION_ARTIFACT_TYPES: Final[tuple[ExecutionArtifactType, ...]] = (
    "accepted_execution_plan",
    "rejected_execution_plan",
)

_SECRET_KEY_MARKERS: Final[frozenset[str]] = frozenset(
    {
        "api_key",
        "api_secret",
        "client_secret",
        "credential",
        "credentials",
        "password",
        "private_key",
    }
)
_SECRET_TEXT_PATTERNS: Final[tuple[re.Pattern[str], ...]] = (
    re.compile(
        r"\b(?:api[_-]?key|api[_-]?secret|client_secret|password)=\S{12,}",
        re.IGNORECASE,
    ),
    re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----", re.IGNORECASE),
)


@dataclass(frozen=True, slots=True)
class StrategyJudgementArtifact:
    artifact_id: str
    strategy_id: str
    strategy_version_id: str
    artifact_type: JudgementArtifactType
    observation_refs: tuple[str, ...]
    candidate_id: str
    judgement_id: str | None
    judgement_summary: str
    evidence_refs: tuple[str, ...]
    created_at: datetime
    assumptions: tuple[str, ...] = ()
    rejection_reasons: tuple[str, ...] = ()
    intent_payload: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _require_non_empty(self.artifact_id, "artifact_id")
        _require_strategy_identity(self.strategy_id, self.strategy_version_id)
        if self.artifact_type not in JUDGEMENT_ARTIFACT_TYPES:
            msg = f"artifact_type is unsupported: {self.artifact_type}"
            raise ValueError(msg)
        _require_non_empty(self.candidate_id, "candidate_id")
        if self.judgement_id is not None:
            _require_non_empty(self.judgement_id, "judgement_id")
        _require_bounded_summary(self.judgement_summary)
        _require_non_empty_sequence(self.evidence_refs, "evidence_refs")
        if self.artifact_type == "approved_intent" and not self.intent_payload:
            msg = "intent_payload is required for approved_intent artifacts"
            raise ValueError(msg)
        if self.artifact_type == "rejected_candidate" and not self.rejection_reasons:
            msg = "rejection_reasons is required for rejected_candidate artifacts"
            raise ValueError(msg)
        _reject_raw_secret_material(
            self.judgement_summary,
            self.assumptions,
            self.intent_payload,
        )


@dataclass(frozen=True, slots=True)
class StrategyExecutionArtifact:
    artifact_id: str
    strategy_id: str
    strategy_version_id: str
    artifact_type: ExecutionArtifactType
    intent_id: str
    plan_id: str
    execution_plan_payload: Mapping[str, Any]
    evidence_refs: tuple[str, ...]
    created_at: datetime
    execution_policy: str | None = None
    risk_decision_payload: Mapping[str, Any] = field(default_factory=dict)
    venue_response_ids: tuple[str, ...] = ()
    reconciliation_status: str | None = None
    post_trade_status: str | None = None
    rejection_reasons: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        _require_non_empty(self.artifact_id, "artifact_id")
        _require_strategy_identity(self.strategy_id, self.strategy_version_id)
        if self.artifact_type not in EXECUTION_ARTIFACT_TYPES:
            msg = f"artifact_type is unsupported: {self.artifact_type}"
            raise ValueError(msg)
        _require_non_empty(self.intent_id, "intent_id")
        _require_non_empty(self.plan_id, "plan_id")
        if not self.execution_plan_payload:
            msg = "execution_plan_payload is required for execution artifacts"
            raise ValueError(msg)
        _require_non_empty_sequence(self.evidence_refs, "evidence_refs")
        if self.artifact_type == "rejected_execution_plan" and not self.rejection_reasons:
            msg = "rejection_reasons is required for rejected_execution_plan artifacts"
            raise ValueError(msg)
        _reject_raw_secret_material(
            self.execution_plan_payload,
            self.risk_decision_payload,
            self.venue_response_ids,
        )


def _require_non_empty(value: str, field_name: str) -> None:
    if not value:
        msg = f"{field_name} must be non-empty"
        raise ValueError(msg)


def _require_strategy_identity(strategy_id: str, strategy_version_id: str) -> None:
    _require_non_empty(strategy_id, "strategy_id")
    _require_non_empty(strategy_version_id, "strategy_version_id")


def _require_non_empty_sequence(values: tuple[str, ...], field_name: str) -> None:
    if not values:
        msg = f"{field_name} must include at least one reference"
        raise ValueError(msg)
    if any(not value for value in values):
        msg = f"{field_name} must not include empty values"
        raise ValueError(msg)


def _require_bounded_summary(summary: str) -> None:
    _require_non_empty(summary, "judgement_summary")
    if len(summary) > MAX_REASONING_SUMMARY_CHARS:
        msg = (
            "judgement_summary exceeds "
            f"{MAX_REASONING_SUMMARY_CHARS} characters"
        )
        raise ValueError(msg)


def _reject_raw_secret_material(*values: object) -> None:
    if any(_contains_raw_secret(value) for value in values):
        msg = "raw secret material is not allowed in strategy artifacts"
        raise ValueError(msg)


def _contains_raw_secret(value: object) -> bool:
    if isinstance(value, Mapping):
        mapping = cast(Mapping[object, object], value)
        for key, child in mapping.items():
            if isinstance(key, str) and key.lower() in _SECRET_KEY_MARKERS:
                return True
            if _contains_raw_secret(child):
                return True
        return False
    if isinstance(value, str):
        return any(pattern.search(value) for pattern in _SECRET_TEXT_PATTERNS)
    if isinstance(value, Sequence):
        sequence = cast(Sequence[object], value)
        return any(_contains_raw_secret(child) for child in sequence)
    return False
