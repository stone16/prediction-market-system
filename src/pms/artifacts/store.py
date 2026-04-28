"""PostgreSQL persistence for strategy run artifacts."""

from __future__ import annotations

from collections.abc import Mapping
import json
from typing import Any, cast

import asyncpg

from pms.artifacts.models import (
    ExecutionArtifactType,
    JudgementArtifactType,
    StrategyExecutionArtifact,
    StrategyJudgementArtifact,
)


class StrategyArtifactStore:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def insert_judgement_artifact(
        self,
        artifact: StrategyJudgementArtifact,
    ) -> None:
        query = """
        INSERT INTO strategy_judgement_artifacts (
            artifact_id,
            strategy_id,
            strategy_version_id,
            artifact_type,
            observation_refs,
            candidate_id,
            judgement_id,
            judgement_summary,
            evidence_refs,
            assumptions,
            rejection_reasons,
            intent_payload,
            created_at
        ) VALUES (
            $1, $2, $3, $4, $5::jsonb, $6, $7, $8, $9::jsonb, $10::jsonb,
            $11::jsonb, $12::jsonb, $13
        )
        ON CONFLICT (artifact_id) DO UPDATE
        SET strategy_id = EXCLUDED.strategy_id,
            strategy_version_id = EXCLUDED.strategy_version_id,
            artifact_type = EXCLUDED.artifact_type,
            observation_refs = EXCLUDED.observation_refs,
            candidate_id = EXCLUDED.candidate_id,
            judgement_id = EXCLUDED.judgement_id,
            judgement_summary = EXCLUDED.judgement_summary,
            evidence_refs = EXCLUDED.evidence_refs,
            assumptions = EXCLUDED.assumptions,
            rejection_reasons = EXCLUDED.rejection_reasons,
            intent_payload = EXCLUDED.intent_payload,
            created_at = EXCLUDED.created_at
        """
        async with self._pool.acquire() as connection:
            async with connection.transaction():
                await connection.execute(
                    query,
                    artifact.artifact_id,
                    artifact.strategy_id,
                    artifact.strategy_version_id,
                    artifact.artifact_type,
                    _json_list(artifact.observation_refs),
                    artifact.candidate_id,
                    artifact.judgement_id,
                    artifact.judgement_summary,
                    _json_list(artifact.evidence_refs),
                    _json_list(artifact.assumptions),
                    _json_list(artifact.rejection_reasons),
                    _json_object(artifact.intent_payload),
                    artifact.created_at,
                )

    async def insert_execution_artifact(
        self,
        artifact: StrategyExecutionArtifact,
    ) -> None:
        query = """
        INSERT INTO strategy_execution_artifacts (
            artifact_id,
            strategy_id,
            strategy_version_id,
            artifact_type,
            intent_id,
            plan_id,
            execution_policy,
            execution_plan_payload,
            risk_decision_payload,
            venue_response_ids,
            reconciliation_status,
            post_trade_status,
            evidence_refs,
            rejection_reasons,
            created_at
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9::jsonb, $10::jsonb,
            $11, $12, $13::jsonb, $14::jsonb, $15
        )
        ON CONFLICT (artifact_id) DO UPDATE
        SET strategy_id = EXCLUDED.strategy_id,
            strategy_version_id = EXCLUDED.strategy_version_id,
            artifact_type = EXCLUDED.artifact_type,
            intent_id = EXCLUDED.intent_id,
            plan_id = EXCLUDED.plan_id,
            execution_policy = EXCLUDED.execution_policy,
            execution_plan_payload = EXCLUDED.execution_plan_payload,
            risk_decision_payload = EXCLUDED.risk_decision_payload,
            venue_response_ids = EXCLUDED.venue_response_ids,
            reconciliation_status = EXCLUDED.reconciliation_status,
            post_trade_status = EXCLUDED.post_trade_status,
            evidence_refs = EXCLUDED.evidence_refs,
            rejection_reasons = EXCLUDED.rejection_reasons,
            created_at = EXCLUDED.created_at
        """
        async with self._pool.acquire() as connection:
            async with connection.transaction():
                await connection.execute(
                    query,
                    artifact.artifact_id,
                    artifact.strategy_id,
                    artifact.strategy_version_id,
                    artifact.artifact_type,
                    artifact.intent_id,
                    artifact.plan_id,
                    artifact.execution_policy,
                    _json_object(artifact.execution_plan_payload),
                    _json_object(artifact.risk_decision_payload),
                    _json_list(artifact.venue_response_ids),
                    artifact.reconciliation_status,
                    artifact.post_trade_status,
                    _json_list(artifact.evidence_refs),
                    _json_list(artifact.rejection_reasons),
                    artifact.created_at,
                )

    async def list_judgement_artifacts(
        self,
        *,
        strategy_id: str,
        strategy_version_id: str,
        limit: int = 100,
    ) -> list[StrategyJudgementArtifact]:
        query = """
        SELECT
            artifact_id,
            strategy_id,
            strategy_version_id,
            artifact_type,
            observation_refs,
            candidate_id,
            judgement_id,
            judgement_summary,
            evidence_refs,
            assumptions,
            rejection_reasons,
            intent_payload,
            created_at
        FROM strategy_judgement_artifacts
        WHERE strategy_id = $1 AND strategy_version_id = $2
        ORDER BY created_at DESC, artifact_id DESC
        LIMIT $3
        """
        async with self._pool.acquire() as connection:
            rows = await connection.fetch(query, strategy_id, strategy_version_id, limit)
        return [_judgement_from_row(cast(Mapping[str, object], row)) for row in rows]

    async def list_execution_artifacts(
        self,
        *,
        strategy_id: str,
        strategy_version_id: str,
        limit: int = 100,
    ) -> list[StrategyExecutionArtifact]:
        query = """
        SELECT
            artifact_id,
            strategy_id,
            strategy_version_id,
            artifact_type,
            intent_id,
            plan_id,
            execution_policy,
            execution_plan_payload,
            risk_decision_payload,
            venue_response_ids,
            reconciliation_status,
            post_trade_status,
            evidence_refs,
            rejection_reasons,
            created_at
        FROM strategy_execution_artifacts
        WHERE strategy_id = $1 AND strategy_version_id = $2
        ORDER BY created_at DESC, artifact_id DESC
        LIMIT $3
        """
        async with self._pool.acquire() as connection:
            rows = await connection.fetch(query, strategy_id, strategy_version_id, limit)
        return [_execution_from_row(cast(Mapping[str, object], row)) for row in rows]


def _json_list(values: tuple[str, ...]) -> str:
    return json.dumps(
        list(values),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    )


def _json_object(value: Mapping[str, Any]) -> str:
    return json.dumps(
        dict(value),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    )


def _json_value(value: object) -> object:
    if isinstance(value, str):
        return json.loads(value)
    return value


def _json_tuple(value: object, field_name: str) -> tuple[str, ...]:
    loaded = _json_value(value)
    if not isinstance(loaded, list):
        msg = f"{field_name} must be a JSON array"
        raise TypeError(msg)
    items: list[str] = []
    for item in loaded:
        if not isinstance(item, str):
            msg = f"{field_name} must only contain strings"
            raise TypeError(msg)
        items.append(item)
    return tuple(items)


def _json_mapping(value: object, field_name: str) -> Mapping[str, Any]:
    loaded = _json_value(value)
    if not isinstance(loaded, Mapping):
        msg = f"{field_name} must be a JSON object"
        raise TypeError(msg)
    return cast(Mapping[str, Any], loaded)


def _string(row: Mapping[str, object], field_name: str) -> str:
    value = row[field_name]
    if not isinstance(value, str):
        msg = f"{field_name} must be a string"
        raise TypeError(msg)
    return value


def _optional_string(row: Mapping[str, object], field_name: str) -> str | None:
    value = row[field_name]
    if value is None:
        return None
    if not isinstance(value, str):
        msg = f"{field_name} must be a string or null"
        raise TypeError(msg)
    return value


def _judgement_from_row(row: Mapping[str, object]) -> StrategyJudgementArtifact:
    created_at = row["created_at"]
    if not hasattr(created_at, "tzinfo"):
        msg = "created_at must be a datetime"
        raise TypeError(msg)
    return StrategyJudgementArtifact(
        artifact_id=_string(row, "artifact_id"),
        strategy_id=_string(row, "strategy_id"),
        strategy_version_id=_string(row, "strategy_version_id"),
        artifact_type=cast(JudgementArtifactType, _string(row, "artifact_type")),
        observation_refs=_json_tuple(row["observation_refs"], "observation_refs"),
        candidate_id=_string(row, "candidate_id"),
        judgement_id=_optional_string(row, "judgement_id"),
        judgement_summary=_string(row, "judgement_summary"),
        evidence_refs=_json_tuple(row["evidence_refs"], "evidence_refs"),
        assumptions=_json_tuple(row["assumptions"], "assumptions"),
        rejection_reasons=_json_tuple(row["rejection_reasons"], "rejection_reasons"),
        intent_payload=_json_mapping(row["intent_payload"], "intent_payload"),
        created_at=cast(Any, created_at),
    )


def _execution_from_row(row: Mapping[str, object]) -> StrategyExecutionArtifact:
    created_at = row["created_at"]
    if not hasattr(created_at, "tzinfo"):
        msg = "created_at must be a datetime"
        raise TypeError(msg)
    return StrategyExecutionArtifact(
        artifact_id=_string(row, "artifact_id"),
        strategy_id=_string(row, "strategy_id"),
        strategy_version_id=_string(row, "strategy_version_id"),
        artifact_type=cast(ExecutionArtifactType, _string(row, "artifact_type")),
        intent_id=_string(row, "intent_id"),
        plan_id=_string(row, "plan_id"),
        execution_policy=_optional_string(row, "execution_policy"),
        execution_plan_payload=_json_mapping(
            row["execution_plan_payload"],
            "execution_plan_payload",
        ),
        risk_decision_payload=_json_mapping(
            row["risk_decision_payload"],
            "risk_decision_payload",
        ),
        venue_response_ids=_json_tuple(row["venue_response_ids"], "venue_response_ids"),
        reconciliation_status=_optional_string(row, "reconciliation_status"),
        post_trade_status=_optional_string(row, "post_trade_status"),
        evidence_refs=_json_tuple(row["evidence_refs"], "evidence_refs"),
        rejection_reasons=_json_tuple(row["rejection_reasons"], "rejection_reasons"),
        created_at=cast(Any, created_at),
    )
