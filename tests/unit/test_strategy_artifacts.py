from __future__ import annotations

from datetime import UTC, datetime
import json
from typing import Any, cast

import asyncpg
import pytest

from pms.artifacts.models import (
    MAX_REASONING_SUMMARY_CHARS,
    StrategyExecutionArtifact,
    StrategyJudgementArtifact,
)
from pms.artifacts.store import StrategyArtifactStore


NOW = datetime(2026, 4, 28, 9, 30, tzinfo=UTC)


class _TransactionRecorder:
    def __init__(self, connection: "_RecordingConnection") -> None:
        self._connection = connection

    async def __aenter__(self) -> "_TransactionRecorder":
        self._connection.in_transaction = True
        self._connection.transaction_entries += 1
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        del exc_type, exc, tb
        self._connection.in_transaction = False


class _RecordingConnection:
    def __init__(self) -> None:
        self.in_transaction = False
        self.transaction_entries = 0
        self.execute_calls: list[tuple[str, tuple[object, ...], bool]] = []
        self.fetch_calls: list[tuple[str, tuple[object, ...]]] = []
        self.fetch_rows: list[object] = []

    async def execute(self, query: str, *args: object) -> str:
        self.execute_calls.append((query, args, self.in_transaction))
        return "OK"

    async def fetch(self, query: str, *args: object) -> list[object]:
        self.fetch_calls.append((query, args))
        return list(self.fetch_rows)

    def transaction(self) -> _TransactionRecorder:
        return _TransactionRecorder(self)


class _AcquireContext:
    def __init__(self, connection: _RecordingConnection) -> None:
        self._connection = connection

    async def __aenter__(self) -> _RecordingConnection:
        return self._connection

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        del exc_type, exc, tb


class _RecordingPool:
    def __init__(self, connection: _RecordingConnection) -> None:
        self._connection = connection

    def acquire(self) -> _AcquireContext:
        return _AcquireContext(self._connection)


def _judgement_artifact(**overrides: object) -> StrategyJudgementArtifact:
    data: dict[str, object] = {
        "artifact_id": "artifact-approved-intent",
        "strategy_id": "default",
        "strategy_version_id": "default-v1",
        "artifact_type": "approved_intent",
        "observation_refs": ("observation-1",),
        "candidate_id": "candidate-1",
        "judgement_id": "judgement-1",
        "judgement_summary": "Approved because edge remains positive after costs.",
        "evidence_refs": ("doc://edge-model", "quote://book-1"),
        "assumptions": ("fees use current venue schedule",),
        "rejection_reasons": (),
        "intent_payload": {"intent_id": "intent-1", "side": "BUY"},
        "created_at": NOW,
    }
    data.update(overrides)
    return StrategyJudgementArtifact(**cast(Any, data))


def _execution_artifact(**overrides: object) -> StrategyExecutionArtifact:
    data: dict[str, object] = {
        "artifact_id": "artifact-accepted-plan",
        "strategy_id": "default",
        "strategy_version_id": "default-v1",
        "artifact_type": "accepted_execution_plan",
        "intent_id": "intent-1",
        "plan_id": "plan-1",
        "execution_policy": "all_or_none",
        "execution_plan_payload": {
            "planned_orders": [{"planned_order_id": "planned-1"}]
        },
        "risk_decision_payload": {"approved": True},
        "venue_response_ids": ("venue-order-1",),
        "reconciliation_status": "open",
        "post_trade_status": "pending",
        "evidence_refs": ("quote://book-1",),
        "rejection_reasons": (),
        "created_at": NOW,
    }
    data.update(overrides)
    return StrategyExecutionArtifact(**cast(Any, data))


def test_judgement_artifact_represents_approved_intent_and_rejected_candidate() -> None:
    approved = _judgement_artifact()
    rejected = _judgement_artifact(
        artifact_id="artifact-rejected-candidate",
        artifact_type="rejected_candidate",
        judgement_summary="Rejected because liquidity was insufficient.",
        rejection_reasons=("insufficient_liquidity",),
        intent_payload={},
    )

    assert approved.artifact_type == "approved_intent"
    assert approved.intent_payload["intent_id"] == "intent-1"
    assert rejected.artifact_type == "rejected_candidate"
    assert rejected.rejection_reasons == ("insufficient_liquidity",)


def test_execution_artifact_represents_accepted_and_rejected_plans() -> None:
    accepted = _execution_artifact()
    rejected = _execution_artifact(
        artifact_id="artifact-rejected-plan",
        artifact_type="rejected_execution_plan",
        execution_plan_payload={"plan_id": "plan-1", "rejection_reason": "stale_book"},
        risk_decision_payload={},
        venue_response_ids=(),
        reconciliation_status=None,
        post_trade_status=None,
        rejection_reasons=("stale_book",),
    )

    assert accepted.artifact_type == "accepted_execution_plan"
    assert accepted.venue_response_ids == ("venue-order-1",)
    assert rejected.artifact_type == "rejected_execution_plan"
    assert rejected.rejection_reasons == ("stale_book",)


@pytest.mark.parametrize(
    ("field_name", "overrides"),
    [
        ("strategy_id", {"strategy_id": ""}),
        ("strategy_version_id", {"strategy_version_id": ""}),
        ("evidence_refs", {"evidence_refs": ()}),
        (
            "judgement_summary",
            {"judgement_summary": "x" * (MAX_REASONING_SUMMARY_CHARS + 1)},
        ),
        ("intent_payload", {"artifact_type": "approved_intent", "intent_payload": {}}),
        (
            "rejection_reasons",
            {"artifact_type": "rejected_candidate", "rejection_reasons": ()},
        ),
    ],
)
def test_judgement_artifact_validates_inner_ring_evidence(
    field_name: str,
    overrides: dict[str, object],
) -> None:
    with pytest.raises(ValueError, match=field_name):
        _judgement_artifact(**overrides)


def test_artifacts_reject_raw_secret_material() -> None:
    with pytest.raises(ValueError, match="raw secret"):
        _execution_artifact(
            risk_decision_payload={"credential": "postgres://user:password@example/db"}
        )


def test_judgement_summary_allows_benign_secret_marker_text() -> None:
    artifact = _judgement_artifact(
        judgement_summary="Docs mention api_key= placeholders must be rotated."
    )

    assert "api_key=" in artifact.judgement_summary


def test_artifacts_reject_value_shaped_secret_text() -> None:
    with pytest.raises(ValueError, match="raw secret"):
        _execution_artifact(
            execution_plan_payload={
                "note": "operator supplied password=supersecretvalue"
            }
        )


@pytest.mark.asyncio
async def test_store_inserts_judgement_artifact_as_json_payloads() -> None:
    connection = _RecordingConnection()
    store = StrategyArtifactStore(cast(asyncpg.Pool, _RecordingPool(connection)))

    await store.insert_judgement_artifact(_judgement_artifact())

    assert connection.transaction_entries == 1
    assert len(connection.execute_calls) == 1
    query, args, in_transaction = connection.execute_calls[0]
    assert in_transaction is True
    assert "INSERT INTO strategy_judgement_artifacts" in query
    assert "ON CONFLICT (artifact_id) DO NOTHING" in query
    assert "DO UPDATE" not in query
    assert args[:7] == (
        "artifact-approved-intent",
        "default",
        "default-v1",
        "approved_intent",
        json.dumps(["observation-1"], sort_keys=True, separators=(",", ":")),
        "candidate-1",
        "judgement-1",
    )
    assert json.loads(cast(str, args[8])) == ["doc://edge-model", "quote://book-1"]
    assert json.loads(cast(str, args[11])) == {"intent_id": "intent-1", "side": "BUY"}


@pytest.mark.asyncio
async def test_store_inserts_run_artifacts_in_one_transaction() -> None:
    connection = _RecordingConnection()
    store = StrategyArtifactStore(cast(asyncpg.Pool, _RecordingPool(connection)))

    await store.insert_run_artifacts(
        _judgement_artifact(),
        (_execution_artifact(),),
    )

    assert connection.transaction_entries == 1
    assert len(connection.execute_calls) == 2
    assert all(in_transaction is True for _, _, in_transaction in connection.execute_calls)


@pytest.mark.asyncio
async def test_store_lists_execution_artifacts_by_strategy_version() -> None:
    connection = _RecordingConnection()
    connection.fetch_rows = [
        {
            "artifact_id": "artifact-accepted-plan",
            "strategy_id": "default",
            "strategy_version_id": "default-v1",
            "artifact_type": "accepted_execution_plan",
            "intent_id": "intent-1",
            "plan_id": "plan-1",
            "execution_policy": "all_or_none",
            "execution_plan_payload": json.dumps(
                {"planned_orders": [{"planned_order_id": "planned-1"}]}
            ),
            "risk_decision_payload": json.dumps({"approved": True}),
            "venue_response_ids": json.dumps(["venue-order-1"]),
            "reconciliation_status": "open",
            "post_trade_status": "pending",
            "evidence_refs": json.dumps(["quote://book-1"]),
            "rejection_reasons": json.dumps([]),
            "created_at": NOW,
        }
    ]
    store = StrategyArtifactStore(cast(asyncpg.Pool, _RecordingPool(connection)))

    rows = await store.list_execution_artifacts(
        strategy_id="default",
        strategy_version_id="default-v1",
        limit=20,
    )

    assert rows == [_execution_artifact()]
    query, args = connection.fetch_calls[0]
    assert "FROM strategy_execution_artifacts" in query
    assert "ORDER BY created_at DESC, artifact_id DESC" in query
    assert args == ("default", "default-v1", 20)
