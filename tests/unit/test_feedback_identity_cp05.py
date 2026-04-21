from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, cast

import pytest

from pms.core.models import Feedback
from pms.storage.feedback_store import _feedback_from_record, insert_feedback_row


class RecordingConnection:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    async def execute(self, query: str, *args: object) -> str:
        self.calls.append((query, args))
        return "INSERT 0 1"


def _feedback(
    *,
    strategy_id: str = "alpha",
    strategy_version_id: str = "alpha-v1",
) -> Feedback:
    return Feedback(
        feedback_id="fb-runtime-identity",
        target="controller",
        source="evaluator",
        message="typed identity should round-trip",
        severity="warning",
        created_at=datetime(2026, 4, 20, tzinfo=UTC),
        strategy_id=strategy_id,
        strategy_version_id=strategy_version_id,
        metadata={"origin": "unit-test"},
    )


@pytest.mark.asyncio
async def test_insert_feedback_row_uses_feedback_identity_when_kwargs_are_omitted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = RecordingConnection()

    async def passthrough_tags(
        inner: RecordingConnection,
        *,
        strategy_id: str,
        strategy_version_id: str | None,
    ) -> tuple[str, str]:
        del inner
        assert strategy_id == "alpha"
        assert strategy_version_id == "alpha-v1"
        return strategy_id, strategy_version_id

    monkeypatch.setattr(
        "pms.storage.feedback_store.resolve_strategy_tags",
        passthrough_tags,
    )

    await insert_feedback_row(connection, _feedback())

    assert len(connection.calls) == 1
    _, args = connection.calls[0]
    assert args[10] == "alpha"
    assert args[11] == "alpha-v1"


def test_feedback_from_record_restores_typed_strategy_identity() -> None:
    feedback = _feedback_from_record(
        cast(
            Any,
            {
                "feedback_id": "fb-runtime-identity",
                "target": "controller",
                "source": "evaluator",
                "message": "typed identity should round-trip",
                "severity": "warning",
                "created_at": datetime(2026, 4, 20, tzinfo=UTC),
                "resolved": False,
                "resolved_at": None,
                "category": None,
                "metadata": {"origin": "unit-test"},
                "strategy_id": "alpha",
                "strategy_version_id": "alpha-v1",
            },
        )
    )

    assert feedback.strategy_id == "alpha"
    assert feedback.strategy_version_id == "alpha-v1"
