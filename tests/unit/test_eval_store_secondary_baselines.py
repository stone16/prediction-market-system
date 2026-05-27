from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any, cast

import pytest

from pms.core.enums import OrderStatus
from pms.core.models import EvalRecord
from pms.storage import eval_store
from pms.storage.eval_store import insert_eval_record_row


class _RecordingConnection:
    def __init__(self) -> None:
        self.execute_calls: list[tuple[str, tuple[object, ...]]] = []

    async def execute(self, query: str, *args: object) -> str:
        self.execute_calls.append((query, args))
        return "INSERT 0 1"


def _record() -> EvalRecord:
    return EvalRecord(
        market_id="market-secondary-baseline",
        decision_id="decision-secondary-baseline",
        strategy_id="default",
        strategy_version_id="default-v1",
        prob_estimate=0.7,
        resolved_outcome=1.0,
        brier_score=0.09,
        baseline_prob_estimate=0.4,
        baseline_brier_score=0.36,
        baseline_prob_estimates={
            "market_implied": 0.4,
            "mid_quote": 0.42,
        },
        baseline_brier_scores={
            "market_implied": 0.36,
            "mid_quote": 0.3364,
        },
        fill_status=OrderStatus.MATCHED.value,
        recorded_at=datetime(2026, 5, 26, tzinfo=UTC),
        citations=["trade-secondary-baseline"],
    )


@pytest.mark.asyncio
async def test_eval_store_persists_secondary_baseline_maps() -> None:
    connection = _RecordingConnection()

    await insert_eval_record_row(cast(Any, connection), _record())

    query, args = connection.execute_calls[0]
    assert "baseline_prob_estimates" in query
    assert "baseline_brier_scores" in query
    assert json.loads(cast(str, args[10])) == {
        "market_implied": 0.4,
        "mid_quote": 0.42,
    }
    assert json.loads(cast(str, args[11])) == {
        "market_implied": 0.36,
        "mid_quote": 0.3364,
    }


def test_eval_store_rehydrates_secondary_baseline_maps() -> None:
    row = {
        "market_id": "market-secondary-baseline",
        "decision_id": "decision-secondary-baseline",
        "strategy_id": "default",
        "strategy_version_id": "default-v1",
        "prob_estimate": 0.7,
        "resolved_outcome": 1.0,
        "brier_score": 0.09,
        "baseline_prob_estimate": 0.4,
        "baseline_brier_score": 0.36,
        "baseline_prob_estimates": json.dumps(
            {"market_implied": 0.4, "mid_quote": 0.42}
        ),
        "baseline_brier_scores": json.dumps(
            {"market_implied": 0.36, "mid_quote": 0.3364}
        ),
        "fill_status": OrderStatus.MATCHED.value,
        "recorded_at": datetime(2026, 5, 26, tzinfo=UTC),
        "citations": json.dumps(["trade-secondary-baseline"]),
        "category": None,
        "model_id": None,
        "pnl": 0.0,
        "slippage_bps": 0.0,
        "filled": True,
        "edge_at_decision": 0.0,
        "spread_bps_at_decision": None,
    }

    record = eval_store._eval_record_from_row(cast(Any, row))

    assert record.baseline_prob_estimates == {
        "market_implied": 0.4,
        "mid_quote": 0.42,
    }
    assert record.baseline_brier_scores == {
        "market_implied": 0.36,
        "mid_quote": 0.3364,
    }
