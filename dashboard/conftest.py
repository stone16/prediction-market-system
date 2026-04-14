from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path


def seed_feedback(data_dir: Path) -> None:
    data_dir.mkdir(parents=True, exist_ok=True)
    now = datetime(2026, 4, 14, tzinfo=UTC).isoformat()
    rows = [
        {
            "feedback_id": "fb-open-1",
            "target": "controller",
            "source": "evaluator",
            "message": "Brier score crossed the review threshold for model-a.",
            "severity": "warning",
            "created_at": now,
            "resolved": False,
            "resolved_at": None,
            "category": "brier:model-a",
            "metadata": {"market_id": "pm-synthetic-010"},
        },
        {
            "feedback_id": "fb-open-2",
            "target": "controller",
            "source": "actuator",
            "message": "Paper fill slippage exceeded the configured limit.",
            "severity": "warning",
            "created_at": now,
            "resolved": False,
            "resolved_at": None,
            "category": "slippage",
            "metadata": {"market_id": "pm-synthetic-011"},
        },
        {
            "feedback_id": "fb-resolved",
            "target": "controller",
            "source": "evaluator",
            "message": "Win-rate feedback already handled.",
            "severity": "info",
            "created_at": now,
            "resolved": True,
            "resolved_at": now,
            "category": "win_rate",
            "metadata": {"market_id": "pm-synthetic-012"},
        },
    ]
    (data_dir / "feedback.jsonl").write_text(
        "\n".join(json.dumps(row) for row in rows) + "\n",
        encoding="utf-8",
    )
