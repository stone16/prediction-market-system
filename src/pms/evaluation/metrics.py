from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass

from pms.core.models import EvalRecord


@dataclass(frozen=True)
class MetricsSnapshot:
    brier_overall: float | None
    brier_by_category: dict[str, float]
    brier_samples: dict[str, int]
    pnl: float
    slippage_bps: float
    fill_rate: float
    win_rate: float
    calibration_samples: dict[str, int]


@dataclass(frozen=True)
class MetricsCollector:
    records: Iterable[EvalRecord]

    def snapshot(self) -> MetricsSnapshot:
        records = list(self.records)
        if not records:
            return MetricsSnapshot(
                brier_overall=None,
                brier_by_category={},
                brier_samples={},
                pnl=0.0,
                slippage_bps=0.0,
                fill_rate=0.0,
                win_rate=0.0,
                calibration_samples={},
            )

        brier_by_category: dict[str, list[float]] = defaultdict(list)
        calibration_samples: dict[str, int] = defaultdict(int)
        for record in records:
            category = record.category or record.model_id or "unknown"
            brier_by_category[category].append(record.brier_score)
            calibration_samples[record.model_id or "unknown"] += 1

        filled_count = sum(1 for record in records if record.filled)
        winning_count = sum(1 for record in records if record.pnl > 0.0)
        return MetricsSnapshot(
            brier_overall=sum(record.brier_score for record in records) / len(records),
            brier_by_category={
                category: sum(scores) / len(scores)
                for category, scores in brier_by_category.items()
            },
            brier_samples={
                category: len(scores) for category, scores in brier_by_category.items()
            },
            pnl=sum(record.pnl for record in records),
            slippage_bps=sum(record.slippage_bps for record in records) / len(records),
            fill_rate=filled_count / len(records),
            win_rate=winning_count / len(records),
            calibration_samples=dict(calibration_samples),
        )
