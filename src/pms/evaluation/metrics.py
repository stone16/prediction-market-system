from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass

from pms.core.models import EvalRecord


StrategyVersionKey = tuple[str, str]


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
class StrategyMetricsSnapshot:
    strategy_id: str
    strategy_version_id: str
    brier_overall: float | None
    brier_by_category: dict[str, float]
    brier_samples: dict[str, int]
    pnl: float
    slippage_bps: float
    fill_rate: float
    win_rate: float
    calibration_samples: dict[str, int]


@dataclass(frozen=True, init=False)
class MetricsCollector:
    records: tuple[EvalRecord, ...]

    def __init__(self, records: Iterable[EvalRecord]) -> None:
        object.__setattr__(self, "records", tuple(records))

    def global_ops_snapshot(self) -> MetricsSnapshot:
        return _build_metrics_snapshot(self.records)

    def snapshot_by_strategy(self) -> Mapping[StrategyVersionKey, StrategyMetricsSnapshot]:
        grouped_records: dict[StrategyVersionKey, list[EvalRecord]] = defaultdict(list)
        for record in self.records:
            grouped_records[(record.strategy_id, record.strategy_version_id)].append(record)
        return {
            key: _build_strategy_metrics_snapshot(key, records)
            for key, records in grouped_records.items()
        }


def _build_metrics_snapshot(records: tuple[EvalRecord, ...] | list[EvalRecord]) -> MetricsSnapshot:
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


def _build_strategy_metrics_snapshot(
    key: StrategyVersionKey,
    records: list[EvalRecord],
) -> StrategyMetricsSnapshot:
    metrics = _build_metrics_snapshot(records)
    return StrategyMetricsSnapshot(
        strategy_id=key[0],
        strategy_version_id=key[1],
        brier_overall=metrics.brier_overall,
        brier_by_category=dict(metrics.brier_by_category),
        brier_samples=dict(metrics.brier_samples),
        pnl=metrics.pnl,
        slippage_bps=metrics.slippage_bps,
        fill_rate=metrics.fill_rate,
        win_rate=metrics.win_rate,
        calibration_samples=dict(metrics.calibration_samples),
    )
