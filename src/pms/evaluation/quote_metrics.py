from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from decimal import Decimal

from pms.core.models import QuoteEvalRecord


StrategyVersionKey = tuple[str, str]


@dataclass(frozen=True)
class QuoteMetricsSnapshot:
    quote_score_overall: float | None
    quote_score_by_category: dict[str, float]
    quote_score_samples: dict[str, int]
    record_count: int
    mtm_pnl: float


@dataclass(frozen=True, init=False)
class QuoteMetricsCollector:
    records: tuple[QuoteEvalRecord, ...]

    def __init__(self, records: Iterable[QuoteEvalRecord]) -> None:
        object.__setattr__(self, "records", tuple(records))

    def global_ops_snapshot(self) -> QuoteMetricsSnapshot:
        return _build_quote_metrics_snapshot(self.records)

    def snapshot_by_strategy(self) -> Mapping[StrategyVersionKey, QuoteMetricsSnapshot]:
        grouped_records: dict[StrategyVersionKey, list[QuoteEvalRecord]] = defaultdict(list)
        for record in self.records:
            grouped_records[(record.strategy_id, record.strategy_version_id)].append(record)
        return {
            key: _build_quote_metrics_snapshot(records)
            for key, records in grouped_records.items()
        }


def _build_quote_metrics_snapshot(
    records: tuple[QuoteEvalRecord, ...] | list[QuoteEvalRecord],
) -> QuoteMetricsSnapshot:
    if not records:
        return QuoteMetricsSnapshot(
            quote_score_overall=None,
            quote_score_by_category={},
            quote_score_samples={},
            record_count=0,
            mtm_pnl=0.0,
        )

    scores_by_category: dict[str, list[Decimal]] = defaultdict(list)
    for record in records:
        category = record.category or record.model_id or "unknown"
        scores_by_category[category].append(Decimal(str(record.quote_score)))

    total_quote_score = sum(Decimal(str(record.quote_score)) for record in records)
    total_mtm_pnl = sum(Decimal(str(record.mtm_pnl)) for record in records)
    return QuoteMetricsSnapshot(
        quote_score_overall=float(total_quote_score / Decimal(len(records))),
        quote_score_by_category={
            category: float(sum(scores) / Decimal(len(scores)))
            for category, scores in scores_by_category.items()
        },
        quote_score_samples={
            category: len(scores) for category, scores in scores_by_category.items()
        },
        record_count=len(records),
        mtm_pnl=float(total_mtm_pnl),
    )
