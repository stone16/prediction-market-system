from __future__ import annotations

from datetime import UTC, date, datetime
import uuid

from pms.core.models import EvalRecord
from pms.meta_evidence.models import CompetitionSnapshot, TrendStatus


def compute_competition_snapshot(
    records: list[EvalRecord],
    *,
    strategy_id: str,
    strategy_version_id: str,
    snapshot_date: date,
) -> CompetitionSnapshot:
    window_records = [
        record
        for record in records
        if (snapshot_date.toordinal() - record.recorded_at.date().toordinal()) < 30
        and record.recorded_at.date() <= snapshot_date
    ]
    edge_window_records = [
        record
        for record in window_records
        if _has_captured_edge(record)
    ]
    edge_values = [record.edge_at_decision for record in edge_window_records]
    spread_values = [
        float(record.spread_bps_at_decision)
        for record in edge_window_records
        if record.spread_bps_at_decision is not None
    ]
    collected_days = _days_collected(records, snapshot_date=snapshot_date)
    trend_status: TrendStatus = "active" if collected_days >= 90 else "warming_up"
    edge_slope_90d = _slope_by_day(
        [
            (record.recorded_at.date(), record.edge_at_decision)
            for record in records
            if _has_captured_edge(record)
            and (snapshot_date.toordinal() - record.recorded_at.date().toordinal()) < 90
            and record.recorded_at.date() <= snapshot_date
        ]
    ) if collected_days >= 90 else None
    spread_slope_90d = _slope_by_day(
        [
            (record.recorded_at.date(), float(record.spread_bps_at_decision))
            for record in records
            if record.spread_bps_at_decision is not None
            and (snapshot_date.toordinal() - record.recorded_at.date().toordinal()) < 90
            and record.recorded_at.date() <= snapshot_date
        ]
    ) if collected_days >= 90 else None

    interpretation = (
        "warming_up"
        if trend_status == "warming_up"
        else interpret_trends(
            edge_trend_slope_90d=edge_slope_90d,
            spread_trend_slope_90d=spread_slope_90d,
        )
    )
    return CompetitionSnapshot(
        snapshot_id=f"alpha-competition-{uuid.uuid4().hex}",
        strategy_id=strategy_id,
        strategy_version_id=strategy_version_id,
        snapshot_date=snapshot_date,
        mean_edge_30d=_mean(edge_values),
        mean_spread_bps_30d=_mean(spread_values),
        edge_trend_slope_90d=edge_slope_90d,
        spread_trend_slope_90d=spread_slope_90d,
        sample_count_30d=len(edge_window_records),
        trend_status=trend_status,
        days_collected=collected_days,
        short_term_slope_30d=None,
        short_term_slope_60d=None,
        interpretation=interpretation,
        created_at=datetime.now(tz=UTC),
    )


def interpret_trends(
    *,
    edge_trend_slope_90d: float | None,
    spread_trend_slope_90d: float | None,
) -> str:
    if edge_trend_slope_90d is None or spread_trend_slope_90d is None:
        return "insufficient_trend_data"
    if edge_trend_slope_90d < 0.0 and spread_trend_slope_90d < 0.0:
        return "market_getting_efficient_edge_compressing"
    if edge_trend_slope_90d < 0.0 and spread_trend_slope_90d >= 0.0:
        return "strategy_edge_compressing"
    if edge_trend_slope_90d >= 0.0 and spread_trend_slope_90d < 0.0:
        return "edge_resilient_despite_tighter_spreads"
    return "edge_and_spreads_expanding"


def _mean(values: list[float]) -> float | None:
    if not values:
        return None
    return sum(values) / len(values)


def _has_captured_edge(record: EvalRecord) -> bool:
    # The 0015 migration backfills legacy eval rows with 0.0 because the column
    # is NOT NULL. Controller decisions only emit positive edge decisions, so a
    # zero value is the legacy/missing sentinel for this metric.
    return record.edge_at_decision != 0.0


def _days_collected(records: list[EvalRecord], *, snapshot_date: date) -> int:
    if not records:
        return 0
    first_day = min(record.recorded_at.date() for record in records)
    return max(0, snapshot_date.toordinal() - first_day.toordinal() + 1)


def _slope_by_day(points: list[tuple[date, float]]) -> float | None:
    if len(points) < 2:
        return None
    x_values = [float(day.toordinal()) for day, _ in points]
    y_values = [value for _, value in points]
    x_mean = sum(x_values) / len(x_values)
    y_mean = sum(y_values) / len(y_values)
    denominator = sum((x_value - x_mean) ** 2 for x_value in x_values)
    if denominator == 0.0:
        return None
    numerator = sum(
        (x_value - x_mean) * (y_value - y_mean)
        for x_value, y_value in zip(x_values, y_values, strict=True)
    )
    return numerator / denominator
