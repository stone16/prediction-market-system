"""Evaluation report generation for completed backtest runs."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
import json
import math
from typing import cast
from uuid import uuid4

import asyncpg

from pms.research.entities import (
    EvaluationRankingMetric,
    EvaluationReport,
    RankedStrategy,
)


MIN_SLICE_DECISION_COUNT = 20


@dataclass(frozen=True, slots=True)
class _StrategyRunSnapshot:
    strategy_id: str
    strategy_version_id: str
    brier: float | None
    pnl_cum: float | None
    drawdown_max: float | None
    fill_rate: float | None
    slippage_bps: float | None


@dataclass(frozen=True, slots=True)
class _StrategyRunSliceSnapshot:
    strategy_id: str
    strategy_version_id: str
    slice_label: str
    slice_start: datetime
    slice_end: datetime
    slice_kind: str
    brier: float | None
    pnl_cum: float | None
    drawdown_max: float | None
    fill_rate: float | None
    slippage_bps: float | None
    opportunity_count: int
    decision_count: int
    fill_count: int


@dataclass(frozen=True, slots=True)
class EvaluationReportGenerator:
    pool: asyncpg.Pool

    async def generate(
        self,
        run_id: str,
        ranking_metric: EvaluationRankingMetric = "brier",
    ) -> EvaluationReport:
        if ranking_metric not in ("brier", "sharpe", "pnl_cum"):
            msg = f"Unsupported ranking metric {ranking_metric!r}"
            raise ValueError(msg)

        connection = await self.pool.acquire()
        try:
            run_row = await connection.fetchrow(
                """
                SELECT run_id, spec_json
                FROM backtest_runs
                WHERE run_id = $1::uuid
                """,
                run_id,
            )
            if run_row is None:
                msg = f"EvaluationReportGenerator could not find run {run_id}"
                raise LookupError(msg)

            strategy_rows = await connection.fetch(
                """
                SELECT
                    strategy_id,
                    strategy_version_id,
                    brier,
                    pnl_cum,
                    drawdown_max,
                    fill_rate,
                    slippage_bps
                FROM strategy_runs
                WHERE run_id = $1::uuid
                ORDER BY strategy_id ASC, strategy_version_id ASC
                """,
                run_id,
            )
            if not strategy_rows:
                msg = f"EvaluationReportGenerator could not find strategy rows for run {run_id}"
                raise LookupError(msg)

            slice_rows = await connection.fetch(
                """
                SELECT
                    strategy_id,
                    strategy_version_id,
                    slice_label,
                    slice_start,
                    slice_end,
                    slice_kind,
                    brier,
                    pnl_cum,
                    drawdown_max,
                    fill_rate,
                    slippage_bps,
                    opportunity_count,
                    decision_count,
                    fill_count
                FROM strategy_run_slices
                WHERE run_id = $1::uuid
                ORDER BY
                    strategy_id ASC,
                    strategy_version_id ASC,
                    slice_start ASC,
                    slice_label ASC
                """,
                run_id,
            )
            strategy_snapshots = tuple(
                _row_to_strategy_snapshot(row) for row in strategy_rows
            )
            slice_snapshots = tuple(
                _row_to_strategy_slice_snapshot(row) for row in slice_rows
            )
            warnings = (
                *_warnings_from_spec_json(run_row["spec_json"]),
                *_warnings_from_slice_metrics(
                    strategy_runs=strategy_snapshots,
                    slice_metrics=slice_snapshots,
                ),
            )
            report = self._build_report(
                run_id=run_id,
                ranking_metric=ranking_metric,
                strategy_runs=strategy_snapshots,
                benchmark_rows=_benchmark_rows_from_slice_metrics(slice_snapshots),
                warnings=warnings,
                generated_at=datetime.now(tz=UTC),
            )
            saved_row = await connection.fetchrow(
                """
                INSERT INTO evaluation_reports (
                    report_id,
                    run_id,
                    ranking_metric,
                    ranked_strategies,
                    benchmark_rows,
                    attribution_commentary,
                    warnings,
                    next_action,
                    generated_at
                ) VALUES (
                    $1::uuid,
                    $2::uuid,
                    $3,
                    $4::jsonb,
                    $5::jsonb,
                    $6,
                    $7::jsonb,
                    $8,
                    $9
                )
                ON CONFLICT (run_id, ranking_metric) DO UPDATE
                SET
                    ranked_strategies = EXCLUDED.ranked_strategies,
                    benchmark_rows = EXCLUDED.benchmark_rows,
                    attribution_commentary = EXCLUDED.attribution_commentary,
                    warnings = EXCLUDED.warnings,
                    next_action = EXCLUDED.next_action,
                    generated_at = EXCLUDED.generated_at
                RETURNING
                    report_id,
                    run_id,
                    ranking_metric,
                    ranked_strategies,
                    benchmark_rows,
                    attribution_commentary,
                    warnings,
                    next_action,
                    generated_at
                """,
                report.report_id,
                report.run_id,
                report.ranking_metric,
                _serialize_ranked_strategies(report.ranked_strategies),
                _serialize_benchmark_rows(report.benchmark_rows),
                report.attribution_commentary,
                _serialize_warnings(report.warnings),
                report.next_action,
                report.generated_at,
            )
        finally:
            await self.pool.release(connection)

        assert saved_row is not None
        return _row_to_evaluation_report(saved_row)

    @staticmethod
    def _build_report(
        *,
        run_id: str,
        ranking_metric: EvaluationRankingMetric,
        strategy_runs: Sequence[_StrategyRunSnapshot],
        warnings: Sequence[str],
        generated_at: datetime,
        benchmark_rows: Sequence[Mapping[str, object]] = (),
    ) -> EvaluationReport:
        ranked_strategies = _ranked_strategies(
            strategy_runs=strategy_runs,
            ranking_metric=ranking_metric,
        )
        return EvaluationReport(
            report_id=str(uuid4()),
            run_id=run_id,
            ranking_metric=ranking_metric,
            ranked_strategies=tuple(ranked_strategies),
            benchmark_rows=tuple(dict(row) for row in benchmark_rows),
            attribution_commentary=_attribution_commentary(
                ranked_strategies=ranked_strategies,
                ranking_metric=ranking_metric,
                benchmark_rows=benchmark_rows,
                warnings=warnings,
            ),
            warnings=tuple(warnings),
            next_action=_next_action(
                ranked_strategies=ranked_strategies,
                warnings=warnings,
            ),
            generated_at=generated_at,
        )


def _row_to_strategy_snapshot(row: asyncpg.Record) -> _StrategyRunSnapshot:
    return _StrategyRunSnapshot(
        strategy_id=cast(str, row["strategy_id"]),
        strategy_version_id=cast(str, row["strategy_version_id"]),
        brier=cast(float | None, row["brier"]),
        pnl_cum=cast(float | None, row["pnl_cum"]),
        drawdown_max=cast(float | None, row["drawdown_max"]),
        fill_rate=cast(float | None, row["fill_rate"]),
        slippage_bps=cast(float | None, row["slippage_bps"]),
    )


def _row_to_strategy_slice_snapshot(row: asyncpg.Record) -> _StrategyRunSliceSnapshot:
    return _StrategyRunSliceSnapshot(
        strategy_id=cast(str, row["strategy_id"]),
        strategy_version_id=cast(str, row["strategy_version_id"]),
        slice_label=cast(str, row["slice_label"]),
        slice_start=cast(datetime, row["slice_start"]),
        slice_end=cast(datetime, row["slice_end"]),
        slice_kind=cast(str, row["slice_kind"]),
        brier=cast(float | None, row["brier"]),
        pnl_cum=cast(float | None, row["pnl_cum"]),
        drawdown_max=cast(float | None, row["drawdown_max"]),
        fill_rate=cast(float | None, row["fill_rate"]),
        slippage_bps=cast(float | None, row["slippage_bps"]),
        opportunity_count=cast(int, row["opportunity_count"]),
        decision_count=cast(int, row["decision_count"]),
        fill_count=cast(int, row["fill_count"]),
    )


def _row_to_evaluation_report(row: asyncpg.Record) -> EvaluationReport:
    return EvaluationReport(
        report_id=cast(str, row["report_id"]),
        run_id=cast(str, row["run_id"]),
        ranking_metric=cast(EvaluationRankingMetric, row["ranking_metric"]),
        ranked_strategies=_deserialize_ranked_strategies(row["ranked_strategies"]),
        benchmark_rows=_deserialize_benchmark_rows(row["benchmark_rows"]),
        attribution_commentary=cast(str, row["attribution_commentary"]),
        warnings=_deserialize_warnings(row["warnings"]),
        next_action=cast(str, row["next_action"]),
        generated_at=cast(datetime, row["generated_at"]),
    )


def _ranked_strategies(
    *,
    strategy_runs: Sequence[_StrategyRunSnapshot],
    ranking_metric: EvaluationRankingMetric,
) -> list[RankedStrategy]:
    sorted_runs = sorted(
        strategy_runs,
        key=lambda snapshot: _sort_key(snapshot=snapshot, ranking_metric=ranking_metric),
    )
    return [
        RankedStrategy(
            strategy_id=snapshot.strategy_id,
            strategy_version_id=snapshot.strategy_version_id,
            metric_value=_metric_value(snapshot=snapshot, ranking_metric=ranking_metric),
            rank=index,
        )
        for index, snapshot in enumerate(sorted_runs, start=1)
    ]


def _sort_key(
    *,
    snapshot: _StrategyRunSnapshot,
    ranking_metric: EvaluationRankingMetric,
) -> tuple[float, str, str]:
    metric_value = _metric_value(snapshot=snapshot, ranking_metric=ranking_metric)
    if ranking_metric == "brier":
        return (
            metric_value,
            snapshot.strategy_id,
            snapshot.strategy_version_id,
        )
    return (
        -metric_value,
        snapshot.strategy_id,
        snapshot.strategy_version_id,
    )


def _metric_value(
    *,
    snapshot: _StrategyRunSnapshot,
    ranking_metric: EvaluationRankingMetric,
) -> float:
    if ranking_metric == "brier":
        return snapshot.brier if snapshot.brier is not None else 1.0
    if ranking_metric == "pnl_cum":
        return snapshot.pnl_cum if snapshot.pnl_cum is not None else 0.0
    return _sharpe_like_value(snapshot)


def _sharpe_like_value(snapshot: _StrategyRunSnapshot) -> float:
    pnl_cum = snapshot.pnl_cum if snapshot.pnl_cum is not None else 0.0
    drawdown_max = snapshot.drawdown_max if snapshot.drawdown_max is not None else 0.0
    # S6 persists cumulative PnL and max drawdown, so use a simple risk-adjusted
    # proxy for the sharpe ranking view until a full return series lands.
    if drawdown_max > 0.0:
        return pnl_cum / drawdown_max
    return pnl_cum


def _attribution_commentary(
    *,
    ranked_strategies: Sequence[RankedStrategy],
    ranking_metric: EvaluationRankingMetric,
    benchmark_rows: Sequence[Mapping[str, object]],
    warnings: Sequence[str],
) -> str:
    leader = ranked_strategies[0]
    warning_text = (
        f" {len(warnings)} warning(s) were carried into the report."
        if warnings
        else " No warnings were recorded."
    )
    slice_metric_rows = [
        row
        for row in benchmark_rows
        if row.get("metric_type") == "walk_forward_slice"
    ]
    slice_text = ""
    if slice_metric_rows:
        distinct_slices = {
            str(row["slice_label"])
            for row in slice_metric_rows
            if isinstance(row.get("slice_label"), str)
        }
        slice_text = (
            f" Report includes {len(slice_metric_rows)} out-of-sample slice "
            f"metric row(s) across {len(distinct_slices)} slice(s)."
        )
    return (
        f"Ranking by {ranking_metric} placed "
        f"{leader.strategy_id}/{leader.strategy_version_id} first at "
        f"{leader.metric_value:.4f} across {len(ranked_strategies)} strategies."
        f"{warning_text}"
        f"{slice_text}"
    )


def _next_action(
    *,
    ranked_strategies: Sequence[RankedStrategy],
    warnings: Sequence[str],
) -> str:
    if warnings:
        return "Review the reported warnings before promoting this run."
    if len(ranked_strategies) == 1:
        leader = ranked_strategies[0]
        return (
            f"Promote {leader.strategy_id}/{leader.strategy_version_id} for paper trading."
        )
    return "Promote the top 2 strategies for paper trading."


def _warnings_from_spec_json(raw_value: object) -> tuple[str, ...]:
    payload = _json_object(raw_value)
    warnings = list(_execution_model_warnings(payload.get("execution_model")))
    dataset = payload.get("dataset")
    if not isinstance(dataset, Mapping):
        return tuple(warnings)
    raw_gaps = dataset.get("data_quality_gaps", [])
    if not isinstance(raw_gaps, list):
        return tuple(warnings)
    for item in raw_gaps:
        if not isinstance(item, list | tuple) or len(item) != 3:
            continue
        warnings.append(f"data gap {item[2]} from {item[0]} to {item[1]}")
    return tuple(warnings)


def _warnings_from_slice_metrics(
    *,
    strategy_runs: Sequence[_StrategyRunSnapshot],
    slice_metrics: Sequence[_StrategyRunSliceSnapshot],
) -> tuple[str, ...]:
    if not slice_metrics:
        return (
            "no walk-forward/out-of-sample slice metrics were recorded; "
            "do not promote from an in-sample-only report",
        )

    warnings: list[str] = []
    strategy_identities = {
        (snapshot.strategy_id, snapshot.strategy_version_id)
        for snapshot in strategy_runs
    }
    slice_identities = {
        (snapshot.strategy_id, snapshot.strategy_version_id)
        for snapshot in slice_metrics
    }
    missing_identities = sorted(strategy_identities - slice_identities)
    if missing_identities:
        formatted = ", ".join(
            f"{strategy_id}/{strategy_version_id}"
            for strategy_id, strategy_version_id in missing_identities
        )
        warnings.append(
            "walk-forward/out-of-sample slice metrics missing for "
            f"{formatted}"
        )

    unknown_identities = sorted(slice_identities - strategy_identities)
    if unknown_identities:
        formatted = ", ".join(
            f"{strategy_id}/{strategy_version_id}"
            for strategy_id, strategy_version_id in unknown_identities
        )
        warnings.append(
            "walk-forward/out-of-sample slice metrics reference unknown "
            f"strategy rows: {formatted}"
        )

    distinct_slices = {snapshot.slice_label for snapshot in slice_metrics}
    if len(distinct_slices) < 2:
        warnings.append(
            "only one walk-forward/out-of-sample slice metric was recorded; "
            "use multiple time/category/liquidity slices before promotion"
        )

    incomplete_rows = sum(
        1
        for snapshot in slice_metrics
        if (
            snapshot.brier is None
            or snapshot.pnl_cum is None
            or snapshot.drawdown_max is None
            or snapshot.fill_rate is None
        )
    )
    if incomplete_rows:
        warnings.append(
            f"{incomplete_rows} walk-forward/out-of-sample slice metric row(s) "
            "have incomplete brier/pnl/fill-rate/drawdown values"
        )

    underpowered_rows = sum(
        1
        for snapshot in slice_metrics
        if snapshot.decision_count < MIN_SLICE_DECISION_COUNT
    )
    if underpowered_rows:
        warnings.append(
            f"{underpowered_rows} walk-forward/out-of-sample slice metric row(s) "
            f"have fewer than {MIN_SLICE_DECISION_COUNT} decision samples; "
            "do not promote until slice coverage is sufficient"
        )

    return tuple(warnings)


def _execution_model_warnings(raw_value: object) -> tuple[str, ...]:
    if not isinstance(raw_value, Mapping):
        return ()
    warnings: list[str] = []
    if raw_value.get("calibration_source") == "static_live_estimate":
        warnings.append(
            "execution model uses static Polymarket live estimates; "
            "calibrate from paper/live telemetry before promotion"
        )
    if (
        raw_value.get("calibration_source") == "telemetry_calibrated"
        and not _positive_number(raw_value.get("adverse_selection_bps"))
    ):
        warnings.append(
            "execution model telemetry profile has no adverse_selection_bps; "
            "include quote-drift samples before promotion"
        )
    return tuple(warnings)


def _positive_number(raw_value: object) -> bool:
    if isinstance(raw_value, bool) or not isinstance(raw_value, int | float):
        return False
    value = float(raw_value)
    return math.isfinite(value) and value > 0.0


def _benchmark_rows_from_slice_metrics(
    slice_metrics: Sequence[_StrategyRunSliceSnapshot],
) -> tuple[Mapping[str, object], ...]:
    rows: list[Mapping[str, object]] = []
    for snapshot in slice_metrics:
        rows.append(
            {
                "metric_type": "walk_forward_slice",
                "strategy_id": snapshot.strategy_id,
                "strategy_version_id": snapshot.strategy_version_id,
                "slice_label": snapshot.slice_label,
                "slice_start": snapshot.slice_start.isoformat(),
                "slice_end": snapshot.slice_end.isoformat(),
                "slice_kind": snapshot.slice_kind,
                "brier": snapshot.brier,
                "pnl_cum": snapshot.pnl_cum,
                "drawdown_max": snapshot.drawdown_max,
                "fill_rate": snapshot.fill_rate,
                "slippage_bps": snapshot.slippage_bps,
                "opportunity_count": snapshot.opportunity_count,
                "decision_count": snapshot.decision_count,
                "fill_count": snapshot.fill_count,
            }
        )
    return tuple(rows)


def _serialize_ranked_strategies(ranked_strategies: Sequence[RankedStrategy]) -> str:
    return json.dumps(
        [
            {
                "strategy_id": entry.strategy_id,
                "strategy_version_id": entry.strategy_version_id,
                "metric_value": entry.metric_value,
                "rank": entry.rank,
            }
            for entry in ranked_strategies
        ],
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    )


def _serialize_benchmark_rows(benchmark_rows: Sequence[Mapping[str, object]]) -> str:
    return json.dumps(
        [dict(row) for row in benchmark_rows],
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    )


def _serialize_warnings(warnings: Sequence[str]) -> str:
    return json.dumps(
        list(warnings),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    )


def _deserialize_ranked_strategies(raw_value: object) -> tuple[RankedStrategy, ...]:
    decoded = json.loads(raw_value) if isinstance(raw_value, str) else raw_value
    if not isinstance(decoded, list):
        msg = "EvaluationReport.ranked_strategies must decode to a JSON array"
        raise TypeError(msg)
    ranked: list[RankedStrategy] = []
    for item in decoded:
        if not isinstance(item, dict):
            msg = "EvaluationReport.ranked_strategies entries must decode to JSON objects"
            raise TypeError(msg)
        ranked.append(
            RankedStrategy(
                strategy_id=_required_str(item, "strategy_id"),
                strategy_version_id=_required_str(item, "strategy_version_id"),
                metric_value=_required_float(item, "metric_value"),
                rank=_required_int(item, "rank"),
            )
        )
    return tuple(ranked)


def _deserialize_benchmark_rows(raw_value: object) -> tuple[Mapping[str, object], ...]:
    decoded = json.loads(raw_value) if isinstance(raw_value, str) else raw_value
    if not isinstance(decoded, list):
        msg = "EvaluationReport.benchmark_rows must decode to a JSON array"
        raise TypeError(msg)
    rows: list[Mapping[str, object]] = []
    for item in decoded:
        if not isinstance(item, dict):
            msg = "EvaluationReport.benchmark_rows entries must decode to JSON objects"
            raise TypeError(msg)
        rows.append(cast(Mapping[str, object], dict(item)))
    return tuple(rows)


def _deserialize_warnings(raw_value: object) -> tuple[str, ...]:
    decoded = json.loads(raw_value) if isinstance(raw_value, str) else raw_value
    if not isinstance(decoded, list):
        msg = "EvaluationReport.warnings must decode to a JSON array"
        raise TypeError(msg)
    warnings: list[str] = []
    for item in decoded:
        if not isinstance(item, str):
            msg = "EvaluationReport.warnings entries must decode to strings"
            raise TypeError(msg)
        warnings.append(item)
    return tuple(warnings)


def _json_object(raw_value: object) -> dict[str, object]:
    decoded = json.loads(raw_value) if isinstance(raw_value, str) else raw_value
    if not isinstance(decoded, dict):
        msg = "Expected JSON object payload"
        raise TypeError(msg)
    return cast(dict[str, object], decoded)


def _required_str(payload: Mapping[str, object], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value:
        msg = f"EvaluationReport field {key!r} must be a non-empty string"
        raise TypeError(msg)
    return value


def _required_float(payload: Mapping[str, object], key: str) -> float:
    value = payload.get(key)
    if not isinstance(value, int | float) or isinstance(value, bool):
        msg = f"EvaluationReport field {key!r} must be numeric"
        raise TypeError(msg)
    return float(value)


def _required_int(payload: Mapping[str, object], key: str) -> int:
    value = payload.get(key)
    if not isinstance(value, int) or isinstance(value, bool):
        msg = f"EvaluationReport field {key!r} must be an integer"
        raise TypeError(msg)
    return value


__all__ = ["EvaluationReportGenerator"]
