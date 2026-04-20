"""Evaluation report generation for completed backtest runs."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
import json
from typing import Any, cast
from uuid import uuid4

import asyncpg

from pms.research.entities import (
    EvaluationRankingMetric,
    EvaluationReport,
    RankedStrategy,
)


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

            report = self._build_report(
                run_id=run_id,
                ranking_metric=ranking_metric,
                strategy_runs=tuple(_row_to_strategy_snapshot(row) for row in strategy_rows),
                warnings=_warnings_from_spec_json(run_row["spec_json"]),
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
                json.dumps([], separators=(",", ":"), ensure_ascii=True),
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
            benchmark_rows=(),
            attribution_commentary=_attribution_commentary(
                ranked_strategies=ranked_strategies,
                ranking_metric=ranking_metric,
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
    warnings: Sequence[str],
) -> str:
    leader = ranked_strategies[0]
    warning_text = (
        f" {len(warnings)} warning(s) were carried into the report."
        if warnings
        else " No warnings were recorded."
    )
    return (
        f"Ranking by {ranking_metric} placed "
        f"{leader.strategy_id}/{leader.strategy_version_id} first at "
        f"{leader.metric_value:.4f} across {len(ranked_strategies)} strategies."
        f"{warning_text}"
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
    dataset = payload.get("dataset")
    if not isinstance(dataset, Mapping):
        return ()
    raw_gaps = dataset.get("data_quality_gaps", [])
    if not isinstance(raw_gaps, list):
        return ()
    warnings: list[str] = []
    for item in raw_gaps:
        if not isinstance(item, list | tuple) or len(item) != 3:
            continue
        warnings.append(f"data gap {item[2]} from {item[0]} to {item[1]}")
    return tuple(warnings)


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
