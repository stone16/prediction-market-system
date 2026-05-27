from __future__ import annotations

from datetime import UTC, datetime, timedelta
import json
from typing import Any, cast

import asyncpg

from pms.core.models import EvalRecord
from pms.meta_evidence.decay import compute_decay_status
from pms.meta_evidence.models import CompetitionSnapshot
from pms.storage.meta_evidence_store import (
    competition_snapshot_from_row,
    performance_peak_from_row,
)


DECAY_RECORD_LOOKBACK_DAYS = 40


async def get_strategy_decay_status(
    pg_pool: asyncpg.Pool,
    *,
    strategy_id: str,
    strategy_version_id: str | None = None,
    min_resolved_samples: int = 10,
) -> dict[str, Any]:
    now = datetime.now(tz=UTC)
    cutoff_ts = now - timedelta(days=DECAY_RECORD_LOOKBACK_DAYS)
    async with pg_pool.acquire() as connection:
        active_version_id = strategy_version_id
        if active_version_id is None:
            row = await connection.fetchrow(
                """
                SELECT active_version_id
                FROM strategies
                WHERE strategy_id = $1
                """,
                strategy_id,
            )
            if row is None or row["active_version_id"] is None:
                msg = f"strategy {strategy_id} has no active version"
                raise LookupError(msg)
            active_version_id = cast(str, row["active_version_id"])

        version_row = await connection.fetchrow(
            """
            SELECT 1
            FROM strategy_versions
            WHERE strategy_id = $1 AND strategy_version_id = $2
            """,
            strategy_id,
            active_version_id,
        )
        if version_row is None:
            msg = (
                "strategy version not found for "
                f"strategy_id={strategy_id!r}, strategy_version_id={active_version_id!r}"
            )
            raise LookupError(msg)

        peak_row = await connection.fetchrow(
            """
            SELECT
                strategy_id,
                strategy_version_id,
                peak_sharpe_7d,
                peak_sharpe_30d,
                peak_hit_rate,
                recorded_at
            FROM strategy_performance_peaks
            WHERE strategy_id = $1 AND strategy_version_id = $2
            """,
            strategy_id,
            active_version_id,
        )
        snapshot_row = await connection.fetchrow(
            """
            SELECT
                snapshot_id,
                strategy_id,
                strategy_version_id,
                snapshot_date,
                mean_edge_30d,
                mean_spread_bps_30d,
                edge_trend_slope_90d,
                spread_trend_slope_90d,
                sample_count_30d,
                trend_status,
                days_collected,
                short_term_slope_30d,
                short_term_slope_60d,
                interpretation,
                created_at
            FROM alpha_competition_snapshots
            WHERE strategy_id = $1 AND strategy_version_id = $2
            ORDER BY snapshot_date DESC
            LIMIT 1
            """,
            strategy_id,
            active_version_id,
        )
        rows = await connection.fetch(
            """
            SELECT
                market_id,
                decision_id,
                strategy_id,
                strategy_version_id,
                prob_estimate,
                resolved_outcome,
                brier_score,
                baseline_prob_estimate,
                baseline_brier_score,
                fill_status,
                recorded_at,
                citations,
                category,
                model_id,
                pnl,
                slippage_bps,
                filled,
                edge_at_decision,
                spread_bps_at_decision
            FROM eval_records
            WHERE strategy_id = $1
              AND strategy_version_id = $2
              AND recorded_at >= $3
            ORDER BY recorded_at ASC, decision_id ASC
            """,
            strategy_id,
            active_version_id,
            cutoff_ts,
        )

    records = [_eval_record_from_row(row) for row in rows]
    peak = None if peak_row is None else performance_peak_from_row(peak_row)
    status = compute_decay_status(
        records,
        strategy_id=strategy_id,
        strategy_version_id=active_version_id,
        now=now,
        min_resolved_samples=min_resolved_samples,
        existing_peak=peak,
    )
    snapshot = None if snapshot_row is None else competition_snapshot_from_row(snapshot_row)
    return {
        "strategy_id": status.strategy_id,
        "strategy_version_id": status.strategy_version_id,
        "decay_status": status.decay_status,
        "rolling_sharpe_7d": status.rolling_sharpe_7d,
        "peak_sharpe_7d": status.peak_sharpe_7d,
        "sharpe_ratio_vs_peak": status.sharpe_ratio_vs_peak,
        "rolling_sharpe_30d": status.rolling_sharpe_30d,
        "hit_rate_7d": status.hit_rate_7d,
        "peak_hit_rate": status.peak_hit_rate,
        "trading_days_in_window": status.trading_days_in_window,
        "resolved_sample_count": status.resolved_sample_count,
        "min_resolved_samples": status.min_resolved_samples,
        "last_updated": status.last_updated.isoformat(),
        "alpha_competition": None if snapshot is None else _competition_payload(snapshot),
    }


def _eval_record_from_row(row: asyncpg.Record) -> EvalRecord:
    return EvalRecord(
        market_id=cast(str, row["market_id"]),
        decision_id=cast(str, row["decision_id"]),
        strategy_id=cast(str, row["strategy_id"]),
        strategy_version_id=cast(str, row["strategy_version_id"]),
        prob_estimate=float(cast(float, row["prob_estimate"])),
        resolved_outcome=float(cast(float, row["resolved_outcome"])),
        brier_score=float(cast(float, row["brier_score"])),
        baseline_prob_estimate=cast(
            float | None,
            _row_value(row, "baseline_prob_estimate", None),
        ),
        baseline_brier_score=cast(
            float | None,
            _row_value(row, "baseline_brier_score", None),
        ),
        fill_status=cast(str, row["fill_status"]),
        recorded_at=cast(datetime, row["recorded_at"]),
        citations=_citations(row["citations"]),
        category=cast(str | None, row["category"]),
        model_id=cast(str | None, row["model_id"]),
        pnl=float(cast(float, row["pnl"])),
        slippage_bps=float(cast(float, row["slippage_bps"])),
        filled=cast(bool, row["filled"]),
        edge_at_decision=float(cast(float, row["edge_at_decision"])),
        spread_bps_at_decision=cast(int | None, row["spread_bps_at_decision"]),
    )


def _citations(value: object) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value]
    if isinstance(value, str):
        loaded = json.loads(value)
        if isinstance(loaded, list):
            return [str(item) for item in loaded]
    return []


def _row_value(row: asyncpg.Record, key: str, default: object) -> object:
    try:
        return row[key]
    except (KeyError, IndexError):
        return default


def _competition_payload(snapshot: CompetitionSnapshot) -> dict[str, Any]:
    return {
        "mean_edge_30d": snapshot.mean_edge_30d,
        "mean_spread_bps_30d": snapshot.mean_spread_bps_30d,
        "edge_trend_slope_90d": snapshot.edge_trend_slope_90d,
        "spread_trend_slope_90d": snapshot.spread_trend_slope_90d,
        "trend_status": snapshot.trend_status,
        "days_collected": snapshot.days_collected,
        "interpretation": snapshot.interpretation,
        "sample_count_30d": snapshot.sample_count_30d,
        "last_snapshot_date": snapshot.snapshot_date.isoformat(),
    }
