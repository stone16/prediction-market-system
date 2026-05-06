from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import cast

import asyncpg

from pms.meta_evidence.models import CompetitionSnapshot, PerformancePeak, TrendStatus


@dataclass
class MetaEvidenceStore:
    pool: asyncpg.Pool

    async def upsert_performance_peak(self, peak: PerformancePeak) -> None:
        async with self.pool.acquire() as connection:
            await connection.execute(
                """
                INSERT INTO strategy_performance_peaks (
                    strategy_id,
                    strategy_version_id,
                    peak_sharpe_7d,
                    peak_sharpe_30d,
                    peak_hit_rate,
                    recorded_at
                ) VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (strategy_id, strategy_version_id) DO UPDATE
                SET peak_sharpe_7d = EXCLUDED.peak_sharpe_7d,
                    peak_sharpe_30d = EXCLUDED.peak_sharpe_30d,
                    peak_hit_rate = EXCLUDED.peak_hit_rate,
                    recorded_at = EXCLUDED.recorded_at
                """,
                peak.strategy_id,
                peak.strategy_version_id,
                peak.peak_sharpe_7d,
                peak.peak_sharpe_30d,
                peak.peak_hit_rate,
                peak.recorded_at,
            )

    async def get_performance_peak(
        self,
        strategy_id: str,
        strategy_version_id: str,
    ) -> PerformancePeak | None:
        async with self.pool.acquire() as connection:
            row = await connection.fetchrow(
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
                strategy_version_id,
            )
        if row is None:
            return None
        return performance_peak_from_row(row)

    async def upsert_competition_snapshot(
        self,
        snapshot: CompetitionSnapshot,
    ) -> None:
        async with self.pool.acquire() as connection:
            await connection.execute(
                """
                INSERT INTO alpha_competition_snapshots (
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
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15
                )
                ON CONFLICT (strategy_id, strategy_version_id, snapshot_date) DO UPDATE
                SET mean_edge_30d = EXCLUDED.mean_edge_30d,
                    mean_spread_bps_30d = EXCLUDED.mean_spread_bps_30d,
                    edge_trend_slope_90d = EXCLUDED.edge_trend_slope_90d,
                    spread_trend_slope_90d = EXCLUDED.spread_trend_slope_90d,
                    sample_count_30d = EXCLUDED.sample_count_30d,
                    trend_status = EXCLUDED.trend_status,
                    days_collected = EXCLUDED.days_collected,
                    short_term_slope_30d = EXCLUDED.short_term_slope_30d,
                    short_term_slope_60d = EXCLUDED.short_term_slope_60d,
                    interpretation = EXCLUDED.interpretation,
                    created_at = EXCLUDED.created_at
                """,
                snapshot.snapshot_id,
                snapshot.strategy_id,
                snapshot.strategy_version_id,
                snapshot.snapshot_date,
                snapshot.mean_edge_30d,
                snapshot.mean_spread_bps_30d,
                snapshot.edge_trend_slope_90d,
                snapshot.spread_trend_slope_90d,
                snapshot.sample_count_30d,
                snapshot.trend_status,
                snapshot.days_collected,
                snapshot.short_term_slope_30d,
                snapshot.short_term_slope_60d,
                snapshot.interpretation,
                snapshot.created_at,
            )

    async def get_latest_competition_snapshot(
        self,
        strategy_id: str,
        strategy_version_id: str,
    ) -> CompetitionSnapshot | None:
        async with self.pool.acquire() as connection:
            row = await connection.fetchrow(
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
                strategy_version_id,
            )
        if row is None:
            return None
        return competition_snapshot_from_row(row)


def performance_peak_from_row(row: asyncpg.Record) -> PerformancePeak:
    return PerformancePeak(
        strategy_id=cast(str, row["strategy_id"]),
        strategy_version_id=cast(str, row["strategy_version_id"]),
        peak_sharpe_7d=float(cast(float, row["peak_sharpe_7d"])),
        peak_sharpe_30d=float(cast(float, row["peak_sharpe_30d"])),
        peak_hit_rate=float(cast(float, row["peak_hit_rate"])),
        recorded_at=cast(datetime, row["recorded_at"]),
    )


def competition_snapshot_from_row(row: asyncpg.Record) -> CompetitionSnapshot:
    return CompetitionSnapshot(
        snapshot_id=cast(str, row["snapshot_id"]),
        strategy_id=cast(str, row["strategy_id"]),
        strategy_version_id=cast(str, row["strategy_version_id"]),
        snapshot_date=cast(date, row["snapshot_date"]),
        mean_edge_30d=cast(float | None, row["mean_edge_30d"]),
        mean_spread_bps_30d=cast(float | None, row["mean_spread_bps_30d"]),
        edge_trend_slope_90d=cast(float | None, row["edge_trend_slope_90d"]),
        spread_trend_slope_90d=cast(float | None, row["spread_trend_slope_90d"]),
        sample_count_30d=cast(int, row["sample_count_30d"]),
        trend_status=cast(TrendStatus, row["trend_status"]),
        days_collected=cast(int, row["days_collected"]),
        short_term_slope_30d=cast(float | None, row["short_term_slope_30d"]),
        short_term_slope_60d=cast(float | None, row["short_term_slope_60d"]),
        interpretation=cast(str, row["interpretation"]),
        created_at=cast(datetime, row["created_at"]),
    )
