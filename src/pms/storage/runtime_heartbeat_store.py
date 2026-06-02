from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import json
from typing import Any, cast

import asyncpg


@dataclass(frozen=True)
class RuntimeContinuity:
    run_id: str
    source: str
    first_observed_at: datetime
    last_observed_at: datetime
    heartbeat_count: int
    healthy_days: int
    max_gap_seconds: float
    unhealthy_heartbeat_count: int
    min_controller_runtimes: int

    def to_payload(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "source": self.source,
            "first_observed_at": self.first_observed_at.isoformat(),
            "last_observed_at": self.last_observed_at.isoformat(),
            "heartbeat_count": self.heartbeat_count,
            "healthy_days": self.healthy_days,
            "max_gap_seconds": self.max_gap_seconds,
            "unhealthy_heartbeat_count": self.unhealthy_heartbeat_count,
            "min_controller_runtimes": self.min_controller_runtimes,
        }


@dataclass
class RuntimeHeartbeatStore:
    pool: asyncpg.Pool

    async def append(
        self,
        *,
        run_id: str,
        mode: str,
        started_at: datetime,
        observed_at: datetime,
        strategy_fingerprint: str | None,
        component_status: dict[str, object],
    ) -> None:
        async with self.pool.acquire() as connection:
            await connection.execute(
                """
                INSERT INTO runtime_heartbeats (
                    run_id,
                    mode,
                    started_at,
                    observed_at,
                    strategy_fingerprint,
                    component_status_json
                ) VALUES ($1, $2, $3, $4, $5, $6::jsonb)
                """,
                run_id,
                mode,
                _aware(started_at),
                _aware(observed_at),
                strategy_fingerprint,
                json.dumps(component_status, sort_keys=True),
            )

    async def continuity(
        self,
        *,
        run_id: str,
        observed_until: datetime | None = None,
    ) -> RuntimeContinuity | None:
        async with self.pool.acquire() as connection:
            row = await connection.fetchrow(
                """
                WITH report_clock AS (
                    SELECT COALESCE($2::timestamptz, NOW()) AS observed_until
                ),
                ordered AS (
                    SELECT
                        started_at,
                        observed_at,
                        component_status_json,
                        LAG(observed_at) OVER (ORDER BY observed_at ASC) AS prev_observed_at
                    FROM runtime_heartbeats
                    WHERE run_id = $1
                ),
                aggregate AS (
                    SELECT
                        MIN(started_at) AS first_started_at,
                        MIN(observed_at) AS first_observed_at,
                        MAX(observed_at) AS last_observed_at,
                        COUNT(*) AS heartbeat_count,
                        COUNT(*) FILTER (
                            WHERE
                                COALESCE((component_status_json->>'running')::boolean, false) IS NOT TRUE
                                OR COALESCE((component_status_json->>'sensor_tasks')::integer, 0) <= 0
                                OR COALESCE((component_status_json->>'controller_runtimes')::integer, 0) <= 0
                        ) AS unhealthy_heartbeat_count,
                        MIN(
                            COALESCE(
                                (component_status_json->>'controller_runtimes')::integer,
                                0
                            )
                        ) AS min_controller_runtimes,
                        GREATEST(
                            0,
                            COALESCE(
                                MAX(EXTRACT(EPOCH FROM observed_at - prev_observed_at)),
                                0
                            ),
                            COALESCE(
                                EXTRACT(EPOCH FROM MIN(observed_at) - MIN(started_at)),
                                0
                            ),
                            COALESCE(
                                EXTRACT(
                                    EPOCH FROM (
                                        (SELECT observed_until FROM report_clock)
                                        - MAX(observed_at)
                                    )
                                ),
                                0
                            )
                        ) AS max_gap_seconds
                    FROM ordered
                )
                SELECT
                    first_started_at,
                    first_observed_at,
                    last_observed_at,
                    heartbeat_count,
                    unhealthy_heartbeat_count,
                    min_controller_runtimes,
                    max_gap_seconds
                FROM aggregate
                WHERE heartbeat_count > 0
                """,
                run_id,
                None if observed_until is None else _aware(observed_until),
            )
        if row is None:
            return None
        first_started_at = _aware(cast(datetime, row["first_started_at"]))
        first_observed_at = _aware(cast(datetime, row["first_observed_at"]))
        last_observed_at = _aware(cast(datetime, row["last_observed_at"]))
        return RuntimeContinuity(
            run_id=run_id,
            source="postgres_runtime_heartbeats",
            first_observed_at=first_observed_at,
            last_observed_at=last_observed_at,
            heartbeat_count=int(cast(int, row["heartbeat_count"])),
            healthy_days=_elapsed_whole_days(first_observed_at, last_observed_at),
            max_gap_seconds=float(row["max_gap_seconds"]),
            unhealthy_heartbeat_count=int(cast(int, row["unhealthy_heartbeat_count"])),
            min_controller_runtimes=int(cast(int, row["min_controller_runtimes"])),
        )


def _elapsed_whole_days(started_at: datetime, observed_until: datetime) -> int:
    elapsed_seconds = (_aware(observed_until) - _aware(started_at)).total_seconds()
    return max(0, int(elapsed_seconds // 86_400))


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)
