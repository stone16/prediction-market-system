from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from typing import cast

import asyncpg
import pytest

from pms.meta_evidence.models import CompetitionSnapshot, PerformancePeak
from pms.storage.meta_evidence_store import MetaEvidenceStore
from pms.storage.strategy_registry import PostgresStrategyRegistry


@dataclass
class _RecordingConnection:
    execute_calls: list[tuple[str, tuple[object, ...]]] = field(default_factory=list)
    fetchrow_rows: list[dict[str, object] | None] = field(default_factory=list)

    async def execute(self, query: str, *args: object) -> str:
        self.execute_calls.append((query, args))
        return "OK"

    async def fetchrow(self, query: str, *args: object) -> dict[str, object] | None:
        del query, args
        if not self.fetchrow_rows:
            return None
        return self.fetchrow_rows.pop(0)


class _Acquire:
    def __init__(self, connection: _RecordingConnection) -> None:
        self._connection = connection

    async def __aenter__(self) -> _RecordingConnection:
        return self._connection

    async def __aexit__(self, *_: object) -> None:
        return None


class _Pool:
    def __init__(self, connection: _RecordingConnection) -> None:
        self._connection = connection

    def acquire(self) -> _Acquire:
        return _Acquire(self._connection)


@pytest.mark.asyncio
async def test_strategy_registry_set_version_metadata_targets_one_version_row() -> None:
    connection = _RecordingConnection()
    registry = PostgresStrategyRegistry(cast(asyncpg.Pool, _Pool(connection)))

    await registry.set_version_metadata(
        "meta-strategy",
        "meta-v1",
        {"validation_regime": "low_vol_bull", "regime_source": "price_changes"},
    )

    query, args = connection.execute_calls[0]
    assert "UPDATE strategy_versions" in query
    assert "metadata_json = metadata_json || $3::jsonb" in query
    assert args[:2] == ("meta-strategy", "meta-v1")
    assert json.loads(cast(str, args[2])) == {
        "validation_regime": "low_vol_bull",
        "regime_source": "price_changes",
    }


@pytest.mark.asyncio
async def test_meta_evidence_store_round_trips_performance_peak() -> None:
    now = datetime(2026, 5, 6, tzinfo=UTC)
    connection = _RecordingConnection(
        fetchrow_rows=[
            {
                "strategy_id": "meta-strategy",
                "strategy_version_id": "meta-v1",
                "peak_sharpe_7d": 1.5,
                "peak_sharpe_30d": 1.8,
                "peak_hit_rate": 0.6,
                "recorded_at": now,
            }
        ]
    )
    store = MetaEvidenceStore(cast(asyncpg.Pool, _Pool(connection)))
    peak = PerformancePeak(
        strategy_id="meta-strategy",
        strategy_version_id="meta-v1",
        peak_sharpe_7d=1.5,
        peak_sharpe_30d=1.8,
        peak_hit_rate=0.6,
        recorded_at=now,
    )

    await store.upsert_performance_peak(peak)
    stored = await store.get_performance_peak("meta-strategy", "meta-v1")

    query, args = connection.execute_calls[0]
    assert "ON CONFLICT (strategy_id, strategy_version_id) DO UPDATE" in query
    assert args == ("meta-strategy", "meta-v1", 1.5, 1.8, 0.6, now)
    assert stored == peak


@pytest.mark.asyncio
async def test_meta_evidence_store_upserts_competition_snapshot_idempotently() -> None:
    now = datetime(2026, 5, 6, tzinfo=UTC)
    snapshot = CompetitionSnapshot(
        snapshot_id="snapshot-meta",
        strategy_id="meta-strategy",
        strategy_version_id="meta-v1",
        snapshot_date=date(2026, 5, 6),
        mean_edge_30d=0.05,
        mean_spread_bps_30d=120.0,
        edge_trend_slope_90d=None,
        spread_trend_slope_90d=None,
        sample_count_30d=12,
        trend_status="warming_up",
        days_collected=12,
        short_term_slope_30d=None,
        short_term_slope_60d=None,
        interpretation="warming_up",
        created_at=now,
    )
    connection = _RecordingConnection()
    store = MetaEvidenceStore(cast(asyncpg.Pool, _Pool(connection)))

    await store.upsert_competition_snapshot(snapshot)

    query, args = connection.execute_calls[0]
    assert "ON CONFLICT (strategy_id, strategy_version_id, snapshot_date) DO UPDATE" in query
    assert args[:4] == (
        "snapshot-meta",
        "meta-strategy",
        "meta-v1",
        date(2026, 5, 6),
    )
