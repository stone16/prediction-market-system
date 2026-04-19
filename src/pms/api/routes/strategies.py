from __future__ import annotations

import asyncio
from collections.abc import Mapping
from datetime import datetime
from typing import Any

import asyncpg
from pydantic import BaseModel, field_serializer

from pms.core.models import EvalRecord
from pms.evaluation.metrics import (
    MetricsCollector,
    StrategyMetricsSnapshot,
    StrategyVersionKey,
)
from pms.storage.eval_store import EvalStore
from pms.storage.strategy_registry import PostgresStrategyRegistry
from pms.strategies.projections import StrategyRow


class StrategyRowResponse(BaseModel):
    strategy_id: str
    active_version_id: str | None
    created_at: datetime

    @field_serializer("created_at")
    def _serialize_created_at(self, created_at: datetime) -> str:
        return created_at.isoformat()


class StrategiesResponse(BaseModel):
    strategies: list[StrategyRowResponse]


class StrategyMetricsRowResponse(BaseModel):
    strategy_id: str
    strategy_version_id: str
    created_at: datetime
    record_count: int
    insufficient_samples: bool
    brier_overall: float | None
    pnl: float
    fill_rate: float
    slippage_bps: float
    drawdown: float

    @field_serializer("created_at")
    def _serialize_created_at(self, created_at: datetime) -> str:
        return created_at.isoformat()


class StrategyMetricsResponse(BaseModel):
    strategies: list[StrategyMetricsRowResponse]


async def list_strategies(pg_pool: asyncpg.Pool) -> dict[str, Any]:
    registry = PostgresStrategyRegistry(pg_pool)
    payload = StrategiesResponse(
        strategies=[
            StrategyRowResponse(
                strategy_id=row.strategy_id,
                active_version_id=row.active_version_id,
                created_at=row.created_at,
            )
            for row in await registry.list_strategies()
        ]
    )
    return payload.model_dump(mode="json")


async def list_strategy_metrics(pg_pool: asyncpg.Pool) -> dict[str, Any]:
    registry = PostgresStrategyRegistry(pg_pool)
    strategy_rows = [
        row for row in await registry.list_strategies() if row.active_version_id is not None
    ]
    eval_store = EvalStore(pg_pool)
    records_by_strategy = await _load_records_by_strategy(eval_store, strategy_rows)
    grouped_snapshots = MetricsCollector(
        record
        for strategy_records in records_by_strategy.values()
        for record in strategy_records
    ).snapshot_by_strategy()

    payload = StrategyMetricsResponse(
        strategies=[
            _strategy_metrics_row(
                row,
                records_by_strategy.get(
                    (row.strategy_id, _active_version_id(row)),
                    [],
                ),
                grouped_snapshots,
            )
            for row in strategy_rows
        ]
    )
    return payload.model_dump(mode="json")


async def _load_records_by_strategy(
    eval_store: EvalStore,
    strategy_rows: list[StrategyRow],
) -> dict[StrategyVersionKey, list[EvalRecord]]:
    if not strategy_rows:
        return {}

    keys = [
        (row.strategy_id, _active_version_id(row))
        for row in strategy_rows
    ]
    results = await asyncio.gather(
        *(
            eval_store.all_for_strategy(strategy_id, strategy_version_id)
            for strategy_id, strategy_version_id in keys
        )
    )
    return dict(zip(keys, results, strict=True))


def _strategy_metrics_row(
    row: StrategyRow,
    records: list[EvalRecord],
    grouped_snapshots: Mapping[StrategyVersionKey, StrategyMetricsSnapshot],
) -> StrategyMetricsRowResponse:
    strategy_version_id = _active_version_id(row)
    snapshot = grouped_snapshots.get((row.strategy_id, strategy_version_id))
    return StrategyMetricsRowResponse(
        strategy_id=row.strategy_id,
        strategy_version_id=strategy_version_id,
        created_at=row.created_at,
        record_count=len(records),
        insufficient_samples=len(records) == 0,
        brier_overall=None if snapshot is None else snapshot.brier_overall,
        pnl=0.0 if snapshot is None else snapshot.pnl,
        fill_rate=0.0 if snapshot is None else snapshot.fill_rate,
        slippage_bps=0.0 if snapshot is None else snapshot.slippage_bps,
        drawdown=_max_drawdown(records),
    )


def _active_version_id(row: StrategyRow) -> str:
    if row.active_version_id is None:
        msg = f"strategy {row.strategy_id} is missing an active version"
        raise ValueError(msg)
    return row.active_version_id


def _max_drawdown(records: list[EvalRecord]) -> float:
    cumulative_pnl = 0.0
    peak_equity = 0.0
    max_drawdown = 0.0
    for record in records:
        cumulative_pnl += record.pnl
        peak_equity = max(peak_equity, cumulative_pnl)
        max_drawdown = max(max_drawdown, peak_equity - cumulative_pnl)
    return max_drawdown
