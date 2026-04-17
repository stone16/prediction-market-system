from __future__ import annotations

from datetime import datetime
from typing import Any

import asyncpg
from pydantic import BaseModel, field_serializer

from pms.storage.strategy_registry import PostgresStrategyRegistry


class StrategyRowResponse(BaseModel):
    strategy_id: str
    active_version_id: str | None
    created_at: datetime

    @field_serializer("created_at")
    def _serialize_created_at(self, created_at: datetime) -> str:
        return created_at.isoformat()


class StrategiesResponse(BaseModel):
    strategies: list[StrategyRowResponse]


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
