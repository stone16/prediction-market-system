from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
import json
from typing import Any, cast

import asyncpg

from pms.core.models import MarketRelation, MarketRelationType


@dataclass
class MarketRelationStore:
    pool: asyncpg.Pool

    async def insert_relations(self, relations: Sequence[MarketRelation]) -> None:
        if not relations:
            return

        args = [
            (
                relation.market_id_a,
                relation.market_id_b,
                relation.relation_type.value,
                relation.confidence,
                relation.detected_at,
                _metadata_json(relation.metadata),
            )
            for relation in relations
        ]
        async with self.pool.acquire() as connection:
            await connection.executemany(
                """
                INSERT INTO market_relations (
                    market_id_a,
                    market_id_b,
                    relation_type,
                    confidence,
                    detected_at,
                    metadata
                ) VALUES ($1, $2, $3, $4, $5, $6::jsonb)
                """,
                args,
            )

    async def get_relations_for_market(self, market_id: str) -> list[MarketRelation]:
        async with self.pool.acquire() as connection:
            rows = await connection.fetch(
                """
                SELECT
                    id,
                    market_id_a,
                    market_id_b,
                    relation_type,
                    confidence,
                    detected_at,
                    metadata
                FROM market_relations
                WHERE market_id_a = $1 OR market_id_b = $1
                ORDER BY detected_at DESC, id ASC
                """,
                market_id,
            )
        return [market_relation_from_row(row) for row in rows]

    async def delete_stale_relations(
        self,
        *,
        ttl: timedelta,
        now: datetime | None = None,
    ) -> int:
        reference_now = now if now is not None else datetime.now(tz=UTC)
        cutoff = reference_now - ttl
        async with self.pool.acquire() as connection:
            result = await connection.execute(
                """
                DELETE FROM market_relations
                WHERE detected_at < $1
                """,
                cutoff,
            )
        return _deleted_count(result)


def market_relation_from_row(row: Mapping[str, Any]) -> MarketRelation:
    return MarketRelation(
        id=cast(int | None, row["id"]),
        market_id_a=cast(str, row["market_id_a"]),
        market_id_b=cast(str, row["market_id_b"]),
        relation_type=MarketRelationType(cast(str, row["relation_type"])),
        confidence=float(cast(float, row["confidence"])),
        detected_at=cast(datetime, row["detected_at"]),
        metadata=_metadata_from_row(row["metadata"]),
    )


def _metadata_json(metadata: Mapping[str, Any]) -> str:
    return json.dumps(
        dict(metadata),
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    )


def _metadata_from_row(value: object) -> Mapping[str, Any]:
    if value is None:
        return {}
    if isinstance(value, str):
        decoded = json.loads(value)
        if isinstance(decoded, dict):
            return cast(dict[str, Any], decoded)
        return {}
    if isinstance(value, Mapping):
        return cast(Mapping[str, Any], value)
    return {}


def _deleted_count(result: str) -> int:
    parts = result.split()
    if not parts:
        return 0
    try:
        return int(parts[-1])
    except ValueError:
        return 0
