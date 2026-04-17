from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import json
from typing import Any, cast

import asyncpg

from pms.core.models import Feedback
from pms.storage.strategy_tags import resolve_strategy_tags


_SELECT_FEEDBACK_COLUMNS = """
SELECT
    feedback_id,
    target,
    source,
    message,
    severity,
    created_at,
    resolved,
    resolved_at,
    category,
    metadata
FROM feedback
"""


@dataclass
class FeedbackStore:
    pool: asyncpg.Pool | None = None

    def bind_pool(self, pool: asyncpg.Pool) -> None:
        self.pool = pool

    async def append(
        self,
        feedback: Feedback,
        *,
        strategy_id: str = "default",
        strategy_version_id: str | None = None,
    ) -> None:
        async with self._pool().acquire() as connection:
            await insert_feedback_row(
                connection,
                feedback,
                strategy_id=strategy_id,
                strategy_version_id=strategy_version_id,
            )

    async def all(self) -> list[Feedback]:
        return await self.list()

    async def list(self, resolved: bool | None = None) -> list[Feedback]:
        if self.pool is None:
            return []

        query = (
            f"{_SELECT_FEEDBACK_COLUMNS}\n"
            "WHERE ($1::boolean IS NULL OR resolved = $1)\n"
            "ORDER BY created_at ASC, feedback_id ASC"
        )
        async with self.pool.acquire() as connection:
            rows = await connection.fetch(query, resolved)
        return [_feedback_from_record(row) for row in rows]

    async def get(self, feedback_id: str) -> Feedback | None:
        if self.pool is None:
            return None

        query = (
            f"{_SELECT_FEEDBACK_COLUMNS}\n"
            "WHERE feedback_id = $1"
        )
        async with self.pool.acquire() as connection:
            row = await connection.fetchrow(query, feedback_id)
        if row is None:
            return None
        return _feedback_from_record(row)

    async def resolve(self, feedback_id: str) -> None:
        if self.pool is None:
            return
        async with self.pool.acquire() as connection:
            await connection.execute(
                """
                UPDATE feedback
                SET resolved = TRUE, resolved_at = now()
                WHERE feedback_id = $1
                """,
                feedback_id,
            )

    def _pool(self) -> asyncpg.Pool:
        if self.pool is None:
            msg = "FeedbackStore pool is not bound"
            raise RuntimeError(msg)
        return self.pool


async def insert_feedback_row(
    connection: asyncpg.Connection,
    feedback: Feedback,
    *,
    strategy_id: str = "default",
    strategy_version_id: str | None = None,
) -> None:
    resolved_strategy_id, resolved_strategy_version_id = await resolve_strategy_tags(
        connection,
        strategy_id=strategy_id,
        strategy_version_id=strategy_version_id,
    )
    await connection.execute(
        """
        INSERT INTO feedback (
            feedback_id,
            target,
            source,
            message,
            severity,
            created_at,
            resolved,
            resolved_at,
            category,
            metadata,
            strategy_id,
            strategy_version_id
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb, $11, $12
        )
        """,
        feedback.feedback_id,
        feedback.target,
        feedback.source,
        feedback.message,
        feedback.severity,
        feedback.created_at,
        feedback.resolved,
        feedback.resolved_at,
        feedback.category,
        json.dumps(feedback.metadata),
        resolved_strategy_id,
        resolved_strategy_version_id,
    )


def _feedback_from_record(record: asyncpg.Record) -> Feedback:
    return Feedback(
        feedback_id=cast(str, record["feedback_id"]),
        target=cast(str, record["target"]),
        source=cast(str, record["source"]),
        message=cast(str, record["message"]),
        severity=cast(str, record["severity"]),
        created_at=cast(datetime, record["created_at"]),
        resolved=cast(bool, record["resolved"]),
        resolved_at=cast(datetime | None, record["resolved_at"]),
        category=cast(str | None, record["category"]),
        metadata=_metadata_from_value(record["metadata"]),
    )


def _metadata_from_value(value: object) -> dict[str, Any]:
    if isinstance(value, dict):
        return cast(dict[str, Any], value)
    if isinstance(value, str):
        loaded = json.loads(value)
        if isinstance(loaded, dict):
            return cast(dict[str, Any], loaded)
    return {}
