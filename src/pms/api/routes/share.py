from __future__ import annotations

from collections.abc import Mapping
from typing import Any, cast

import asyncpg
from pydantic import BaseModel


SHARE_NOT_FOUND_DETAIL = "This strategy doesn't exist or has been unshared"


class ShareResponse(BaseModel):
    strategy_id: str
    title: str | None
    description: str | None
    brier_overall: float | None
    trade_count: int
    version_id_short: str | None


async def get_shared_strategy(
    pg_pool: asyncpg.Pool,
    strategy_id: str,
) -> ShareResponse | None:
    async with pg_pool.acquire() as connection:
        return await public_strategy_projection(connection, strategy_id)


async def public_strategy_projection(
    connection: Any,
    strategy_id: str,
) -> ShareResponse | None:
    row = await connection.fetchrow(
        """
        SELECT
            strategies.strategy_id,
            strategies.title,
            strategies.description,
            eval_stats.brier_overall,
            COALESCE(fill_stats.trade_count, 0) AS trade_count,
            CASE
                WHEN strategies.active_version_id IS NULL THEN NULL
                ELSE substring(strategies.active_version_id FROM 1 FOR 8)
            END AS version_id_short
        FROM strategies
        LEFT JOIN (
            SELECT
                strategy_id,
                strategy_version_id,
                AVG(brier_score)::double precision AS brier_overall
            FROM eval_records
            GROUP BY strategy_id, strategy_version_id
        ) AS eval_stats
            ON eval_stats.strategy_id = strategies.strategy_id
           AND eval_stats.strategy_version_id = strategies.active_version_id
        LEFT JOIN (
            SELECT
                strategy_id,
                strategy_version_id,
                COUNT(*)::integer AS trade_count
            FROM fills
            GROUP BY strategy_id, strategy_version_id
        ) AS fill_stats
            ON fill_stats.strategy_id = strategies.strategy_id
           AND fill_stats.strategy_version_id = strategies.active_version_id
        WHERE strategies.strategy_id = $1
          AND strategies.archived IS NOT TRUE
          AND strategies.share_enabled IS NOT FALSE
        """,
        strategy_id,
    )
    if row is None:
        return None
    return _share_response_from_row(cast(Mapping[str, object], row))


def _share_response_from_row(row: Mapping[str, object]) -> ShareResponse:
    return ShareResponse(
        strategy_id=str(row["strategy_id"]),
        title=_optional_string(row.get("title")),
        description=_optional_string(row.get("description")),
        brier_overall=_optional_float(row.get("brier_overall")),
        trade_count=_required_int(row["trade_count"]),
        version_id_short=_optional_string(row.get("version_id_short")),
    )


def _optional_string(value: object | None) -> str | None:
    if value is None:
        return None
    return str(value)


def _optional_float(value: object | None) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float, str)):
        return float(value)
    msg = f"expected float-like value, got {type(value).__name__}"
    raise TypeError(msg)


def _required_int(value: object) -> int:
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        return int(value)
    msg = f"expected int-like value, got {type(value).__name__}"
    raise TypeError(msg)
