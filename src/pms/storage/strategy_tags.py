from __future__ import annotations

import asyncpg


async def resolve_strategy_tags(
    connection: asyncpg.Connection,
    *,
    strategy_id: str,
    strategy_version_id: str | None,
) -> tuple[str, str]:
    if strategy_version_id is not None:
        return strategy_id, strategy_version_id

    active_version_id = await connection.fetchval(
        """
        SELECT active_version_id
        FROM strategies
        WHERE strategy_id = $1
        """,
        strategy_id,
    )
    if not isinstance(active_version_id, str):
        msg = f"strategy {strategy_id!r} has no active version"
        raise RuntimeError(msg)
    return strategy_id, active_version_id
