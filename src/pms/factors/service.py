from __future__ import annotations

import asyncpg

from pms.factors.base import FactorValueRow


async def persist_factor_value(pool: asyncpg.Pool, row: FactorValueRow) -> None:
    query = """
    INSERT INTO factor_values (
        factor_id,
        param,
        market_id,
        ts,
        value
    ) VALUES ($1, $2, $3, $4, $5)
    """
    async with pool.acquire() as connection:
        await connection.execute(
            query,
            row.factor_id,
            row.param,
            row.market_id,
            row.ts,
            row.value,
        )
