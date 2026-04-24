from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import cast

import asyncpg


@dataclass(frozen=True)
class MarketSubscriptionRow:
    token_id: str
    condition_id: str
    source: str
    created_at: datetime


class PostgresMarketSubscriptionStore:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def upsert_user_subscription(
        self,
        token_id: str,
    ) -> MarketSubscriptionRow | None:
        async with self._pool.acquire() as connection:
            async with connection.transaction():
                token_row = await connection.fetchrow(
                    """
                    SELECT token_id, condition_id
                    FROM tokens
                    WHERE token_id = $1
                    """,
                    token_id,
                )
                if token_row is None:
                    return None

                await connection.execute(
                    """
                    INSERT INTO market_subscriptions (token_id, source)
                    VALUES ($1, 'user')
                    ON CONFLICT (token_id) DO NOTHING
                    """,
                    token_id,
                )
                row = await connection.fetchrow(
                    """
                    SELECT
                        market_subscriptions.token_id,
                        tokens.condition_id,
                        market_subscriptions.source,
                        market_subscriptions.created_at
                    FROM market_subscriptions
                    INNER JOIN tokens
                        ON tokens.token_id = market_subscriptions.token_id
                    WHERE market_subscriptions.token_id = $1
                    """,
                    token_id,
                )
        if row is None:
            return None
        return _row_from_record(row)

    async def delete_user_subscription(self, token_id: str) -> bool:
        async with self._pool.acquire() as connection:
            row = await connection.fetchrow(
                """
                DELETE FROM market_subscriptions
                WHERE token_id = $1
                RETURNING token_id
                """,
                token_id,
            )
        return row is not None

    async def read_user_subscriptions(self) -> set[str]:
        async with self._pool.acquire() as connection:
            rows = await connection.fetch(
                """
                SELECT token_id
                FROM market_subscriptions
                WHERE source = 'user'
                ORDER BY token_id ASC
                """
            )
        return {cast(str, row["token_id"]) for row in rows}

    async def read_token_condition_id(self, token_id: str) -> str | None:
        async with self._pool.acquire() as connection:
            row = await connection.fetchrow(
                """
                SELECT condition_id
                FROM tokens
                WHERE token_id = $1
                """,
                token_id,
            )
        if row is None:
            return None
        return cast(str, row["condition_id"])


def _row_from_record(row: asyncpg.Record) -> MarketSubscriptionRow:
    return MarketSubscriptionRow(
        token_id=cast(str, row["token_id"]),
        condition_id=cast(str, row["condition_id"]),
        source=cast(str, row["source"]),
        created_at=cast(datetime, row["created_at"]),
    )
