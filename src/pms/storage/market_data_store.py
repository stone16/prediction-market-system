from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import cast

import asyncpg

from pms.core.models import (
    BookLevel,
    BookSnapshot,
    Market,
    Outcome,
    PriceChange,
    Token,
    Trade,
    Venue,
)


class PostgresMarketDataStore:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    @property
    def pool(self) -> asyncpg.Pool:
        return self._pool

    async def read_market(self, market_id: str) -> Market | None:
        query = """
        SELECT condition_id, slug, question, venue, resolves_at, created_at, last_seen_at, volume_24h
        FROM markets
        WHERE condition_id = $1
        """
        async with self._pool.acquire() as connection:
            row = await connection.fetchrow(query, market_id)
        if row is None:
            return None
        return Market(
            condition_id=row["condition_id"],
            slug=row["slug"],
            question=row["question"],
            venue=row["venue"],
            resolves_at=row["resolves_at"],
            created_at=row["created_at"],
            last_seen_at=row["last_seen_at"],
            volume_24h=row["volume_24h"],
        )

    async def read_tokens_for_market(self, market_id: str) -> list[Token]:
        query = """
        SELECT token_id, condition_id, outcome
        FROM tokens
        WHERE condition_id = $1
        ORDER BY CASE outcome WHEN 'YES' THEN 0 ELSE 1 END, token_id ASC
        """
        async with self._pool.acquire() as connection:
            rows = await connection.fetch(query, market_id)
        return [
            Token(
                token_id=row["token_id"],
                condition_id=row["condition_id"],
                outcome=row["outcome"],
            )
            for row in rows
        ]

    async def read_eligible_markets(
        self,
        venue: str,
        max_horizon_days: int | None,
        min_volume_usdc: float,
    ) -> list[tuple[Market, list[Token]]]:
        now = datetime.now(tz=UTC)
        upper_bound = (
            None
            if max_horizon_days is None
            else now + timedelta(days=max_horizon_days)
        )
        query = """
        SELECT
            markets.condition_id,
            markets.slug,
            markets.question,
            markets.venue,
            markets.resolves_at,
            markets.created_at,
            markets.last_seen_at,
            markets.volume_24h,
            tokens.token_id,
            tokens.outcome
        FROM markets
        LEFT JOIN tokens
            ON tokens.condition_id = markets.condition_id
        WHERE markets.venue = $1
          AND (
                $4::double precision <= 0
                OR (
                    markets.volume_24h IS NOT NULL
                    AND markets.volume_24h >= $4
                )
          )
          AND (
                (
                    $3::timestamptz IS NULL
                    AND (
                        markets.resolves_at IS NULL
                        OR markets.resolves_at > $2
                    )
                )
                OR (
                    $3::timestamptz IS NOT NULL
                    AND markets.resolves_at IS NOT NULL
                    AND markets.resolves_at > $2
                    AND markets.resolves_at <= $3
                )
          )
        ORDER BY
            markets.condition_id ASC,
            CASE tokens.outcome WHEN 'YES' THEN 0 WHEN 'NO' THEN 1 ELSE 2 END,
            tokens.token_id ASC
        """
        async with self._pool.acquire() as connection:
            rows = await connection.fetch(query, venue, now, upper_bound, min_volume_usdc)

        eligible_markets: list[tuple[Market, list[Token]]] = []
        current_market_id: str | None = None
        current_tokens: list[Token] | None = None

        for row in rows:
            market_id = cast(str, row["condition_id"])
            if market_id != current_market_id:
                current_market_id = market_id
                current_tokens = []
                eligible_markets.append(
                    (
                        Market(
                            condition_id=market_id,
                            slug=cast(str, row["slug"]),
                            question=cast(str, row["question"]),
                            venue=cast(Venue, row["venue"]),
                            resolves_at=cast(datetime | None, row["resolves_at"]),
                            created_at=cast(datetime, row["created_at"]),
                            last_seen_at=cast(datetime, row["last_seen_at"]),
                            volume_24h=cast(float | None, row["volume_24h"]),
                        ),
                        current_tokens,
                    )
                )

            token_id = row["token_id"]
            outcome = row["outcome"]
            if (
                current_tokens is not None
                and token_id is not None
                and outcome is not None
            ):
                current_tokens.append(
                    Token(
                        token_id=cast(str, token_id),
                        condition_id=market_id,
                        outcome=cast(Outcome, outcome),
                    )
                )

        return eligible_markets

    async def write_market(self, market: Market) -> None:
        query = """
        INSERT INTO markets (
            condition_id,
            slug,
            question,
            venue,
            resolves_at,
            created_at,
            last_seen_at,
            volume_24h
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT (condition_id) DO UPDATE
        SET slug = EXCLUDED.slug,
            question = EXCLUDED.question,
            venue = EXCLUDED.venue,
            resolves_at = EXCLUDED.resolves_at,
            last_seen_at = EXCLUDED.last_seen_at,
            volume_24h = EXCLUDED.volume_24h
        """
        async with self._pool.acquire() as connection:
            await connection.execute(
                query,
                market.condition_id,
                market.slug,
                market.question,
                market.venue,
                market.resolves_at,
                market.created_at,
                market.last_seen_at,
                market.volume_24h,
            )

    async def write_token(self, token: Token) -> None:
        query = """
        INSERT INTO tokens (
            token_id,
            condition_id,
            outcome
        ) VALUES ($1, $2, $3)
        ON CONFLICT (token_id) DO UPDATE
        SET condition_id = EXCLUDED.condition_id,
            outcome = EXCLUDED.outcome
        """
        async with self._pool.acquire() as connection:
            await connection.execute(
                query,
                token.token_id,
                token.condition_id,
                token.outcome,
            )

    async def write_book_snapshot(
        self,
        snapshot: BookSnapshot,
        levels: list[BookLevel],
    ) -> int:
        snapshot_query = """
        INSERT INTO book_snapshots (
            market_id,
            token_id,
            ts,
            hash,
            source
        ) VALUES ($1, $2, $3, $4, $5)
        RETURNING id
        """
        level_query = """
        INSERT INTO book_levels (
            snapshot_id,
            market_id,
            side,
            price,
            size
        ) VALUES ($1, $2, $3, $4, $5)
        """
        async with self._pool.acquire() as connection:
            async with connection.transaction():
                snapshot_id = await connection.fetchval(
                    snapshot_query,
                    snapshot.market_id,
                    snapshot.token_id,
                    snapshot.ts,
                    snapshot.hash,
                    snapshot.source,
                )
                if not isinstance(snapshot_id, int):
                    msg = "book_snapshots.id did not return an integer"
                    raise TypeError(msg)
                for level in levels:
                    await connection.execute(
                        level_query,
                        snapshot_id,
                        level.market_id,
                        level.side,
                        level.price,
                        level.size,
                    )
        return snapshot_id

    async def write_price_change(self, price_change: PriceChange) -> None:
        query = """
        INSERT INTO price_changes (
            market_id,
            token_id,
            ts,
            side,
            price,
            size,
            best_bid,
            best_ask,
            hash
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        """
        async with self._pool.acquire() as connection:
            await connection.execute(
                query,
                price_change.market_id,
                price_change.token_id,
                price_change.ts,
                price_change.side,
                price_change.price,
                price_change.size,
                price_change.best_bid,
                price_change.best_ask,
                price_change.hash,
            )

    async def write_trade(self, trade: Trade) -> None:
        query = """
        INSERT INTO trades (
            market_id,
            token_id,
            ts,
            price
        ) VALUES ($1, $2, $3, $4)
        """
        async with self._pool.acquire() as connection:
            await connection.execute(
                query,
                trade.market_id,
                trade.token_id,
                trade.ts,
                trade.price,
            )

    async def read_latest_snapshot(
        self,
        market_id: str,
        token_id: str,
    ) -> BookSnapshot | None:
        query = """
        SELECT id, market_id, token_id, ts, hash, source
        FROM book_snapshots
        WHERE market_id = $1 AND token_id = $2
        ORDER BY ts DESC, id DESC
        LIMIT 1
        """
        async with self._pool.acquire() as connection:
            row = await connection.fetchrow(query, market_id, token_id)
        if row is None:
            return None
        return BookSnapshot(
            id=row["id"],
            market_id=row["market_id"],
            token_id=row["token_id"],
            ts=row["ts"],
            hash=row["hash"],
            source=row["source"],
        )

    async def read_latest_book_snapshot(self, market_id: str) -> BookSnapshot | None:
        query = """
        SELECT id, market_id, token_id, ts, hash, source
        FROM book_snapshots
        WHERE market_id = $1
        ORDER BY ts DESC, id DESC
        LIMIT 1
        """
        async with self._pool.acquire() as connection:
            row = await connection.fetchrow(query, market_id)
        if row is None:
            return None
        return BookSnapshot(
            id=row["id"],
            market_id=row["market_id"],
            token_id=row["token_id"],
            ts=row["ts"],
            hash=row["hash"],
            source=row["source"],
        )

    async def read_levels_for_snapshot(self, snapshot_id: int) -> list[BookLevel]:
        query = """
        SELECT snapshot_id, market_id, side, price, size
        FROM book_levels
        WHERE snapshot_id = $1
        ORDER BY
            CASE side WHEN 'BUY' THEN 0 ELSE 1 END,
            price DESC
        """
        async with self._pool.acquire() as connection:
            rows = await connection.fetch(query, snapshot_id)
        return [
            BookLevel(
                snapshot_id=row["snapshot_id"],
                market_id=row["market_id"],
                side=row["side"],
                price=row["price"],
                size=row["size"],
            )
            for row in rows
        ]

    async def read_price_changes_since(
        self,
        market_id: str,
        token_id: str,
        since_ts: object,
    ) -> list[PriceChange]:
        query = """
        SELECT id, market_id, token_id, ts, side, price, size, best_bid, best_ask, hash
        FROM price_changes
        WHERE market_id = $1 AND token_id = $2 AND ts >= $3
        ORDER BY ts ASC, id ASC
        """
        async with self._pool.acquire() as connection:
            rows = await connection.fetch(query, market_id, token_id, since_ts)
        return [
            PriceChange(
                id=row["id"],
                market_id=row["market_id"],
                token_id=row["token_id"],
                ts=row["ts"],
                side=row["side"],
                price=row["price"],
                size=row["size"],
                best_bid=row["best_bid"],
                best_ask=row["best_ask"],
                hash=row["hash"],
            )
            for row in rows
        ]
