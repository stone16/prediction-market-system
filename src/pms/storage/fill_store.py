from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
import json
from typing import Any, cast

import asyncpg

from pms.core.models import FillRecord, Position, Venue


_CREATE_FILL_PAYLOADS_TABLE = """
CREATE TABLE IF NOT EXISTS fill_payloads (
    fill_id TEXT PRIMARY KEY REFERENCES fills(fill_id) ON DELETE CASCADE,
    payload JSONB NOT NULL
)
"""


@dataclass
class FillStore:
    pool: asyncpg.Pool | None = None

    def bind_pool(self, pool: asyncpg.Pool) -> None:
        self.pool = pool

    async def insert(self, fill: FillRecord) -> None:
        fill_id = fill.fill_id or fill.trade_id
        async with self._pool().acquire() as connection:
            await _ensure_fill_payloads_table(connection)
            async with connection.transaction():
                await connection.execute(
                    """
                    INSERT INTO fills (
                        fill_id,
                        order_id,
                        market_id,
                        ts,
                        fill_notional_usdc,
                        fill_quantity,
                        strategy_id,
                        strategy_version_id
                    ) VALUES (
                        $1, $2, $3, $4, $5, $6, $7, $8
                    )
                    ON CONFLICT (fill_id) DO UPDATE
                    SET order_id = EXCLUDED.order_id,
                        market_id = EXCLUDED.market_id,
                        ts = EXCLUDED.ts,
                        fill_notional_usdc = EXCLUDED.fill_notional_usdc,
                        fill_quantity = EXCLUDED.fill_quantity,
                        strategy_id = EXCLUDED.strategy_id,
                        strategy_version_id = EXCLUDED.strategy_version_id
                    """,
                    fill_id,
                    fill.order_id,
                    fill.market_id,
                    fill.filled_at,
                    fill.fill_notional_usdc,
                    fill.fill_quantity,
                    fill.strategy_id,
                    fill.strategy_version_id,
                )
                await connection.execute(
                    """
                    INSERT INTO fill_payloads (fill_id, payload)
                    VALUES ($1, $2::jsonb)
                    ON CONFLICT (fill_id) DO UPDATE
                    SET payload = EXCLUDED.payload
                    """,
                    fill_id,
                    json.dumps(_fill_payload(fill)),
                )

    async def get(self, fill_id: str | None) -> FillRecord | None:
        if self.pool is None or fill_id is None:
            return None

        async with self.pool.acquire() as connection:
            await _ensure_fill_payloads_table(connection)
            row = await connection.fetchrow(
                """
                SELECT
                    fills.fill_id,
                    fills.order_id,
                    fills.market_id,
                    fills.ts,
                    fills.fill_notional_usdc,
                    fills.fill_quantity,
                    fills.strategy_id,
                    fills.strategy_version_id,
                    fill_payloads.payload
                FROM fills
                LEFT JOIN fill_payloads
                    ON fill_payloads.fill_id = fills.fill_id
                WHERE fills.fill_id = $1
                """,
                fill_id,
            )
        if row is None or row["payload"] is None:
            return None
        return _fill_from_row(row)

    async def read_positions(self) -> list[Position]:
        async with self._pool().acquire() as connection:
            await _ensure_fill_payloads_table(connection)
            rows = await connection.fetch(
                """
                WITH raw_positions AS (
                    SELECT
                        fills.market_id,
                        fill_payloads.payload->>'token_id' AS token_id,
                        fill_payloads.payload->>'venue' AS venue,
                        fills.strategy_id,
                        fills.strategy_version_id,
                        SUM(
                            CASE
                                WHEN UPPER(fill_payloads.payload->>'side') = 'SELL'
                                THEN -fills.fill_quantity
                                ELSE fills.fill_quantity
                            END
                        ) AS net_shares,
                        SUM(
                            CASE
                                WHEN UPPER(fill_payloads.payload->>'side') = 'SELL'
                                THEN 0.0
                                ELSE fills.fill_quantity
                            END
                        ) AS buy_shares,
                        SUM(
                            CASE
                                WHEN UPPER(fill_payloads.payload->>'side') = 'SELL'
                                THEN 0.0
                                ELSE fills.fill_notional_usdc
                            END
                        ) AS buy_notional,
                        SUM(
                            CASE
                                WHEN UPPER(fill_payloads.payload->>'side') = 'SELL'
                                THEN fills.fill_quantity
                                ELSE 0.0
                            END
                        ) AS sell_shares,
                        SUM(
                            CASE
                                WHEN UPPER(fill_payloads.payload->>'side') = 'SELL'
                                THEN fills.fill_notional_usdc
                                ELSE 0.0
                            END
                        ) AS sell_notional,
                        MIN(
                            CASE
                                WHEN UPPER(fill_payloads.payload->>'side') = 'SELL'
                                THEN NULL
                                ELSE fill_payloads.payload->>'side'
                            END
                        ) AS long_side,
                        MIN(fills.ts) AS opened_at,
                        MAX(fills.ts) AS last_fill_at
                    FROM fills
                    INNER JOIN fill_payloads
                        ON fill_payloads.fill_id = fills.fill_id
                    GROUP BY
                        fills.market_id,
                        fill_payloads.payload->>'token_id',
                        fill_payloads.payload->>'venue',
                        fills.strategy_id,
                        fills.strategy_version_id
                ),
                valued_positions AS (
                    SELECT
                        raw_positions.market_id,
                        raw_positions.token_id,
                        raw_positions.venue,
                        CASE
                            WHEN raw_positions.net_shares < 0 THEN 'SELL'
                            ELSE COALESCE(raw_positions.long_side, 'BUY')
                        END AS side,
                        raw_positions.strategy_id,
                        raw_positions.strategy_version_id,
                        ABS(raw_positions.net_shares) AS shares_held,
                        CASE
                            WHEN raw_positions.net_shares < 0 THEN
                                CASE
                                    WHEN raw_positions.sell_shares = 0 THEN 0.0
                                    ELSE raw_positions.sell_notional / raw_positions.sell_shares
                                END
                            ELSE
                                CASE
                                    WHEN raw_positions.buy_shares = 0 THEN 0.0
                                    ELSE raw_positions.buy_notional / raw_positions.buy_shares
                                END
                        END AS avg_entry_price,
                        raw_positions.opened_at,
                        raw_positions.last_fill_at
                    FROM raw_positions
                    WHERE ABS(raw_positions.net_shares) > 1e-9
                ),
                aggregated_positions AS (
                    SELECT
                        valued_positions.market_id,
                        valued_positions.token_id,
                        valued_positions.venue,
                        valued_positions.side,
                        valued_positions.strategy_id,
                        valued_positions.strategy_version_id,
                        valued_positions.shares_held,
                        valued_positions.avg_entry_price,
                        (
                            valued_positions.avg_entry_price
                            * valued_positions.shares_held
                        ) AS locked_usdc,
                        valued_positions.opened_at,
                        valued_positions.last_fill_at
                    FROM valued_positions
                )
                SELECT
                    aggregated_positions.market_id,
                    aggregated_positions.token_id,
                    aggregated_positions.venue,
                    aggregated_positions.side,
                    aggregated_positions.strategy_id,
                    aggregated_positions.strategy_version_id,
                    aggregated_positions.shares_held,
                    aggregated_positions.avg_entry_price,
                    aggregated_positions.locked_usdc,
                    aggregated_positions.opened_at,
                    COALESCE(
                        clob_marks.best_bid,
                        CASE
                            WHEN tokens.outcome = 'YES' THEN COALESCE(
                                markets.yes_price::double precision,
                                CASE
                                    WHEN markets.no_price IS NULL THEN NULL
                                    ELSE (1 - markets.no_price)::double precision
                                END
                            )
                            WHEN tokens.outcome = 'NO' THEN COALESCE(
                                markets.no_price::double precision,
                                CASE
                                    WHEN markets.yes_price IS NULL THEN NULL
                                    ELSE (1 - markets.yes_price)::double precision
                                END
                            )
                            ELSE markets.yes_price::double precision
                        END
                    ) AS current_price,
                    CASE
                        WHEN clob_marks.best_bid IS NOT NULL THEN 'clob'
                        ELSE 'gamma'
                    END AS mark_source,
                    CASE
                        WHEN clob_marks.snapshot_ts IS NOT NULL
                        THEN EXTRACT(EPOCH FROM NOW() - clob_marks.snapshot_ts)
                        ELSE NULL
                    END AS mark_age_seconds
                FROM aggregated_positions
                LEFT JOIN LATERAL (
                    SELECT
                        MAX(book_levels.price) AS best_bid,
                        latest_snapshot.snapshot_ts
                    FROM (
                        SELECT
                            book_snapshots.id,
                            book_snapshots.ts AS snapshot_ts
                        FROM book_snapshots
                        WHERE book_snapshots.market_id = aggregated_positions.market_id
                          AND book_snapshots.token_id = aggregated_positions.token_id
                          AND book_snapshots.ts > NOW() - INTERVAL '60 seconds'
                        ORDER BY book_snapshots.ts DESC, book_snapshots.id DESC
                        LIMIT 1
                    ) AS latest_snapshot
                    INNER JOIN book_levels
                       ON book_levels.snapshot_id = latest_snapshot.id
                      AND book_levels.market_id = aggregated_positions.market_id
                      AND book_levels.side = 'BUY'
                    GROUP BY latest_snapshot.snapshot_ts
                ) AS clob_marks ON TRUE
                LEFT JOIN tokens
                    ON tokens.token_id = aggregated_positions.token_id
                LEFT JOIN markets
                    ON markets.condition_id = aggregated_positions.market_id
                ORDER BY aggregated_positions.last_fill_at DESC,
                    aggregated_positions.market_id ASC
                """
            )
        return [_position_from_row(row) for row in rows]

    async def read_trades(self, *, limit: int) -> list["StoredTradeRow"]:
        async with self._pool().acquire() as connection:
            await _ensure_fill_payloads_table(connection)
            rows = await connection.fetch(
                """
                SELECT
                    fills.fill_id,
                    fills.order_id,
                    fills.market_id,
                    fills.ts,
                    fills.fill_notional_usdc,
                    fills.fill_quantity,
                    fills.strategy_id,
                    fills.strategy_version_id,
                    fill_payloads.payload,
                    COALESCE(markets.question, fills.market_id) AS question
                FROM fills
                LEFT JOIN fill_payloads
                    ON fill_payloads.fill_id = fills.fill_id
                LEFT JOIN markets
                    ON markets.condition_id = fills.market_id
                ORDER BY fills.ts DESC, fills.fill_id DESC
                LIMIT $1
                """,
                limit,
            )
        return [
            _trade_from_row(row)
            for row in rows
            if row["payload"] is not None
        ]

    def _pool(self) -> asyncpg.Pool:
        if self.pool is None:
            msg = "FillStore pool is not bound"
            raise RuntimeError(msg)
        return self.pool


@dataclass(frozen=True)
class StoredTradeRow:
    trade_id: str
    fill_id: str
    order_id: str
    decision_id: str
    market_id: str
    question: str
    token_id: str | None
    venue: Venue
    side: str
    fill_price: float
    fill_notional_usdc: float
    fill_quantity: float
    executed_at: datetime
    filled_at: datetime
    status: str
    strategy_id: str
    strategy_version_id: str


async def _ensure_fill_payloads_table(connection: asyncpg.Connection) -> None:
    # The current branch still uses shell rows in `fills`; the sidecar preserves
    # the full runtime object until a later schema checkpoint widens the table.
    await connection.execute(_CREATE_FILL_PAYLOADS_TABLE)


def _fill_payload(fill: FillRecord) -> dict[str, object]:
    return {
        "trade_id": fill.trade_id,
        "decision_id": fill.decision_id,
        "token_id": fill.token_id,
        "venue": fill.venue,
        "side": fill.side,
        "fill_price": fill.fill_price,
        "executed_at": fill.executed_at.isoformat(),
        "status": fill.status,
        "anomaly_flags": list(fill.anomaly_flags),
        "fee_bps": fill.fee_bps,
        "fees": fill.fees,
        "liquidity_side": fill.liquidity_side,
        "transaction_ref": fill.transaction_ref,
        "resolved_outcome": fill.resolved_outcome,
    }


def _fill_from_row(row: asyncpg.Record) -> FillRecord:
    payload = _json_object(row["payload"])
    return FillRecord(
        trade_id=cast(str, payload["trade_id"]),
        fill_id=cast(str, row["fill_id"]),
        order_id=cast(str, row["order_id"]),
        decision_id=cast(str, payload["decision_id"]),
        market_id=cast(str, row["market_id"]),
        token_id=cast(str | None, payload.get("token_id")),
        venue=cast(Venue, payload["venue"]),
        side=cast(str, payload["side"]),
        fill_price=cast(float, payload["fill_price"]),
        fill_notional_usdc=cast(float, row["fill_notional_usdc"]),
        fill_quantity=cast(float, row["fill_quantity"]),
        executed_at=datetime.fromisoformat(cast(str, payload["executed_at"])),
        filled_at=cast(datetime, row["ts"]),
        status=cast(str, payload["status"]),
        anomaly_flags=_string_list(payload.get("anomaly_flags")),
        strategy_id=cast(str, row["strategy_id"]),
        strategy_version_id=cast(str, row["strategy_version_id"]),
        fee_bps=cast(int | None, payload.get("fee_bps")),
        fees=cast(float | None, payload.get("fees")),
        liquidity_side=cast(str | None, payload.get("liquidity_side")),
        transaction_ref=cast(str | None, payload.get("transaction_ref")),
        resolved_outcome=cast(float | None, payload.get("resolved_outcome")),
    )


def _position_from_row(row: asyncpg.Record) -> Position:
    return Position(
        market_id=cast(str, row["market_id"]),
        token_id=cast(str | None, row["token_id"]),
        venue=cast(Venue, row["venue"]),
        side=cast(str, row["side"]),
        shares_held=float(cast(float, row["shares_held"])),
        avg_entry_price=float(cast(float, row["avg_entry_price"])),
        unrealized_pnl=_unrealized_pnl_from_row(row),
        locked_usdc=float(cast(float, row["locked_usdc"])),
        mark_source=cast(str | None, _optional_row_value(row, "mark_source")),
        mark_age_seconds=_float_or_none(_optional_row_value(row, "mark_age_seconds")),
        current_price=_float_or_none(_optional_row_value(row, "current_price")),
        opened_at=cast(datetime | None, _optional_row_value(row, "opened_at")),
        strategy_id=cast(str, _optional_row_value(row, "strategy_id") or "default"),
        strategy_version_id=cast(
            str,
            _optional_row_value(row, "strategy_version_id") or "default-v1",
        ),
    )


def _unrealized_pnl_from_row(row: asyncpg.Record) -> float:
    current_price = _decimal_or_none(_optional_row_value(row, "current_price"))
    if current_price is None:
        return 0.0

    shares_held = Decimal(str(row["shares_held"]))
    avg_entry_price = Decimal(str(row["avg_entry_price"]))
    if str(row["side"]).upper() == "SELL":
        pnl = (avg_entry_price - current_price) * shares_held
    else:
        pnl = (current_price - avg_entry_price) * shares_held
    return float(pnl)


def _optional_row_value(row: asyncpg.Record, key: str) -> object | None:
    try:
        return cast(object, row[key])
    except KeyError:
        return None


def _decimal_or_none(value: object | None) -> Decimal | None:
    if value is None:
        return None
    return Decimal(str(value))


def _float_or_none(value: object | None) -> float | None:
    if value is None:
        return None
    return float(cast(float, value))


def _trade_from_row(row: asyncpg.Record) -> StoredTradeRow:
    payload = _json_object(row["payload"])
    return StoredTradeRow(
        trade_id=cast(str, payload["trade_id"]),
        fill_id=cast(str, row["fill_id"]),
        order_id=cast(str, row["order_id"]),
        decision_id=cast(str, payload["decision_id"]),
        market_id=cast(str, row["market_id"]),
        question=cast(str, row["question"]),
        token_id=cast(str | None, payload.get("token_id")),
        venue=cast(Venue, payload["venue"]),
        side=cast(str, payload["side"]),
        fill_price=cast(float, payload["fill_price"]),
        fill_notional_usdc=cast(float, row["fill_notional_usdc"]),
        fill_quantity=cast(float, row["fill_quantity"]),
        executed_at=datetime.fromisoformat(cast(str, payload["executed_at"])),
        filled_at=cast(datetime, row["ts"]),
        status=cast(str, payload["status"]),
        strategy_id=cast(str, row["strategy_id"]),
        strategy_version_id=cast(str, row["strategy_version_id"]),
    )


def _json_object(value: object) -> dict[str, Any]:
    if isinstance(value, dict):
        return cast(dict[str, Any], value)
    if isinstance(value, str):
        loaded = json.loads(value)
        if isinstance(loaded, dict):
            return cast(dict[str, Any], loaded)
    msg = "fill payload must be a JSON object"
    raise RuntimeError(msg)


def _string_list(value: object) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value]
    return []
