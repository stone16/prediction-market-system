from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import json
from typing import Any, cast

import asyncpg

from pms.core.models import OrderState, Venue


_CREATE_ORDER_PAYLOADS_TABLE = """
CREATE TABLE IF NOT EXISTS order_payloads (
    order_id TEXT PRIMARY KEY REFERENCES orders(order_id) ON DELETE CASCADE,
    payload JSONB NOT NULL
)
"""


@dataclass
class OrderStore:
    pool: asyncpg.Pool | None = None

    def bind_pool(self, pool: asyncpg.Pool) -> None:
        self.pool = pool

    async def insert(self, order: OrderState) -> None:
        async with self._pool().acquire() as connection:
            await _ensure_order_payloads_table(connection)
            async with connection.transaction():
                await connection.execute(
                    """
                    INSERT INTO orders (
                        order_id,
                        market_id,
                        ts,
                        requested_notional_usdc,
                        filled_notional_usdc,
                        remaining_notional_usdc,
                        filled_quantity,
                        strategy_id,
                        strategy_version_id
                    ) VALUES (
                        $1, $2, $3, $4, $5, $6, $7, $8, $9
                    )
                    ON CONFLICT (order_id) DO UPDATE
                    SET market_id = EXCLUDED.market_id,
                        ts = EXCLUDED.ts,
                        requested_notional_usdc = EXCLUDED.requested_notional_usdc,
                        filled_notional_usdc = EXCLUDED.filled_notional_usdc,
                        remaining_notional_usdc = EXCLUDED.remaining_notional_usdc,
                        filled_quantity = EXCLUDED.filled_quantity,
                        strategy_id = EXCLUDED.strategy_id,
                        strategy_version_id = EXCLUDED.strategy_version_id
                    """,
                    order.order_id,
                    order.market_id,
                    order.submitted_at,
                    order.requested_notional_usdc,
                    order.filled_notional_usdc,
                    order.remaining_notional_usdc,
                    order.filled_quantity,
                    order.strategy_id,
                    order.strategy_version_id,
                )
                await connection.execute(
                    """
                    INSERT INTO order_payloads (order_id, payload)
                    VALUES ($1, $2::jsonb)
                    ON CONFLICT (order_id) DO UPDATE
                    SET payload = EXCLUDED.payload
                    """,
                    order.order_id,
                    json.dumps(_order_payload(order)),
                )

    async def get(self, order_id: str) -> OrderState | None:
        if self.pool is None:
            return None

        async with self.pool.acquire() as connection:
            await _ensure_order_payloads_table(connection)
            row = await connection.fetchrow(
                """
                SELECT
                    orders.order_id,
                    orders.market_id,
                    orders.ts,
                    orders.requested_notional_usdc,
                    orders.filled_notional_usdc,
                    orders.remaining_notional_usdc,
                    orders.filled_quantity,
                    orders.strategy_id,
                    orders.strategy_version_id,
                    order_payloads.payload
                FROM orders
                LEFT JOIN order_payloads
                    ON order_payloads.order_id = orders.order_id
                WHERE orders.order_id = $1
                """,
                order_id,
            )
        if row is None or row["payload"] is None:
            return None
        return _order_from_row(row)

    def _pool(self) -> asyncpg.Pool:
        if self.pool is None:
            msg = "OrderStore pool is not bound"
            raise RuntimeError(msg)
        return self.pool


async def _ensure_order_payloads_table(connection: asyncpg.Connection) -> None:
    # The current branch still uses shell rows in `orders`; the sidecar preserves
    # the full runtime object until a later schema checkpoint widens the table.
    await connection.execute(_CREATE_ORDER_PAYLOADS_TABLE)


def _order_payload(order: OrderState) -> dict[str, object]:
    return {
        "decision_id": order.decision_id,
        "status": order.status,
        "token_id": order.token_id,
        "venue": order.venue,
        "fill_price": order.fill_price,
        "last_updated_at": order.last_updated_at.isoformat(),
        "raw_status": order.raw_status,
        "pre_submit_quote": dict(order.pre_submit_quote),
    }


def _order_from_row(row: asyncpg.Record) -> OrderState:
    payload = _json_object(row["payload"])
    return OrderState(
        order_id=cast(str, row["order_id"]),
        decision_id=cast(str, payload["decision_id"]),
        status=cast(str, payload["status"]),
        market_id=cast(str, row["market_id"]),
        token_id=cast(str | None, payload.get("token_id")),
        venue=cast(Venue, payload["venue"]),
        requested_notional_usdc=cast(float, row["requested_notional_usdc"]),
        filled_notional_usdc=cast(float, row["filled_notional_usdc"]),
        remaining_notional_usdc=cast(float, row["remaining_notional_usdc"]),
        fill_price=cast(float | None, payload.get("fill_price")),
        submitted_at=cast(datetime, row["ts"]),
        last_updated_at=datetime.fromisoformat(cast(str, payload["last_updated_at"])),
        raw_status=cast(str, payload["raw_status"]),
        strategy_id=cast(str, row["strategy_id"]),
        strategy_version_id=cast(str, row["strategy_version_id"]),
        filled_quantity=cast(float, row["filled_quantity"]),
        pre_submit_quote=cast(
            dict[str, Any],
            payload.get("pre_submit_quote", {}),
        ),
    )


def _json_object(value: object) -> dict[str, Any]:
    if isinstance(value, dict):
        return cast(dict[str, Any], value)
    if isinstance(value, str):
        loaded = json.loads(value)
        if isinstance(loaded, dict):
            return cast(dict[str, Any], loaded)
    msg = "order payload must be a JSON object"
    raise RuntimeError(msg)
