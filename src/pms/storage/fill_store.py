from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import json
from typing import Any, cast

import asyncpg

from pms.core.models import FillRecord, Venue


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

    def _pool(self) -> asyncpg.Pool:
        if self.pool is None:
            msg = "FillStore pool is not bound"
            raise RuntimeError(msg)
        return self.pool


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
