from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
import json
from typing import Any, Literal, cast

import asyncpg

from pms.core.models import Opportunity


@dataclass
class OpportunityStore:
    pool: asyncpg.Pool | None = None

    def bind_pool(self, pool: asyncpg.Pool) -> None:
        self.pool = pool

    async def insert(self, opportunity: Opportunity) -> None:
        if self.pool is None or not hasattr(self.pool, "acquire"):
            return

        async with self.pool.acquire() as connection:
            await connection.execute(
                """
                INSERT INTO opportunities (
                    opportunity_id,
                    market_id,
                    token_id,
                    side,
                    selected_factor_values,
                    expected_edge,
                    rationale,
                    target_size_usdc,
                    expiry,
                    staleness_policy,
                    strategy_id,
                    strategy_version_id,
                    created_at,
                    factor_snapshot_hash,
                    composition_trace
                ) VALUES (
                    $1, $2, $3, $4, $5::jsonb, $6, $7, $8, $9, $10, $11, $12, $13,
                    $14, $15::jsonb
                )
                """,
                opportunity.opportunity_id,
                opportunity.market_id,
                opportunity.token_id,
                opportunity.side,
                json.dumps(dict(opportunity.selected_factor_values)),
                opportunity.expected_edge,
                opportunity.rationale,
                opportunity.target_size_usdc,
                opportunity.expiry,
                opportunity.staleness_policy,
                opportunity.strategy_id,
                opportunity.strategy_version_id,
                opportunity.created_at,
                opportunity.factor_snapshot_hash,
                json.dumps(dict(opportunity.composition_trace)),
            )

    async def all(self) -> list[Opportunity]:
        if self.pool is None or not hasattr(self.pool, "acquire"):
            return []

        async with self.pool.acquire() as connection:
            rows = await connection.fetch(
                """
                SELECT
                    opportunity_id,
                    market_id,
                    token_id,
                    side,
                    selected_factor_values,
                    expected_edge,
                    rationale,
                    target_size_usdc,
                    expiry,
                    staleness_policy,
                    strategy_id,
                    strategy_version_id,
                    created_at,
                    factor_snapshot_hash,
                    composition_trace
                FROM opportunities
                ORDER BY created_at ASC, opportunity_id ASC
                """
            )
        return [_opportunity_from_row(row) for row in rows]


def _opportunity_from_row(row: asyncpg.Record) -> Opportunity:
    raw_values = row["selected_factor_values"]
    if isinstance(raw_values, str):
        decoded = json.loads(raw_values)
    else:
        decoded = raw_values
    selected_factor_values = {
        str(key): float(value)
        for key, value in cast(dict[str, object], decoded).items()
        if isinstance(value, (int, float)) and not isinstance(value, bool)
    }
    composition_trace = _json_object(row["composition_trace"])
    return Opportunity(
        opportunity_id=cast(str, row["opportunity_id"]),
        market_id=cast(str, row["market_id"]),
        token_id=cast(str, row["token_id"]),
        side=cast(Literal["yes", "no"], row["side"]),
        selected_factor_values=selected_factor_values,
        expected_edge=cast(float, row["expected_edge"]),
        rationale=cast(str, row["rationale"]),
        target_size_usdc=cast(float, row["target_size_usdc"]),
        expiry=cast(datetime | None, row["expiry"]),
        staleness_policy=cast(str, row["staleness_policy"]),
        strategy_id=cast(str, row["strategy_id"]),
        strategy_version_id=cast(str, row["strategy_version_id"]),
        created_at=cast(datetime, row["created_at"]),
        factor_snapshot_hash=cast(str | None, row["factor_snapshot_hash"]),
        composition_trace=composition_trace,
    )


def _json_object(value: object) -> Mapping[str, Any]:
    if isinstance(value, str):
        decoded = json.loads(value)
        if isinstance(decoded, dict):
            return cast(dict[str, Any], decoded)
        return {}
    if isinstance(value, dict):
        return cast(dict[str, Any], value)
    return {}
