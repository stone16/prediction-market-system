from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from hashlib import sha256
import json
from typing import Protocol

import asyncpg

from pms.strategies.projections import FactorCompositionStep


FactorKey = tuple[str, str]


@dataclass(frozen=True, slots=True)
class FactorSnapshot:
    values: Mapping[FactorKey, float]
    missing_factors: tuple[FactorKey, ...] = ()
    snapshot_hash: str | None = None


class FactorSnapshotReader(Protocol):
    async def snapshot(
        self,
        *,
        market_id: str,
        as_of: datetime,
        required: Sequence[FactorCompositionStep],
        strategy_id: str,
        strategy_version_id: str,
    ) -> FactorSnapshot: ...


@dataclass(frozen=True, slots=True)
class NullFactorSnapshotReader:
    async def snapshot(
        self,
        *,
        market_id: str,
        as_of: datetime,
        required: Sequence[FactorCompositionStep],
        strategy_id: str,
        strategy_version_id: str,
    ) -> FactorSnapshot:
        missing_factors = tuple(
            dict.fromkeys((step.factor_id, step.param) for step in required)
        )
        return FactorSnapshot(
            values={},
            missing_factors=missing_factors,
            snapshot_hash=_snapshot_hash(
                market_id=market_id,
                as_of=as_of,
                strategy_id=strategy_id,
                strategy_version_id=strategy_version_id,
                required_keys=missing_factors,
                values={},
                missing_factors=missing_factors,
            ),
        )


@dataclass(frozen=True, slots=True)
class PostgresFactorSnapshotReader:
    pool: asyncpg.Pool

    async def snapshot(
        self,
        *,
        market_id: str,
        as_of: datetime,
        required: Sequence[FactorCompositionStep],
        strategy_id: str,
        strategy_version_id: str,
    ) -> FactorSnapshot:
        required_keys = tuple(
            dict.fromkeys((step.factor_id, step.param) for step in required)
        )
        if not required_keys:
            return FactorSnapshot(
                values={},
                missing_factors=(),
                snapshot_hash=_snapshot_hash(
                    market_id=market_id,
                    as_of=as_of,
                    strategy_id=strategy_id,
                    strategy_version_id=strategy_version_id,
                    required_keys=(),
                    values={},
                    missing_factors=(),
                ),
            )

        factor_ids = [factor_id for factor_id, _ in required_keys]
        params = [param for _, param in required_keys]
        async with self.pool.acquire() as connection:
            rows = await connection.fetch(
                """
                WITH required(factor_id, param) AS (
                    SELECT * FROM unnest($3::text[], $4::text[])
                ),
                latest AS (
                    SELECT DISTINCT ON (fv.factor_id, fv.param)
                        fv.factor_id,
                        fv.param,
                        fv.value
                    FROM factor_values AS fv
                    INNER JOIN required
                        ON required.factor_id = fv.factor_id
                       AND required.param = fv.param
                    WHERE fv.market_id = $1
                      AND fv.ts <= $2
                    ORDER BY fv.factor_id ASC, fv.param ASC, fv.ts DESC
                )
                SELECT
                    required.factor_id,
                    required.param,
                    latest.value
                FROM required
                LEFT JOIN latest
                    ON latest.factor_id = required.factor_id
                   AND latest.param = required.param
                ORDER BY required.factor_id ASC, required.param ASC
                """,
                market_id,
                as_of,
                factor_ids,
                params,
            )
        values: dict[FactorKey, float] = {}
        missing_factors: list[FactorKey] = []
        for row in rows:
            key = (row["factor_id"], row["param"])
            value = row["value"]
            if value is None:
                missing_factors.append(key)
                continue
            values[key] = float(value)
        return FactorSnapshot(
            values=values,
            missing_factors=tuple(missing_factors),
            snapshot_hash=_snapshot_hash(
                market_id=market_id,
                as_of=as_of,
                strategy_id=strategy_id,
                strategy_version_id=strategy_version_id,
                required_keys=required_keys,
                values=values,
                missing_factors=tuple(missing_factors),
            ),
        )


def _snapshot_hash(
    *,
    market_id: str,
    as_of: datetime,
    strategy_id: str,
    strategy_version_id: str,
    required_keys: Sequence[FactorKey],
    values: Mapping[FactorKey, float],
    missing_factors: Sequence[FactorKey],
) -> str:
    payload = {
        "market_id": market_id,
        "as_of": as_of.isoformat(),
        "strategy_id": strategy_id,
        "strategy_version_id": strategy_version_id,
        "required_keys": [
            [factor_id, param]
            for factor_id, param in sorted(required_keys)
        ],
        "values": [
            [factor_id, param, values[(factor_id, param)]]
            for factor_id, param in sorted(values)
        ],
        "missing_factors": [
            [factor_id, param]
            for factor_id, param in sorted(missing_factors)
        ],
    }
    return sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode(
            "utf-8"
        )
    ).hexdigest()
