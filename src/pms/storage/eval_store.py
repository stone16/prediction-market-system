from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import json
from typing import cast

import asyncpg

from pms.core.models import EvalRecord
from pms.storage.strategy_tags import resolve_strategy_tags


@dataclass
class EvalStore:
    pool: asyncpg.Pool | None = None

    def bind_pool(self, pool: asyncpg.Pool) -> None:
        self.pool = pool

    async def append(
        self,
        record: EvalRecord,
        *,
        strategy_id: str = "default",
        strategy_version_id: str | None = None,
    ) -> None:
        async with self._pool().acquire() as connection:
            await insert_eval_record_row(
                connection,
                record,
                strategy_id=strategy_id,
                strategy_version_id=strategy_version_id,
            )

    async def all(self) -> list[EvalRecord]:
        if self.pool is None:
            return []

        async with self.pool.acquire() as connection:
            rows = await connection.fetch(
                """
                SELECT
                    market_id,
                    decision_id,
                    prob_estimate,
                    resolved_outcome,
                    brier_score,
                    fill_status,
                    recorded_at,
                    citations,
                    category,
                    model_id,
                    pnl,
                    slippage_bps,
                    filled
                FROM eval_records
                ORDER BY recorded_at ASC, decision_id ASC
                """
            )
        return [_eval_record_from_row(row) for row in rows]

    def _pool(self) -> asyncpg.Pool:
        if self.pool is None:
            msg = "EvalStore pool is not bound"
            raise RuntimeError(msg)
        return self.pool


async def insert_eval_record_row(
    connection: asyncpg.Connection,
    record: EvalRecord,
    *,
    strategy_id: str = "default",
    strategy_version_id: str | None = None,
) -> None:
    resolved_strategy_id, resolved_strategy_version_id = await resolve_strategy_tags(
        connection,
        strategy_id=strategy_id,
        strategy_version_id=strategy_version_id,
    )
    await connection.execute(
        """
        INSERT INTO eval_records (
            decision_id,
            market_id,
            prob_estimate,
            resolved_outcome,
            brier_score,
            fill_status,
            recorded_at,
            citations,
            category,
            model_id,
            pnl,
            slippage_bps,
            filled,
            strategy_id,
            strategy_version_id
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9, $10, $11, $12, $13, $14, $15
        )
        """,
        record.decision_id,
        record.market_id,
        record.prob_estimate,
        record.resolved_outcome,
        record.brier_score,
        record.fill_status,
        record.recorded_at,
        json.dumps(record.citations),
        record.category,
        record.model_id,
        record.pnl,
        record.slippage_bps,
        record.filled,
        resolved_strategy_id,
        resolved_strategy_version_id,
    )


def _eval_record_from_row(row: asyncpg.Record) -> EvalRecord:
    citations_value = row["citations"]
    citations: list[str]
    if isinstance(citations_value, list):
        citations = [str(item) for item in citations_value]
    elif isinstance(citations_value, str):
        loaded = json.loads(citations_value)
        citations = [str(item) for item in loaded] if isinstance(loaded, list) else []
    else:
        citations = []

    return EvalRecord(
        market_id=cast(str, row["market_id"]),
        decision_id=cast(str, row["decision_id"]),
        prob_estimate=cast(float, row["prob_estimate"]),
        resolved_outcome=cast(float, row["resolved_outcome"]),
        brier_score=cast(float, row["brier_score"]),
        fill_status=cast(str, row["fill_status"]),
        recorded_at=cast(datetime, row["recorded_at"]),
        citations=citations,
        category=cast(str | None, row["category"]),
        model_id=cast(str | None, row["model_id"]),
        pnl=cast(float, row["pnl"]),
        slippage_bps=cast(float, row["slippage_bps"]),
        filled=cast(bool, row["filled"]),
    )
