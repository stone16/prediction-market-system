from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
import json
from typing import Any, SupportsFloat, cast

import asyncpg

from pms.core.models import QuoteEvalRecord


_CREATE_QUOTE_EVAL_RECORDS_TABLE = """
CREATE TABLE IF NOT EXISTS quote_eval_records (
    fill_id TEXT NOT NULL,
    decision_id TEXT NOT NULL,
    market_id TEXT NOT NULL,
    token_id TEXT,
    strategy_id TEXT NOT NULL,
    strategy_version_id TEXT NOT NULL,
    prob_estimate DOUBLE PRECISION NOT NULL,
    quote_price DOUBLE PRECISION NOT NULL,
    quote_source TEXT NOT NULL,
    quote_lag_seconds INTEGER NOT NULL,
    quote_score DOUBLE PRECISION NOT NULL,
    mtm_pnl DOUBLE PRECISION NOT NULL,
    book_ts TIMESTAMPTZ NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL,
    citations JSONB NOT NULL DEFAULT '[]'::jsonb,
    category TEXT,
    model_id TEXT,
    PRIMARY KEY (fill_id, quote_lag_seconds),
    CONSTRAINT quote_eval_records_strategy_identity_check
        CHECK (strategy_id != '' AND strategy_version_id != '')
)
"""

_SELECT_COLUMNS = """
SELECT
    fill_id,
    decision_id,
    market_id,
    token_id,
    strategy_id,
    strategy_version_id,
    prob_estimate,
    quote_price,
    quote_source,
    quote_lag_seconds,
    quote_score,
    mtm_pnl,
    book_ts,
    recorded_at,
    citations,
    category,
    model_id
FROM quote_eval_records
"""


@dataclass
class QuoteEvalStore:
    pool: asyncpg.Pool | None = None

    def bind_pool(self, pool: asyncpg.Pool) -> None:
        self.pool = pool

    async def append(self, record: QuoteEvalRecord) -> None:
        async with self._pool().acquire() as connection:
            await _ensure_quote_eval_records_table(connection)
            await connection.execute(
                """
                INSERT INTO quote_eval_records (
                    fill_id,
                    decision_id,
                    market_id,
                    token_id,
                    strategy_id,
                    strategy_version_id,
                    prob_estimate,
                    quote_price,
                    quote_source,
                    quote_lag_seconds,
                    quote_score,
                    mtm_pnl,
                    book_ts,
                    recorded_at,
                    citations,
                    category,
                    model_id
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14,
                    $15::jsonb, $16, $17
                )
                ON CONFLICT (fill_id, quote_lag_seconds) DO UPDATE
                SET decision_id = EXCLUDED.decision_id,
                    market_id = EXCLUDED.market_id,
                    token_id = EXCLUDED.token_id,
                    strategy_id = EXCLUDED.strategy_id,
                    strategy_version_id = EXCLUDED.strategy_version_id,
                    prob_estimate = EXCLUDED.prob_estimate,
                    quote_price = EXCLUDED.quote_price,
                    quote_source = EXCLUDED.quote_source,
                    quote_score = EXCLUDED.quote_score,
                    mtm_pnl = EXCLUDED.mtm_pnl,
                    book_ts = EXCLUDED.book_ts,
                    recorded_at = EXCLUDED.recorded_at,
                    citations = EXCLUDED.citations,
                    category = EXCLUDED.category,
                    model_id = EXCLUDED.model_id
                """,
                record.fill_id,
                record.decision_id,
                record.market_id,
                record.token_id,
                record.strategy_id,
                record.strategy_version_id,
                record.prob_estimate,
                record.quote_price,
                record.quote_source,
                record.quote_lag_seconds,
                record.quote_score,
                record.mtm_pnl,
                record.book_ts,
                record.recorded_at,
                json.dumps(record.citations),
                record.category,
                record.model_id,
            )

    async def all(self) -> list[QuoteEvalRecord]:
        if self.pool is None:
            return []

        async with self.pool.acquire() as connection:
            rows = await connection.fetch(
                _SELECT_COLUMNS + "\nORDER BY recorded_at ASC, fill_id ASC"
            )
        return [_quote_eval_record_from_row(row) for row in rows]

    async def all_for_strategy(
        self,
        strategy_id: str,
        strategy_version_id: str,
    ) -> list[QuoteEvalRecord]:
        if self.pool is None:
            return []

        async with self.pool.acquire() as connection:
            rows = await connection.fetch(
                _SELECT_COLUMNS
                + """
                WHERE strategy_id = $1 AND strategy_version_id = $2
                ORDER BY recorded_at ASC, fill_id ASC
                """,
                strategy_id,
                strategy_version_id,
            )
        return [_quote_eval_record_from_row(row) for row in rows]

    def _pool(self) -> asyncpg.Pool:
        if self.pool is None:
            msg = "QuoteEvalStore pool is not bound"
            raise RuntimeError(msg)
        return self.pool


async def _ensure_quote_eval_records_table(connection: asyncpg.Connection) -> None:
    await connection.execute(_CREATE_QUOTE_EVAL_RECORDS_TABLE)
    await connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_quote_eval_records_strategy_identity_recorded_at
            ON quote_eval_records(strategy_id, strategy_version_id, recorded_at DESC)
        """
    )


def _quote_eval_record_from_row(row: Mapping[str, Any]) -> QuoteEvalRecord:
    citations_value = row["citations"]
    citations: list[str]
    if isinstance(citations_value, list):
        citations = [str(item) for item in citations_value]
    elif isinstance(citations_value, str):
        loaded = json.loads(citations_value)
        citations = [str(item) for item in loaded] if isinstance(loaded, list) else []
    else:
        citations = []

    return QuoteEvalRecord(
        fill_id=cast(str, row["fill_id"]),
        decision_id=cast(str, row["decision_id"]),
        market_id=cast(str, row["market_id"]),
        token_id=cast(str | None, row["token_id"]),
        strategy_id=cast(str, row["strategy_id"]),
        strategy_version_id=cast(str, row["strategy_version_id"]),
        prob_estimate=float(cast(SupportsFloat, row["prob_estimate"])),
        quote_price=float(cast(SupportsFloat, row["quote_price"])),
        quote_source=cast(str, row["quote_source"]),
        quote_lag_seconds=int(row["quote_lag_seconds"]),
        quote_score=float(cast(SupportsFloat, row["quote_score"])),
        mtm_pnl=float(cast(SupportsFloat, row["mtm_pnl"])),
        book_ts=cast(datetime, row["book_ts"]),
        recorded_at=cast(datetime, row["recorded_at"]),
        citations=citations,
        category=cast(str | None, row["category"]),
        model_id=cast(str | None, row["model_id"]),
    )
