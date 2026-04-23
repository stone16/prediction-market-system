from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import json
from typing import Final, Literal, cast

import asyncpg

from pms.core.models import TradeDecision


DecisionStatus = Literal["pending", "accepted", "rejected", "expired"]

DECISION_STATUSES: Final[tuple[DecisionStatus, ...]] = (
    "pending",
    "accepted",
    "rejected",
    "expired",
)
_VALID_STATUS_TRANSITIONS: Final[dict[DecisionStatus, frozenset[DecisionStatus]]] = {
    "pending": frozenset({"pending", "accepted", "rejected", "expired"}),
    "accepted": frozenset({"accepted"}),
    "rejected": frozenset({"rejected"}),
    "expired": frozenset({"expired"}),
}
_CREATE_DECISION_PAYLOADS_TABLE = """
CREATE TABLE IF NOT EXISTS decision_payloads (
    decision_id TEXT PRIMARY KEY REFERENCES decisions(decision_id) ON DELETE CASCADE,
    payload JSONB NOT NULL
)
"""


@dataclass
class DecisionStore:
    pool: asyncpg.Pool | None = None

    def bind_pool(self, pool: asyncpg.Pool) -> None:
        self.pool = pool

    async def insert(
        self,
        decision: TradeDecision,
        *,
        factor_snapshot_hash: str | None,
        created_at: datetime,
        expires_at: datetime,
        status: DecisionStatus = "pending",
    ) -> None:
        if self.pool is None or not hasattr(self.pool, "acquire"):
            return

        validate_decision_status_transition("pending", status)
        async with self.pool.acquire() as connection:
            await _ensure_decision_payloads_table(connection)
            async with connection.transaction():
                await connection.execute(
                    """
                    INSERT INTO decisions (
                        decision_id,
                        opportunity_id,
                        strategy_id,
                        strategy_version_id,
                        status,
                        factor_snapshot_hash,
                        created_at,
                        updated_at,
                        expires_at
                    ) VALUES (
                        $1, $2, $3, $4, $5, $6, $7, $8, $9
                    )
                    ON CONFLICT (decision_id) DO UPDATE
                    SET opportunity_id = EXCLUDED.opportunity_id,
                        strategy_id = EXCLUDED.strategy_id,
                        strategy_version_id = EXCLUDED.strategy_version_id,
                        status = EXCLUDED.status,
                        factor_snapshot_hash = EXCLUDED.factor_snapshot_hash,
                        updated_at = EXCLUDED.updated_at,
                        expires_at = EXCLUDED.expires_at
                    """,
                    decision.decision_id,
                    decision.opportunity_id,
                    decision.strategy_id,
                    decision.strategy_version_id,
                    status,
                    factor_snapshot_hash,
                    created_at,
                    created_at,
                    expires_at,
                )
                # Keep the full decision object durable without widening the
                # shell table ahead of the accept-flow checkpoint.
                await connection.execute(
                    """
                    INSERT INTO decision_payloads (decision_id, payload)
                    VALUES ($1, $2::jsonb)
                    ON CONFLICT (decision_id) DO UPDATE
                    SET payload = EXCLUDED.payload
                    """,
                    decision.decision_id,
                    json.dumps(_decision_payload(decision)),
                )

    async def expire_pending(self, *, before: datetime) -> int:
        if self.pool is None or not hasattr(self.pool, "acquire"):
            return 0

        async with self.pool.acquire() as connection:
            rows = await connection.fetch(
                """
                UPDATE decisions
                SET status = 'expired',
                    updated_at = $1
                WHERE status = 'pending'
                  AND expires_at < $1
                RETURNING decision_id
                """,
                before,
            )
        return len(rows)


def validate_decision_status_transition(
    current_status: str,
    next_status: str,
) -> None:
    normalized_current = _coerce_decision_status(current_status)
    normalized_next = _coerce_decision_status(next_status)
    if normalized_next not in _VALID_STATUS_TRANSITIONS[normalized_current]:
        msg = (
            "invalid decision status transition: "
            f"{normalized_current} -> {normalized_next}"
        )
        raise ValueError(msg)


async def _ensure_decision_payloads_table(connection: asyncpg.Connection) -> None:
    await connection.execute(_CREATE_DECISION_PAYLOADS_TABLE)


def _coerce_decision_status(status: str) -> DecisionStatus:
    if status not in DECISION_STATUSES:
        msg = f"unknown decision status: {status}"
        raise ValueError(msg)
    return status


def _decision_payload(decision: TradeDecision) -> dict[str, object]:
    return {
        "market_id": decision.market_id,
        "token_id": decision.token_id,
        "venue": decision.venue,
        "side": decision.side,
        "notional_usdc": decision.notional_usdc,
        "order_type": decision.order_type,
        "max_slippage_bps": decision.max_slippage_bps,
        "stop_conditions": list(decision.stop_conditions),
        "prob_estimate": decision.prob_estimate,
        "expected_edge": decision.expected_edge,
        "time_in_force": decision.time_in_force.value,
        "opportunity_id": decision.opportunity_id,
        "strategy_id": decision.strategy_id,
        "strategy_version_id": decision.strategy_version_id,
        "limit_price": decision.limit_price,
        "action": decision.action,
        "outcome": decision.outcome,
        "model_id": decision.model_id,
    }
