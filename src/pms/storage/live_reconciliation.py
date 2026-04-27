from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import asyncpg


SubmissionUnknownResolutionStatus = Literal["filled", "not_found", "open"]


@dataclass(frozen=True, slots=True)
class SubmissionUnknownReconciliationStore:
    pool: asyncpg.Pool

    async def reconcile_submission_unknown(
        self,
        *,
        decision_id: str,
        venue_order_id: str | None,
        status: SubmissionUnknownResolutionStatus,
        reconciled_by: str,
        note: str | None = None,
    ) -> bool:
        async with self.pool.acquire() as connection:
            row = await connection.fetchrow(
                """
                UPDATE order_intents
                SET reconciled_at = now(),
                    venue_order_id = $2,
                    reconciliation_status = $3,
                    reconciled_by = $4,
                    reconciliation_note = $5
                WHERE decision_id = $1
                  AND outcome = 'submission_unknown'
                RETURNING decision_id
                """,
                decision_id,
                venue_order_id,
                status,
                reconciled_by,
                note,
            )
        return row is not None
