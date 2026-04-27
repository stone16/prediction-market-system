from __future__ import annotations

import asyncio
import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pms.core.models import OrderState, TradeDecision


@dataclass(frozen=True, slots=True)
class LiveEmergencyAuditWriter:
    path: Path

    async def append(
        self,
        *,
        phase: str,
        decision: TradeDecision,
        order_state: OrderState | None,
        error: BaseException,
    ) -> None:
        record = _audit_record(
            phase=phase,
            decision=decision,
            order_state=order_state,
            error=error,
        )
        await asyncio.to_thread(_append_jsonl, self.path, record)


def _audit_record(
    *,
    phase: str,
    decision: TradeDecision,
    order_state: OrderState | None,
    error: BaseException,
) -> dict[str, Any]:
    return {
        "timestamp": datetime.now(tz=UTC).isoformat(),
        "phase": phase,
        "decision_id": decision.decision_id,
        "intent_key": decision.intent_key,
        "market_id": decision.market_id,
        "token_id": decision.token_id,
        "venue": decision.venue,
        "strategy_id": decision.strategy_id,
        "strategy_version_id": decision.strategy_version_id,
        "order_id": None if order_state is None else order_state.order_id,
        "raw_status": None if order_state is None else order_state.raw_status,
        "status": None if order_state is None else order_state.status,
        "pre_submit_quote": (
            {} if order_state is None else dict(order_state.pre_submit_quote)
        ),
        "requested_notional_usdc": (
            decision.notional_usdc
            if order_state is None
            else order_state.requested_notional_usdc
        ),
        "filled_notional_usdc": (
            None if order_state is None else order_state.filled_notional_usdc
        ),
        "remaining_notional_usdc": (
            None if order_state is None else order_state.remaining_notional_usdc
        ),
        "error_type": type(error).__name__,
        "error": str(error),
    }


def _append_jsonl(path: Path, record: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as file:
        file.write(json.dumps(record, sort_keys=True, separators=(",", ":")))
        file.write("\n")
