from __future__ import annotations

import json
import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, replace
from typing import Any, Protocol, cast

import httpx

from pms.core.models import FillRecord, TradeDecision
from pms.evaluation.adapters.scoring import Scorer


logger = logging.getLogger(__name__)

# A settled binary market pays exactly 1.0 / 0.0. Closed-but-unsettled
# markets still show near-resolution trade prices (e.g. 0.97/0.03); admitting
# them would corrupt Brier ground truth, so only unambiguous payouts pass.
# Tolerance matches the settlement heuristic in scripts/flb_data_feasibility.py.
_SETTLED_TOLERANCE = 0.001
_CONDITION_ID_BATCH_SIZE = 20


class ResolutionSource(Protocol):
    async def fetch_resolutions(
        self,
        condition_ids: Sequence[str],
    ) -> Mapping[str, float]: ...


class ResolutionFillStore(Protocol):
    async def read_unresolved_fills(self) -> list[FillRecord]: ...

    async def resolve_fill(
        self,
        fill_id: str,
        *,
        resolved_outcome: float,
    ) -> bool: ...


class StoredDecisionLike(Protocol):
    @property
    def decision(self) -> TradeDecision: ...

    @property
    def decision_evidence(self) -> Mapping[str, Any]: ...


class ResolutionDecisionReader(Protocol):
    async def get_decision(self, decision_id: str) -> StoredDecisionLike | None: ...


class ResolutionEvalSpool(Protocol):
    """The shared evaluator spool: swept resolutions are enqueued through it
    so scoring, eval-record appends, and post-append hooks (feedback
    generation and any future sink) follow the exact live fill path."""

    def enqueue(
        self,
        fill: FillRecord,
        decision: TradeDecision,
        *,
        decision_evidence: Mapping[str, object] | None = None,
    ) -> None: ...


@dataclass(frozen=True)
class ResolutionSweepResult:
    unresolved_fills: int
    fills_resolved: int
    eval_records_enqueued: int


@dataclass(frozen=True)
class _FillSweepOutcome:
    eval_record_enqueued: bool
    fill_resolved: bool


@dataclass
class GammaResolutionSource:
    """Reads market resolutions from the Polymarket Gamma API."""

    http_client: httpx.AsyncClient
    batch_size: int = _CONDITION_ID_BATCH_SIZE

    async def fetch_resolutions(
        self,
        condition_ids: Sequence[str],
    ) -> Mapping[str, float]:
        resolutions: dict[str, float] = {}
        unique_ids = list(dict.fromkeys(condition_ids))
        for start in range(0, len(unique_ids), self.batch_size):
            batch = unique_ids[start : start + self.batch_size]
            # Live Gamma contract (verified 2026-06-10): batching closed
            # markets' condition_ids WITHOUT closed=true returns 0 rows, so
            # closed=true is load-bearing — without it no fill ever resolves.
            # The explicit limit pins the page size to the batch instead of
            # relying on the server default (measured: exactly 20).
            # NOTE: /markets responds with `deprecation: true`,
            # `sunset: 2026-05-01` (past) and `warning: 299 use
            # /markets/keyset`; plan the keyset migration before the
            # endpoint disappears.
            params: list[tuple[str, str | int | float | bool | None]] = [
                ("condition_ids", condition_id) for condition_id in batch
            ]
            params.append(("closed", "true"))
            params.append(("limit", str(len(batch))))
            response = await self.http_client.get("/markets", params=params)
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, list):
                msg = "Expected Gamma API /markets response to be a list"
                raise ValueError(msg)
            for row in payload:
                if not isinstance(row, dict):
                    continue
                resolution = _resolution_from_gamma_row(cast(dict[str, Any], row))
                if resolution is not None:
                    condition_id, resolved_outcome = resolution
                    resolutions[condition_id] = resolved_outcome
        return resolutions


@dataclass
class ResolutionSweeper:
    """Backfills resolved outcomes for fills whose markets resolved later.

    Live PAPER/LIVE fills are persisted with NULL ``resolved_outcome`` (only
    backtest replay carries it at fill time), so without this sweep no final
    Brier evidence ever accrues. Each sweep finds unresolved fills, asks the
    resolution source which of their markets settled, enqueues the resolved
    fills through the shared evaluator spool — the exact live scoring path,
    including post-append hooks — and then marks the fills resolved.
    """

    fill_store: ResolutionFillStore
    decision_reader: ResolutionDecisionReader
    eval_spool: ResolutionEvalSpool
    resolution_source: ResolutionSource
    scorer: Scorer = field(default_factory=Scorer)

    async def sweep_once(self) -> ResolutionSweepResult:
        unresolved = await self.fill_store.read_unresolved_fills()
        if not unresolved:
            return ResolutionSweepResult(
                unresolved_fills=0,
                fills_resolved=0,
                eval_records_enqueued=0,
            )
        condition_ids = list(dict.fromkeys(fill.market_id for fill in unresolved))
        resolutions = await self.resolution_source.fetch_resolutions(condition_ids)
        fills_resolved = 0
        eval_records_enqueued = 0
        for fill in unresolved:
            resolved_outcome = resolutions.get(fill.market_id)
            if resolved_outcome is None:
                continue
            try:
                outcome = await self._enqueue_and_resolve(fill, resolved_outcome)
            except Exception:  # noqa: BLE001
                logger.exception(
                    "resolution scoring failed for fill %s; leaving it "
                    "unresolved for retry",
                    fill.fill_id or fill.trade_id,
                )
                continue
            if outcome.fill_resolved:
                fills_resolved += 1
            if outcome.eval_record_enqueued:
                eval_records_enqueued += 1
        return ResolutionSweepResult(
            unresolved_fills=len(unresolved),
            fills_resolved=fills_resolved,
            eval_records_enqueued=eval_records_enqueued,
        )

    async def _enqueue_and_resolve(
        self,
        fill: FillRecord,
        resolved_outcome: float,
    ) -> _FillSweepOutcome:
        """Enqueue eval evidence through the evaluator spool, then commit
        the fill's NULL -> value transition.

        The ordering is load-bearing: evidence is enqueued BEFORE
        ``resolve_fill`` commits, so a crash in between leaves the fill
        unresolved and the next sweep retries it instead of permanently
        losing the eval record. Duplicate appends from retries (and LIVE
        partial fills sharing a decision_id) are absorbed by the eval
        store's ``ON CONFLICT (decision_id) DO NOTHING`` insert.
        """
        fill_id = fill.fill_id or fill.trade_id
        stored_decision = await self.decision_reader.get_decision(fill.decision_id)
        if stored_decision is None:
            updated = await self.fill_store.resolve_fill(
                fill_id,
                resolved_outcome=resolved_outcome,
            )
            if updated:
                logger.warning(
                    "fill %s resolved to %s but decision %s is not recoverable; "
                    "skipping eval record",
                    fill_id,
                    resolved_outcome,
                    fill.decision_id,
                )
            return _FillSweepOutcome(
                eval_record_enqueued=False,
                fill_resolved=updated,
            )
        resolved_fill = replace(fill, resolved_outcome=resolved_outcome)
        # Scorer.score is pure; validating here keeps deterministically
        # unscorable fills out of the shared evaluator spool task and leaves
        # them unresolved so they keep surfacing in unresolved counts.
        self.scorer.score(resolved_fill, stored_decision.decision)
        self.eval_spool.enqueue(
            resolved_fill,
            stored_decision.decision,
            decision_evidence=stored_decision.decision_evidence,
        )
        updated = await self.fill_store.resolve_fill(
            fill_id,
            resolved_outcome=resolved_outcome,
        )
        return _FillSweepOutcome(eval_record_enqueued=True, fill_resolved=updated)


def _resolution_from_gamma_row(row: dict[str, Any]) -> tuple[str, float] | None:
    condition_id = str(row.get("conditionId") or row.get("condition_id") or "")
    if condition_id == "":
        return None
    if row.get("closed") is not True:
        return None
    outcomes = _json_string_list(row.get("outcomes"))
    prices = _json_string_list(row.get("outcomePrices"))
    if (
        outcomes is None
        or prices is None
        or len(outcomes) != 2
        or len(prices) != 2
    ):
        return None
    normalized_outcomes = [outcome.strip().lower() for outcome in outcomes]
    if sorted(normalized_outcomes) != ["no", "yes"]:
        return None
    yes_index = normalized_outcomes.index("yes")
    try:
        yes_price = float(prices[yes_index])
        no_price = float(prices[1 - yes_index])
    except (TypeError, ValueError):
        return None
    if (
        abs(yes_price - 1.0) <= _SETTLED_TOLERANCE
        and abs(no_price) <= _SETTLED_TOLERANCE
    ):
        return condition_id, 1.0
    if (
        abs(no_price - 1.0) <= _SETTLED_TOLERANCE
        and abs(yes_price) <= _SETTLED_TOLERANCE
    ):
        return condition_id, 0.0
    return None


def _json_string_list(value: object) -> list[str] | None:
    payload: object = value
    if isinstance(value, str):
        try:
            payload = json.loads(value)
        except json.JSONDecodeError:
            return None
    if not isinstance(payload, list):
        return None
    return [str(item) for item in cast(list[object], payload)]
