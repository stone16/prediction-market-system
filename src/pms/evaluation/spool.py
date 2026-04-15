from __future__ import annotations

import asyncio
import logging
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import UTC, datetime

from pms.core.models import EvalRecord, FillRecord, MarketSignal, TradeDecision
from pms.evaluation.adapters.scoring import Scorer
from pms.storage.eval_store import EvalStore


logger = logging.getLogger(__name__)

# Queue item: (fill_or_none, decision, signal_or_none)
# fill is None when the decision was rejected by risk / liquidity.
# signal is None only in legacy unit-test call-sites where fill already
# carries a resolved_outcome (the spool still works correctly in that case).
_QueueItem = tuple[FillRecord | None, TradeDecision, MarketSignal | None]


@dataclass
class EvalSpool:
    store: EvalStore
    scorer: Scorer
    _queue: asyncio.Queue[_QueueItem] = field(
        default_factory=asyncio.Queue,
    )
    _task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._run())

    def enqueue(
        self,
        fill: FillRecord | None,
        decision: TradeDecision,
        signal: MarketSignal | None = None,
    ) -> None:
        self._queue.put_nowait((fill, decision, signal))

    async def join(self) -> None:
        await self._queue.join()

    async def stop(self) -> None:
        if self._task is None:
            return
        self._task.cancel()
        with suppress(asyncio.CancelledError):
            await self._task
        self._task = None

    async def _run(self) -> None:
        while True:
            fill, decision, signal = await self._queue.get()
            try:
                resolved_outcome = _resolved_outcome(fill, signal)
                if resolved_outcome is None:
                    logger.info(
                        "skipping unresolved decision in evaluator spool: %s",
                        decision.decision_id,
                    )
                    continue
                if fill is not None:
                    self.store.append(self.scorer.score(fill, decision))
                else:
                    self.store.append(_unfilled_record(decision, resolved_outcome))
            finally:
                self._queue.task_done()


def _resolved_outcome(
    fill: FillRecord | None, signal: MarketSignal | None
) -> float | None:
    """Return resolved_outcome from fill (preferred) or signal."""
    if fill is not None and fill.resolved_outcome is not None:
        return fill.resolved_outcome
    if signal is not None:
        raw = signal.external_signal.get("resolved_outcome")
        if raw is not None:
            return min(max(float(raw), 0.0), 1.0)
    return None


def _unfilled_record(
    decision: TradeDecision, resolved_outcome: float
) -> EvalRecord:
    """Build an EvalRecord for a decision that was rejected / never filled."""
    brier_score = (decision.prob_estimate - resolved_outcome) ** 2
    return EvalRecord(
        market_id=decision.market_id,
        decision_id=decision.decision_id,
        prob_estimate=decision.prob_estimate,
        resolved_outcome=resolved_outcome,
        brier_score=brier_score,
        fill_status="unfilled",
        recorded_at=datetime.now(tz=UTC),
        citations=[],
        category=None,
        model_id=None,
        pnl=0.0,
        slippage_bps=0.0,
        filled=False,
    )
