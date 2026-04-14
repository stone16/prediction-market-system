from __future__ import annotations

import asyncio
import logging
from contextlib import suppress
from dataclasses import dataclass, field

from pms.core.models import FillRecord, TradeDecision
from pms.evaluation.adapters.scoring import Scorer
from pms.storage.eval_store import EvalStore


logger = logging.getLogger(__name__)


@dataclass
class EvalSpool:
    store: EvalStore
    scorer: Scorer
    _queue: asyncio.Queue[tuple[FillRecord, TradeDecision]] = field(
        default_factory=asyncio.Queue,
    )
    _task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._run())

    def enqueue(self, fill: FillRecord, decision: TradeDecision) -> None:
        self._queue.put_nowait((fill, decision))

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
            fill, decision = await self._queue.get()
            try:
                if fill.resolved_outcome is None:
                    logger.info(
                        "skipping unresolved fill in evaluator spool: %s",
                        fill.trade_id,
                    )
                    continue
                self.store.append(self.scorer.score(fill, decision))
            finally:
                self._queue.task_done()
