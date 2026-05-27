from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Mapping
from contextlib import suppress
from dataclasses import dataclass, field
from math import isfinite
from typing import Protocol, cast

from pms.core.models import BookSummary
from pms.core.models import FillRecord, TradeDecision
from pms.evaluation.adapters.scoring import Scorer
from pms.evaluation.feedback import EvaluatorFeedback
from pms.evaluation.metrics import StrategyMetricsSnapshot, StrategyVersionKey
from pms.evaluation.quote_scoring import QuoteScorer
from pms.storage.eval_store import EvalStore
from pms.storage.quote_eval_store import QuoteEvalStore
from pms.strategies.projections import EvalSpec


logger = logging.getLogger(__name__)

StrategyMetricsProvider = Callable[
    [],
    Awaitable[
        Mapping[
            StrategyVersionKey,
            tuple[StrategyMetricsSnapshot, EvalSpec],
        ]
    ],
]


class QuoteReader(Protocol):
    async def latest_book_summary(
        self,
        market_id: str,
        token_id: str | None,
    ) -> BookSummary | None: ...


@dataclass
class EvalSpool:
    store: EvalStore
    scorer: Scorer
    feedback_generator: EvaluatorFeedback | None = None
    metrics_provider: StrategyMetricsProvider | None = None
    quote_store: QuoteEvalStore | None = None
    quote_reader: QuoteReader | None = None
    quote_scorer: QuoteScorer = field(default_factory=QuoteScorer)
    quote_lag_seconds: int = 0
    _queue: asyncio.Queue[
        tuple[FillRecord, TradeDecision, Mapping[str, object] | None]
    ] = field(
        default_factory=asyncio.Queue,
    )
    _task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._run())

    def enqueue(
        self,
        fill: FillRecord,
        decision: TradeDecision,
        *,
        decision_evidence: Mapping[str, object] | None = None,
    ) -> None:
        self._queue.put_nowait((fill, decision, decision_evidence))

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
            fill, decision, decision_evidence = await self._queue.get()
            try:
                if fill.resolved_outcome is None:
                    await self._record_quote_eval(fill, decision)
                    continue
                await self.store.append(
                    self.scorer.score(
                        fill,
                        decision,
                        baseline_prob_estimates=_baseline_prob_estimates_from_evidence(
                            decision_evidence,
                        ),
                    )
                )
                try:
                    await self._generate_feedback()
                except Exception:  # noqa: BLE001
                    logger.exception("feedback generation failed in evaluator spool")
            finally:
                self._queue.task_done()

    async def _generate_feedback(self) -> None:
        if self.feedback_generator is None or self.metrics_provider is None:
            return
        metrics_by_strategy = await self.metrics_provider()
        if not metrics_by_strategy:
            return
        await self.feedback_generator.generate(metrics_by_strategy)

    async def _record_quote_eval(
        self,
        fill: FillRecord,
        decision: TradeDecision,
    ) -> None:
        if self.quote_store is None or self.quote_reader is None:
            logger.info(
                "skipping unresolved fill in evaluator spool: %s",
                fill.trade_id,
            )
            return
        try:
            quote = await self.quote_reader.latest_book_summary(
                fill.market_id,
                fill.token_id,
            )
            if quote is None:
                logger.info(
                    "skipping unresolved fill without quote in evaluator spool: %s",
                    fill.trade_id,
                )
                return
            await self.quote_store.append(
                self.quote_scorer.score(
                    fill,
                    decision,
                    quote,
                    quote_lag_seconds=self.quote_lag_seconds,
                )
            )
        except Exception:  # noqa: BLE001
            logger.exception("quote evaluation failed in evaluator spool")


def _baseline_prob_estimates_from_evidence(
    decision_evidence: Mapping[str, object] | None,
) -> dict[str, float]:
    if decision_evidence is None:
        return {}

    baselines: dict[str, float] = {}
    for evidence_key, baseline_source in (
        ("market_implied_baseline_prob_estimate", "market_implied"),
        ("mid_quote_baseline_prob_estimate", "mid_quote"),
        ("last_trade_baseline_prob_estimate", "last_trade"),
        ("category_prior_baseline_prob_estimate", "category_prior"),
    ):
        probability = _probability_or_none(decision_evidence.get(evidence_key))
        if probability is not None:
            baselines[baseline_source] = probability
    return baselines


def _probability_or_none(value: object) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        probability = float(cast(int | float | str, value))
    except (TypeError, ValueError):
        return None
    if not isfinite(probability) or probability < 0.0 or probability > 1.0:
        return None
    return probability
