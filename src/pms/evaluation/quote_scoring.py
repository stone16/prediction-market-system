from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from pms.core.enums import Side
from pms.core.models import BookSummary, FillRecord, QuoteEvalRecord, TradeDecision


@dataclass(frozen=True)
class QuoteScorer:
    quote_source: str = "postgres_snapshot"

    def score(
        self,
        fill: FillRecord,
        decision: TradeDecision,
        quote: BookSummary,
        *,
        quote_lag_seconds: int,
        recorded_at: datetime | None = None,
    ) -> QuoteEvalRecord:
        if (
            fill.strategy_id != decision.strategy_id
            or fill.strategy_version_id != decision.strategy_version_id
        ):
            msg = "FillRecord and TradeDecision strategy identity must match for quote scoring"
            raise ValueError(msg)
        if quote.best_bid <= 0.0 or quote.best_ask <= 0.0:
            msg = "quote bid/ask must be positive"
            raise ValueError(msg)

        quote_price = (quote.best_bid + quote.best_ask) / 2.0
        return QuoteEvalRecord(
            fill_id=fill.fill_id or fill.trade_id,
            decision_id=decision.decision_id,
            market_id=fill.market_id,
            token_id=fill.token_id,
            strategy_id=fill.strategy_id,
            strategy_version_id=fill.strategy_version_id,
            prob_estimate=decision.prob_estimate,
            quote_price=quote_price,
            quote_source=self.quote_source,
            quote_lag_seconds=quote_lag_seconds,
            quote_score=(decision.prob_estimate - quote_price) ** 2,
            mtm_pnl=_mtm_pnl(fill, decision, quote),
            book_ts=quote.timestamp,
            recorded_at=recorded_at or datetime.now(tz=UTC),
            citations=[fill.trade_id],
            category="unknown" if decision.model_id is None else decision.model_id,
            model_id=decision.model_id,
        )


def _mtm_pnl(fill: FillRecord, decision: TradeDecision, quote: BookSummary) -> float:
    action = decision.action if decision.action is not None else decision.side
    if action == Side.SELL.value:
        return (fill.fill_price - quote.best_ask) * fill.fill_quantity
    return (quote.best_bid - fill.fill_price) * fill.fill_quantity
