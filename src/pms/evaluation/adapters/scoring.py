from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from pms.core.enums import OrderStatus, Side
from pms.core.models import EvalRecord, FillRecord, TradeDecision


@dataclass(frozen=True)
class Scorer:
    def score(self, fill: FillRecord, decision: TradeDecision) -> EvalRecord:
        if fill.resolved_outcome is None:
            raise ValueError("FillRecord.resolved_outcome is required for scoring")

        brier_score = (decision.prob_estimate - fill.resolved_outcome) ** 2
        model_id = _model_id(decision)
        return EvalRecord(
            market_id=fill.market_id,
            decision_id=decision.decision_id,
            prob_estimate=decision.prob_estimate,
            resolved_outcome=fill.resolved_outcome,
            brier_score=brier_score,
            fill_status=fill.status,
            recorded_at=datetime.now(tz=UTC),
            citations=[fill.trade_id],
            category=model_id,
            model_id=model_id,
            pnl=_pnl(fill, decision),
            slippage_bps=_slippage_bps(fill, decision),
            filled=fill.status == OrderStatus.MATCHED.value,
        )


def _model_id(decision: TradeDecision) -> str:
    return "unknown" if decision.model_id is None else decision.model_id


def _pnl(fill: FillRecord, decision: TradeDecision) -> float:
    if fill.resolved_outcome is None:
        return 0.0
    if decision.side == Side.SELL.value:
        return (fill.fill_price - fill.resolved_outcome) * fill.fill_size
    return (fill.resolved_outcome - fill.fill_price) * fill.fill_size


def _slippage_bps(fill: FillRecord, decision: TradeDecision) -> float:
    if decision.price == 0.0:
        return 0.0
    if decision.side == Side.SELL.value:
        slippage = decision.price - fill.fill_price
    else:
        slippage = fill.fill_price - decision.price
    return max(0.0, slippage / decision.price * 10_000)
