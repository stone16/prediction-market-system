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
        if (
            fill.strategy_id != decision.strategy_id
            or fill.strategy_version_id != decision.strategy_version_id
        ):
            msg = (
                "FillRecord and TradeDecision strategy identity must match for scoring"
            )
            raise ValueError(msg)

        brier_score = (decision.prob_estimate - fill.resolved_outcome) ** 2
        model_id = _model_id(decision)
        return EvalRecord(
            market_id=fill.market_id,
            decision_id=decision.decision_id,
            strategy_id=fill.strategy_id,
            strategy_version_id=fill.strategy_version_id,
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
    contract_outcome = _contract_outcome(fill.resolved_outcome, decision)
    action = decision.action if decision.action is not None else decision.side
    if action == Side.SELL.value:
        return (fill.fill_price - contract_outcome) * fill.fill_size
    return (contract_outcome - fill.fill_price) * fill.fill_size


def _slippage_bps(fill: FillRecord, decision: TradeDecision) -> float:
    limit_price = decision.limit_price if decision.limit_price is not None else decision.price
    if limit_price == 0.0:
        return 0.0
    action = decision.action if decision.action is not None else decision.side
    if action == Side.SELL.value:
        slippage = limit_price - fill.fill_price
    else:
        slippage = fill.fill_price - limit_price
    return max(0.0, slippage / limit_price * 10_000)


def _contract_outcome(resolved_outcome: float, decision: TradeDecision) -> float:
    if decision.outcome == "NO":
        return 1.0 - resolved_outcome
    return resolved_outcome
