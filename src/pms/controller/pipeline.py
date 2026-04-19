from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
import uuid
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import TypeVar

from pms.config import PMSSettings
from pms.controller.calibrators.netcal import NetcalCalibrator
from pms.controller.forecasters.llm import LLMForecaster
from pms.controller.forecasters.rules import RulesForecaster
from pms.controller.forecasters.statistical import StatisticalForecaster
from pms.controller.router import Router
from pms.controller.sizers.kelly import KellySizer
from pms.core.interfaces import ICalibrator, IForecaster, ISizer
from pms.core.models import MarketSignal, Opportunity, Portfolio, TradeDecision

logger = logging.getLogger(__name__)

ForecastResult = tuple[float, float, str]
OpportunityEmission = tuple[Opportunity, TradeDecision]
T = TypeVar("T")


@dataclass
class ControllerPipeline:
    strategy_id: str = "default"
    strategy_version_id: str = "default-v1"
    forecasters: Sequence[IForecaster] | None = None
    calibrator: ICalibrator | None = None
    sizer: ISizer | None = None
    router: Router | None = None
    settings: PMSSettings = field(default_factory=PMSSettings)

    def __post_init__(self) -> None:
        if self.forecasters is None:
            self.forecasters = (
                RulesForecaster(),
                StatisticalForecaster(),
                LLMForecaster(),
            )
        if self.calibrator is None:
            self.calibrator = NetcalCalibrator()
        if self.sizer is None:
            self.sizer = KellySizer(risk=self.settings.risk)
        if self.router is None:
            self.router = Router(self.settings.controller)

    async def on_signal(
        self,
        signal: MarketSignal,
        portfolio: Portfolio | None = None,
    ) -> OpportunityEmission | None:
        router = _required(self.router, "router")
        if not router.gate(signal):
            return None
        if signal.token_id is None:
            return None

        forecasters = _required(self.forecasters, "forecasters")
        tasks = [
            self._predict_forecaster(forecaster, signal)
            for forecaster in forecasters
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        probabilities: list[float] = []
        model_ids: list[str] = []
        rationales: list[str] = []
        calibrator = _required(self.calibrator, "calibrator")
        for forecaster, result in zip(forecasters, results, strict=True):
            if isinstance(result, BaseException):
                logger.warning("forecaster failed: %s", result)
                continue
            if result is None:
                continue
            model_id = _model_id(result, forecaster)
            model_ids.append(model_id)
            probabilities.append(calibrator.calibrate(float(result[0]), model_id=model_id))
            if result[2]:
                rationales.append(f"{model_id}:{result[2]}")

        if not probabilities:
            return None

        prob_estimate = sum(probabilities) / len(probabilities)
        expected_edge = prob_estimate - signal.yes_price
        active_portfolio = portfolio or _default_portfolio()
        sizer = _required(self.sizer, "sizer")
        size = sizer.size(
            prob=prob_estimate,
            market_price=signal.yes_price,
            portfolio=active_portfolio,
        )
        now = datetime.now(tz=UTC)
        opportunity = Opportunity(
            opportunity_id=f"opportunity-{uuid.uuid4().hex}",
            market_id=signal.market_id,
            token_id=signal.token_id,
            side="yes" if expected_edge >= 0.0 else "no",
            selected_factor_values=_selected_factor_values(signal),
            expected_edge=expected_edge,
            rationale=_rationale_text(rationales),
            target_size_usdc=size,
            expiry=signal.resolves_at,
            staleness_policy="market_signal_freshness",
            strategy_id=self.strategy_id,
            strategy_version_id=self.strategy_version_id,
            created_at=now,
        )

        return opportunity, TradeDecision(
            decision_id=f"decision-{uuid.uuid4().hex}",
            market_id=signal.market_id,
            token_id=signal.token_id,
            venue=signal.venue,
            side="BUY" if expected_edge >= 0.0 else "SELL",
            price=signal.yes_price,
            size=size,
            order_type="limit",
            max_slippage_bps=self.settings.controller.max_slippage_bps,
            stop_conditions=router.stop_conditions(signal),
            prob_estimate=prob_estimate,
            expected_edge=expected_edge,
            time_in_force=self.settings.controller.time_in_force,
            opportunity_id=opportunity.opportunity_id,
            strategy_id=self.strategy_id,
            strategy_version_id=self.strategy_version_id,
            model_id=_decision_model_id(model_ids),
        )

    async def decide(
        self,
        signal: MarketSignal,
        portfolio: Portfolio | None = None,
    ) -> TradeDecision | None:
        emission = await self.on_signal(signal, portfolio=portfolio)
        if emission is None:
            return None
        _, decision = emission
        return decision

    async def _predict_forecaster(
        self,
        forecaster: IForecaster,
        signal: MarketSignal,
    ) -> ForecastResult | None:
        return await asyncio.to_thread(forecaster.predict, signal)


def _model_id(result: ForecastResult, forecaster: IForecaster) -> str:
    raw_model_id = getattr(result, "model_id", None)
    if isinstance(raw_model_id, str) and raw_model_id:
        return raw_model_id
    return forecaster.__class__.__name__


def _decision_model_id(model_ids: Sequence[str]) -> str | None:
    unique_ids = tuple(dict.fromkeys(model_ids))
    if not unique_ids:
        return None
    if len(unique_ids) == 1:
        return unique_ids[0]
    return "ensemble"


def _selected_factor_values(signal: MarketSignal) -> dict[str, float]:
    return {
        key: float(value)
        for key, value in signal.external_signal.items()
        if isinstance(value, (int, float)) and not isinstance(value, bool)
    }


def _rationale_text(rationales: Sequence[str]) -> str:
    if rationales:
        return " | ".join(dict.fromkeys(rationales))
    return "calibrated market signal"


def _default_portfolio() -> Portfolio:
    return Portfolio(
        total_usdc=0.0,
        free_usdc=0.0,
        locked_usdc=0.0,
        open_positions=[],
    )


def _required(value: T | None, name: str) -> T:
    if value is None:
        msg = f"ControllerPipeline {name} is not initialized"
        raise RuntimeError(msg)
    return value
