from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field

from pms.config import LLMSettings, PMSSettings, RiskSettings
from pms.controller.calibrators.netcal import NetcalCalibrator
from pms.controller.factor_snapshot import (
    FactorSnapshotReader,
    NullFactorSnapshotReader,
)
from pms.controller.forecasters.llm import LLMForecaster
from pms.controller.forecasters.rules import RulesForecaster
from pms.controller.forecasters.statistical import StatisticalForecaster
from pms.controller.outcome_tokens import (
    NullOutcomeTokenResolver,
    OutcomeTokenResolver,
)
from pms.controller.pipeline import ControllerPipeline
from pms.controller.router import Router
from pms.controller.sizers.kelly import KellySizer
from pms.core.interfaces import IForecaster
from pms.strategies.projections import ActiveStrategy


@dataclass
class ControllerPipelineFactory:
    settings: PMSSettings = field(default_factory=PMSSettings)
    factor_reader: FactorSnapshotReader = field(
        default_factory=NullFactorSnapshotReader
    )
    outcome_token_resolver: OutcomeTokenResolver = field(
        default_factory=NullOutcomeTokenResolver
    )

    def build_many(
        self,
        strategies: Sequence[ActiveStrategy],
    ) -> dict[str, ControllerPipeline]:
        return {
            strategy.strategy_id: self.build(strategy)
            for strategy in strategies
        }

    def build(self, strategy: ActiveStrategy) -> ControllerPipeline:
        return ControllerPipeline(
            strategy=strategy,
            strategy_id=strategy.strategy_id,
            strategy_version_id=strategy.strategy_version_id,
            factor_reader=self.factor_reader,
            outcome_token_resolver=self.outcome_token_resolver,
            forecasters=self._build_forecasters(strategy),
            calibrator=NetcalCalibrator(),
            sizer=KellySizer(
                risk=_risk_settings(
                    strategy,
                    fallback=self.settings.risk,
                )
            ),
            router=Router(self.settings.controller),
            settings=self.settings,
        )

    def _build_forecasters(
        self,
        strategy: ActiveStrategy,
    ) -> tuple[IForecaster, ...]:
        return tuple(
            _build_forecaster(
                name=name,
                raw_params=raw_params,
                llm_settings=self.settings.llm,
            )
            for name, raw_params in strategy.forecaster.forecasters
        )


def _build_forecaster(
    *,
    name: str,
    raw_params: tuple[tuple[str, str], ...],
    llm_settings: LLMSettings,
) -> IForecaster:
    params = dict(raw_params)
    if name == "rules":
        threshold = params.get("threshold")
        return RulesForecaster(
            min_edge=0.02 if threshold is None else float(threshold)
        )
    if name == "stats":
        prior_strength = params.get("prior_strength")
        return StatisticalForecaster(
            prior_strength=2.0 if prior_strength is None else float(prior_strength)
        )
    if name == "llm":
        if raw_params:
            msg = (
                "LLMForecaster does not yet accept per-strategy params: "
                f"{raw_params!r}"
            )
            raise ValueError(msg)
        return LLMForecaster(config=llm_settings)
    msg = f"Unsupported forecaster {name!r}"
    raise ValueError(msg)


def _risk_settings(
    strategy: ActiveStrategy,
    *,
    fallback: RiskSettings,
) -> RiskSettings:
    return RiskSettings(
        max_position_per_market=strategy.risk.max_position_notional_usdc,
        max_total_exposure=fallback.max_total_exposure,
        max_drawdown_pct=strategy.risk.max_daily_drawdown_pct,
        max_open_positions=fallback.max_open_positions,
        min_order_usdc=strategy.risk.min_order_size_usdc,
        slippage_threshold_bps=fallback.slippage_threshold_bps,
    )
