from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from hashlib import sha256
import json
import uuid
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Literal, TypeVar

from pms.config import PMSSettings
from pms.controller.calibrators.netcal import NetcalCalibrator
from pms.controller.diagnostics import ControllerDiagnostic
from pms.controller.factor_snapshot import (
    FactorKey,
    FactorSnapshotReader,
    NullFactorSnapshotReader,
    required_factor_keys,
)
from pms.controller.forecasters.llm import LLMForecaster
from pms.controller.forecasters.rules import RulesForecaster
from pms.controller.forecasters.statistical import StatisticalForecaster
from pms.controller.outcome_tokens import (
    NullOutcomeTokenResolver,
    OutcomeTokenResolver,
)
from pms.controller.router import Router
from pms.controller.sizers.kelly import KellySizer
from pms.core.enums import RunMode, TimeInForce
from pms.core.interfaces import ICalibrator, IForecaster, ISizer
from pms.core.models import MarketSignal, Opportunity, Portfolio, TradeDecision
from pms.factors.composition import apply_composition, evaluate_branch_probabilities
from pms.strategies.projections import ActiveStrategy

logger = logging.getLogger(__name__)

ForecastResult = tuple[float, float, str]
OpportunityEmission = tuple[Opportunity, TradeDecision]
T = TypeVar("T")


@dataclass
class ControllerPipeline:
    strategy_id: str = "default"
    strategy_version_id: str = "default-v1"
    strategy: ActiveStrategy | None = None
    factor_reader: FactorSnapshotReader = field(
        default_factory=NullFactorSnapshotReader
    )
    outcome_token_resolver: OutcomeTokenResolver = field(
        default_factory=NullOutcomeTokenResolver
    )
    forecasters: Sequence[IForecaster] | None = None
    calibrator: ICalibrator | None = None
    sizer: ISizer | None = None
    router: Router | None = None
    settings: PMSSettings = field(default_factory=PMSSettings)
    last_diagnostic: ControllerDiagnostic | None = field(init=False, default=None)
    suppressed_zero_size: int = field(init=False, default=0)

    def __post_init__(self) -> None:
        if self.strategy is not None:
            self.strategy_id = self.strategy.strategy_id
            self.strategy_version_id = self.strategy.strategy_version_id
        if self.forecasters is None:
            self.forecasters = (
                RulesForecaster(),
                StatisticalForecaster(),
                LLMForecaster(config=self.settings.llm),
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
        self.last_diagnostic = None
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
        runtime_probabilities: dict[str, float] = {}
        forecaster_names = _strategy_forecaster_names(self.strategy)
        calibrator = _required(self.calibrator, "calibrator")
        for index, (forecaster, result) in enumerate(
            zip(forecasters, results, strict=True)
        ):
            if isinstance(result, BaseException):
                logger.warning("forecaster failed: %s", result)
                continue
            if result is None:
                continue
            model_id = _model_id(result, forecaster)
            calibrated_probability = calibrator.calibrate(
                float(result[0]),
                model_id=model_id,
            )
            model_ids.append(model_id)
            probabilities.append(calibrated_probability)
            if result[2]:
                rationales.append(f"{model_id}:{result[2]}")
            runtime_factor_id = _runtime_factor_id(
                forecaster_names[index] if index < len(forecaster_names) else None
            )
            if runtime_factor_id is not None and not _is_placeholder_forecast(
                result,
                model_id=model_id,
            ):
                runtime_probabilities[runtime_factor_id] = calibrated_probability

        if not probabilities:
            return None

        prob_estimate = sum(probabilities) / len(probabilities)
        factor_snapshot_hash: str | None = None
        composition_trace: dict[str, object] = {}
        factor_values = _signal_factor_values(signal)
        factor_values.update(
            {
                (factor_id, ""): value
                for factor_id, value in runtime_probabilities.items()
            }
        )
        if self.strategy is not None and self.strategy.config.factor_composition:
            factor_snapshot = await self.factor_reader.snapshot(
                market_id=signal.market_id,
                as_of=signal.timestamp,
                required=self.strategy.config.factor_composition,
                strategy_id=self.strategy.strategy_id,
                strategy_version_id=self.strategy.strategy_version_id,
            )
            signal_factor_values = _signal_factor_values(signal)
            factor_values = dict(factor_snapshot.values)
            factor_values.update(signal_factor_values)
            factor_values.update(
                {
                    (factor_id, ""): value
                    for factor_id, value in runtime_probabilities.items()
                }
            )
            reported_missing_factors = factor_snapshot.missing_factors
            if not reported_missing_factors and not factor_snapshot.values:
                reported_missing_factors = required_factor_keys(
                    self.strategy.config.factor_composition
                )
            unresolved_missing_factors = tuple(
                key for key in reported_missing_factors if key not in factor_values
            )
            if _strict_factor_gates(self.settings) and unresolved_missing_factors:
                self.last_diagnostic = _composition_diagnostic(
                    code="missing_required_factors",
                    message="Skipping decision because required raw factors are missing.",
                    signal=signal,
                    strategy_id=self.strategy_id,
                    strategy_version_id=self.strategy_version_id,
                    factors=unresolved_missing_factors,
                )
                logger.info(
                    "controller diagnostic %s for %s",
                    self.last_diagnostic.code,
                    signal.market_id,
                    extra={"controller_diagnostic": self.last_diagnostic},
                )
                return None
            unresolved_stale_factors = tuple(
                key for key in factor_snapshot.stale_factors if key not in signal_factor_values
            )
            if _strict_factor_gates(self.settings) and unresolved_stale_factors:
                self.last_diagnostic = _composition_diagnostic(
                    code="stale_required_factors",
                    message="Skipping decision because required raw factors are stale.",
                    signal=signal,
                    strategy_id=self.strategy_id,
                    strategy_version_id=self.strategy_version_id,
                    factors=unresolved_stale_factors,
                )
                logger.info(
                    "controller diagnostic %s for %s",
                    self.last_diagnostic.code,
                    signal.market_id,
                    extra={"controller_diagnostic": self.last_diagnostic},
                )
                return None
            try:
                branch_probabilities = evaluate_branch_probabilities(
                    self.strategy.config.factor_composition,
                    factor_values,
                )
                if (
                    self.settings.mode != RunMode.LIVE
                    and not branch_probabilities
                    and unresolved_missing_factors
                    and "resolved_outcome" in signal.external_signal
                ):
                    prob_estimate = 0.5
                else:
                    prob_estimate = apply_composition(
                        self.strategy.config.factor_composition,
                        factor_values,
                    )
                factor_snapshot_hash = factor_snapshot.snapshot_hash
                composition_trace = {
                    "selected_probability": prob_estimate,
                    "expected_edge": prob_estimate - signal.yes_price,
                    "factor_snapshot_hash": factor_snapshot.snapshot_hash,
                    "missing_factors": [
                        _factor_key_label((factor_id, param))
                        for factor_id, param in unresolved_missing_factors
                    ],
                    "branch_probabilities": branch_probabilities,
                }
                if unresolved_stale_factors:
                    composition_trace["stale_factors"] = [
                        _factor_key_label((factor_id, param))
                        for factor_id, param in unresolved_stale_factors
                    ]
                if factor_snapshot.ages_ms:
                    composition_trace["factor_ages_ms"] = {
                        _factor_key_label((factor_id, param)): age
                        for (factor_id, param), age in factor_snapshot.ages_ms.items()
                    }
            except (KeyError, ValueError) as exc:
                logger.warning("composition resolution failed: %s", exc)
                return None
        yes_probability = prob_estimate
        yes_reference_price = signal.yes_price
        yes_edge = yes_probability - yes_reference_price
        active_portfolio = portfolio or _default_portfolio()
        sizer = _required(self.sizer, "sizer")
        now = datetime.now(tz=UTC)
        opportunity_side: Literal["yes", "no"] = "yes"
        decision_token_id = signal.token_id
        decision_outcome: Literal["YES", "NO"] = "YES"
        decision_probability = yes_probability
        decision_price = yes_reference_price
        decision_edge = yes_edge
        if yes_edge < 0.0:
            outcome_tokens = await self.outcome_token_resolver.resolve(
                market_id=signal.market_id,
                signal_token_id=signal.token_id,
            )
            if outcome_tokens.no_token_id is None:
                self.last_diagnostic = ControllerDiagnostic(
                    code="missing_no_token",
                    message=(
                        "Skipping bearish decision because no NO token could be resolved."
                    ),
                    market_id=signal.market_id,
                    strategy_id=self.strategy_id,
                    strategy_version_id=self.strategy_version_id,
                    token_id=signal.token_id,
                    severity="warning",
                    metadata={
                        "signal_token_id": signal.token_id,
                        "yes_token_id": outcome_tokens.yes_token_id,
                        "outcome": "NO",
                    },
                )
                logger.info(
                    "controller diagnostic %s for %s",
                    self.last_diagnostic.code,
                    signal.market_id,
                    extra={"controller_diagnostic": self.last_diagnostic},
                )
                return None
            decision_token_id = outcome_tokens.no_token_id
            decision_outcome = "NO"
            opportunity_side = "no"
            decision_probability = 1.0 - yes_probability
            decision_price = max(1e-6, min(1.0 - 1e-6, 1.0 - yes_reference_price))
            decision_edge = decision_probability - decision_price
        if decision_edge <= 0.0:
            return None
        size = sizer.size(
            prob=decision_probability,
            market_price=decision_price,
            portfolio=active_portfolio,
        )
        min_order_usdc = self.settings.risk.min_order_usdc
        if self.strategy is not None:
            min_order_usdc = self.strategy.risk.min_order_size_usdc
        if size <= 0.0 or size < min_order_usdc:
            self.suppressed_zero_size += 1
            return None
        opportunity = Opportunity(
            opportunity_id=f"opportunity-{uuid.uuid4().hex}",
            market_id=signal.market_id,
            token_id=decision_token_id,
            side=opportunity_side,
            selected_factor_values=_selected_factor_values(factor_values),
            expected_edge=decision_edge,
            rationale=_rationale_text(rationales),
            target_size_usdc=size,
            expiry=signal.resolves_at,
            staleness_policy="market_signal_freshness",
            strategy_id=self.strategy_id,
            strategy_version_id=self.strategy_version_id,
            created_at=now,
            factor_snapshot_hash=factor_snapshot_hash,
            composition_trace={
                **composition_trace,
                "selected_probability": decision_probability,
                "expected_edge": decision_edge,
                "yes_probability": yes_probability,
                "yes_reference_price": yes_reference_price,
                "yes_edge": yes_edge,
                "traded_outcome": decision_outcome,
                "traded_probability": decision_probability,
                "traded_price": decision_price,
                "traded_edge": decision_edge,
            },
        )

        intent_key = _intent_key(
            strategy_id=self.strategy_id,
            strategy_version_id=self.strategy_version_id,
            market_id=signal.market_id,
            token_id=decision_token_id,
            action="BUY",
            outcome=decision_outcome,
            limit_price=decision_price,
            notional_usdc=size,
            factor_snapshot_hash=factor_snapshot_hash,
            signal_timestamp=signal.timestamp,
        )
        return opportunity, TradeDecision(
            decision_id=f"decision-{uuid.uuid4().hex}",
            market_id=signal.market_id,
            token_id=decision_token_id,
            venue=signal.venue,
            side="BUY",
            limit_price=decision_price,
            notional_usdc=size,
            order_type="limit",
            max_slippage_bps=self.settings.controller.max_slippage_bps,
            stop_conditions=router.stop_conditions(signal),
            prob_estimate=decision_probability,
            expected_edge=decision_edge,
            time_in_force=TimeInForce(self.settings.controller.time_in_force.upper()),
            opportunity_id=opportunity.opportunity_id,
            strategy_id=self.strategy_id,
            strategy_version_id=self.strategy_version_id,
            action="BUY",
            outcome=decision_outcome,
            model_id=_decision_model_id(model_ids),
            intent_key=intent_key,
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


def _selected_factor_values(factor_values: dict[FactorKey, float]) -> dict[str, float]:
    return {
        _factor_key_label(key): value
        for key, value in sorted(factor_values.items())
    }


def _composition_diagnostic(
    *,
    code: str,
    message: str,
    signal: MarketSignal,
    strategy_id: str,
    strategy_version_id: str,
    factors: Sequence[FactorKey],
) -> ControllerDiagnostic:
    return ControllerDiagnostic(
        code=code,
        message=message,
        market_id=signal.market_id,
        strategy_id=strategy_id,
        strategy_version_id=strategy_version_id,
        token_id=signal.token_id,
        severity="warning",
        metadata={
            "factors": [_factor_key_label(key) for key in factors],
        },
    )


def _is_placeholder_forecast(
    result: ForecastResult,
    *,
    model_id: str,
) -> bool:
    return model_id == "neutral" or result[2] == "pre-s5-neutral"


def _signal_factor_values(signal: MarketSignal) -> dict[tuple[str, str], float]:
    return {("yes_price", ""): signal.yes_price}


def _strict_factor_gates(settings: PMSSettings) -> bool:
    return settings.mode == RunMode.LIVE or settings.controller.strict_factor_gates


def _factor_key_label(key: FactorKey) -> str:
    factor_id, param = key
    if not param:
        return factor_id
    return f"{factor_id}:{param}"


def _strategy_forecaster_names(strategy: ActiveStrategy | None) -> tuple[str, ...]:
    if strategy is None:
        return ()
    return tuple(name for name, _ in strategy.forecaster.forecasters)


def _runtime_factor_id(forecaster_name: str | None) -> str | None:
    if forecaster_name is None:
        return None
    if forecaster_name == "stats":
        return "statistical"
    if forecaster_name in {"rules", "llm"}:
        return forecaster_name
    return None


def _intent_key(
    *,
    strategy_id: str,
    strategy_version_id: str,
    market_id: str,
    token_id: str | None,
    action: str,
    outcome: str,
    limit_price: float,
    notional_usdc: float,
    factor_snapshot_hash: str | None,
    signal_timestamp: datetime,
) -> str:
    payload = {
        "strategy_id": strategy_id,
        "strategy_version_id": strategy_version_id,
        "market_id": market_id,
        "token_id": token_id,
        "action": action,
        "outcome": outcome,
        "limit_price": round(limit_price, 4),
        "notional_usdc": round(notional_usdc, 2),
        "factor_snapshot_hash": factor_snapshot_hash,
        "signal_ts_bucket": signal_timestamp.replace(microsecond=0).isoformat(),
    }
    return sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode(
            "utf-8"
        )
    ).hexdigest()


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
