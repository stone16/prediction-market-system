from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from hashlib import sha256
import json
import uuid
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Literal, TypeVar, cast

from pms.config import PMSSettings
from pms.controller._price_utils import best_ask, spread_bps_at_decision
from pms.controller.calibrators.extreme_clamp import ExtremeProbClamp
from pms.controller.calibrators.netcal import NetcalCalibrator
from pms.controller.calibrators.shrinkage import LogitShrinkageCalibrator
from pms.controller.diagnostics import ControllerDiagnostic, DiagnosticSeverity
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
from pms.core.interfaces import ICalibrator, IForecaster, IPreCalibrator, ISizer
from pms.core.models import MarketSignal, Opportunity, Portfolio, TradeDecision
from pms.factors.composition import apply_composition, evaluate_branch_probabilities
from pms.strategies.projections import ActiveStrategy, CalibrationContext, CalibrationSpec

logger = logging.getLogger(__name__)

ForecastResult = tuple[float, float, str]
OpportunityEmission = tuple[Opportunity, TradeDecision]
T = TypeVar("T")
_ORIGINAL_RULES_PREDICT = RulesForecaster.predict
_ORIGINAL_STATISTICAL_PREDICT = StatisticalForecaster.predict


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
    _last_decision_emitted_at: dict[tuple[str, str, str, str, str], datetime] = field(
        init=False,
        default_factory=dict,
    )

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

    def _set_drop_diagnostic(
        self,
        signal: MarketSignal,
        *,
        code: str,
        message: str,
        severity: DiagnosticSeverity = "info",
        metadata: Mapping[str, Any] | None = None,
    ) -> None:
        """Record (and log) a ControllerDiagnostic for a signal that the
        pipeline drops without emitting a decision.

        Every operationally-meaningful early return in ``on_signal`` calls this
        so an operator watching ``state.controller_diagnostics`` can tell *why*
        order flow stopped — distinguishing 'idle' from 'every signal filtered'.
        """
        self.last_diagnostic = ControllerDiagnostic(
            code=code,
            message=message,
            market_id=signal.market_id,
            strategy_id=self.strategy_id,
            strategy_version_id=self.strategy_version_id,
            token_id=signal.token_id,
            severity=severity,
            metadata=dict(metadata or {}),
        )
        logger.info(
            "controller diagnostic %s for %s",
            code,
            signal.market_id,
            extra={"controller_diagnostic": self.last_diagnostic},
        )

    async def on_signal(
        self,
        signal: MarketSignal,
        portfolio: Portfolio | None = None,
    ) -> OpportunityEmission | None:
        self.last_diagnostic = None
        router = _required(self.router, "router")
        # Call gate() (rather than gate_reason() twice) so the router funnel
        # log is emitted exactly once per signal. gate() returns a bool; we
        # re-derive the specific reason only on failure so we can attach it
        # to the diagnostic.
        if not router.gate(signal):
            gate_reason = router.gate_reason(signal)
            _log_pipeline_funnel(signal, forecasted_count=0, traded_count=0)
            self.last_diagnostic = ControllerDiagnostic(
                code=f"router_gate:{gate_reason}",
                message=(
                    "Signal rejected by router gate: "
                    f"{(gate_reason or 'unknown').replace('_', ' ')}."
                ),
                market_id=signal.market_id,
                strategy_id=self.strategy_id,
                strategy_version_id=self.strategy_version_id,
                token_id=signal.token_id,
                severity="info",
                metadata={"gate_reason": gate_reason or "unknown"},
            )
            return None
        if signal.token_id is None:
            _log_pipeline_funnel(signal, forecasted_count=0, traded_count=0)
            self.last_diagnostic = ControllerDiagnostic(
                code="missing_token_id",
                message=(
                    "Signal arrived without token_id; skipping decision until "
                    "the sensor populates the outcome token."
                ),
                market_id=signal.market_id,
                strategy_id=self.strategy_id,
                strategy_version_id=self.strategy_version_id,
                token_id=None,
                severity="info",
                metadata={},
            )
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

        has_factor_composition = bool(
            self.strategy is not None and self.strategy.config.factor_composition
        )
        if not probabilities and not has_factor_composition:
            _log_pipeline_funnel(signal, forecasted_count=0, traded_count=0)
            self.last_diagnostic = ControllerDiagnostic(
                code="no_forecaster_output",
                message=(
                    "No forecaster produced a probability and the strategy has "
                    "no factor composition; skipping decision."
                ),
                market_id=signal.market_id,
                strategy_id=self.strategy_id,
                strategy_version_id=self.strategy_version_id,
                token_id=signal.token_id,
                severity="info",
                metadata={"forecaster_count": len(forecasters)},
            )
            return None

        prob_estimate = (
            sum(probabilities) / len(probabilities)
            if probabilities
            else signal.yes_price
        )
        factor_snapshot_hash: str | None = None
        composition_trace: dict[str, object] = {}
        signal_factor_values = _signal_factor_values(signal)
        runtime_factor_values = {
            (factor_id, ""): value
            for factor_id, value in runtime_probabilities.items()
        }
        if has_factor_composition and self.strategy is not None:
            factor_snapshot = await self.factor_reader.snapshot(
                market_id=signal.market_id,
                as_of=signal.timestamp,
                required=self.strategy.config.factor_composition,
                strategy_id=self.strategy.strategy_id,
                strategy_version_id=self.strategy.strategy_version_id,
            )
            factor_values = dict(factor_snapshot.values)
            factor_values.update(signal_factor_values)
            factor_values.update(runtime_factor_values)
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
                self._set_drop_diagnostic(
                    signal,
                    code="composition_resolution_failed",
                    message=(
                        "Skipping decision because factor composition could not "
                        "resolve a probability."
                    ),
                    severity="error",
                    metadata={
                        "error_type": type(exc).__name__,
                        "error": str(exc),
                    },
                )
                _log_pipeline_funnel(
                    signal,
                    forecasted_count=len(probabilities),
                    traded_count=0,
                )
                return None
        else:
            factor_values = dict(signal_factor_values)
            factor_values.update(runtime_factor_values)
        calibrated_estimate = _apply_pre_calibrators(
            prob_estimate,
            model_ids=model_ids,
            calibrator=calibrator,
            strategy=self.strategy,
            signal=signal,
        )
        if calibrated_estimate is None:
            self._set_drop_diagnostic(
                signal,
                code="calibration_clamp_rejected",
                message=(
                    "Skipping decision because pre-calibration rejected the "
                    "forecast (extreme probability with insufficient resolved "
                    "samples to trust it)."
                ),
                metadata={"raw_probability": prob_estimate},
            )
            _log_pipeline_funnel(
                signal,
                forecasted_count=len(probabilities),
                traded_count=0,
            )
            return None
        prob_estimate = calibrated_estimate
        yes_probability = prob_estimate
        yes_reference_price = _yes_reference_price(signal, self.strategy)
        yes_edge = yes_probability - yes_reference_price
        active_portfolio = portfolio or _default_portfolio()
        sizer = _required(self.sizer, "sizer")
        now = datetime.now(tz=UTC)
        opportunity_side: Literal["yes", "no"] = "yes"
        decision_token_id = signal.token_id
        decision_yes_token_id: str | None = signal.token_id
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
            decision_yes_token_id = outcome_tokens.yes_token_id
            decision_token_id = outcome_tokens.no_token_id
            decision_outcome = "NO"
            opportunity_side = "no"
            decision_probability = 1.0 - yes_probability
            decision_price = max(1e-6, min(1.0 - 1e-6, 1.0 - yes_reference_price))
            decision_edge = decision_probability - decision_price
        if decision_edge <= 0.0:
            self._set_drop_diagnostic(
                signal,
                code="decision_edge_not_positive",
                message=(
                    "Skipping decision because the best available edge after "
                    "side selection is not positive."
                ),
                metadata={
                    "decision_edge": decision_edge,
                    "decision_outcome": decision_outcome,
                    "decision_probability": decision_probability,
                    "decision_price": decision_price,
                },
            )
            _log_pipeline_funnel(
                signal,
                forecasted_count=len(probabilities),
                traded_count=0,
            )
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
            self._set_drop_diagnostic(
                signal,
                code="order_size_below_minimum",
                message=(
                    "Skipping decision because the sized order is below the "
                    "minimum order notional."
                ),
                metadata={
                    "size_usdc": size,
                    "min_order_usdc": min_order_usdc,
                    "decision_outcome": decision_outcome,
                },
            )
            _log_pipeline_funnel(
                signal,
                forecasted_count=len(probabilities),
                traded_count=0,
            )
            return None
        cooldown_key = (
            self.strategy_id,
            self.strategy_version_id,
            signal.market_id,
            decision_token_id,
            decision_outcome,
        )
        if _within_decision_cooldown(
            self._last_decision_emitted_at,
            key=cooldown_key,
            current_ts=signal.timestamp,
            settings=self.settings,
        ):
            self._set_drop_diagnostic(
                signal,
                code="within_decision_cooldown",
                message=(
                    "Skipping decision because an identical decision was emitted "
                    "within the configured cooldown window."
                ),
                metadata={
                    "cooldown_s": self.settings.controller.decision_cooldown_s,
                    "decision_outcome": decision_outcome,
                },
            )
            _log_pipeline_funnel(
                signal,
                forecasted_count=len(probabilities),
                traded_count=0,
            )
            return None
        self._last_decision_emitted_at[cooldown_key] = signal.timestamp
        _log_pipeline_funnel(
            signal,
            forecasted_count=len(probabilities),
            traded_count=1,
        )
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
            risk_group_id=_risk_group_id(signal),
            spread_bps_at_decision=spread_bps_at_decision(
                signal,
                token_id=decision_token_id,
                outcome=decision_outcome,
                yes_token_id=decision_yes_token_id,
            ),
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
        async_predict = getattr(forecaster, "apredict", None)
        if callable(async_predict) and not _predict_method_overridden(forecaster):
            return await cast(
                Callable[[MarketSignal], Awaitable[ForecastResult | None]],
                async_predict,
            )(signal)
        return await asyncio.to_thread(forecaster.predict, signal)


def _apply_pre_calibrators(
    prob_estimate: float,
    *,
    model_ids: Sequence[str],
    calibrator: ICalibrator,
    strategy: ActiveStrategy | None,
    signal: MarketSignal,
) -> float | None:
    raw_prob = prob_estimate
    spec = strategy.calibration if strategy is not None else CalibrationSpec()
    if not spec.enabled:
        return raw_prob
    context = CalibrationContext(
        resolved_sample_count=_resolved_sample_count(calibrator, model_ids),
        model_id=_decision_model_id(model_ids) or "unknown",
    )
    current: float | None = raw_prob
    for pre_calibrator in _pre_calibrators(spec):
        if current is None:
            return None
        next_prob = pre_calibrator.calibrate(current, context=context)
        if next_prob is None:
            logger.info(
                "calibration clamp rejected forecast for %s",
                signal.market_id,
                extra={
                    "event": "clamp_rejection",
                    "market_id": signal.market_id,
                    "raw_prob": raw_prob,
                    "calibrated_prob": current,
                    "clamp_action": "reject",
                    "resolved_sample_count": context.resolved_sample_count,
                },
            )
            return None
        current = next_prob
    return current


def _pre_calibrators(spec: CalibrationSpec) -> tuple[IPreCalibrator, ...]:
    return (LogitShrinkageCalibrator(spec), ExtremeProbClamp(spec))


def _resolved_sample_count(calibrator: ICalibrator, model_ids: Sequence[str]) -> int:
    sample_count = getattr(calibrator, "sample_count", None)
    if not callable(sample_count) or not model_ids:
        return 0
    return max(int(sample_count(model_id)) for model_id in model_ids)


def _log_pipeline_funnel(
    signal: MarketSignal,
    *,
    forecasted_count: int,
    traded_count: int,
) -> None:
    logger.info(
        "controller pipeline funnel market_id=%s forecasted=%d traded=%d",
        signal.market_id,
        forecasted_count,
        traded_count,
        extra={
            "event": "funnel_pipeline",
            "market_id": signal.market_id,
            "forecasted_count": forecasted_count,
            "traded_count": traded_count,
        },
    )


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


def _predict_method_overridden(forecaster: IForecaster) -> bool:
    if isinstance(forecaster, RulesForecaster):
        return type(forecaster).predict is not _ORIGINAL_RULES_PREDICT
    if isinstance(forecaster, StatisticalForecaster):
        return type(forecaster).predict is not _ORIGINAL_STATISTICAL_PREDICT
    return False


def _signal_factor_values(signal: MarketSignal) -> dict[tuple[str, str], float]:
    return {("yes_price", ""): signal.yes_price}


def _risk_group_id(signal: MarketSignal) -> str | None:
    explicit = _external_text(signal, "risk_group_id")
    if explicit is not None:
        return explicit
    legacy = _external_text(signal, "risk_group")
    if legacy is not None:
        return legacy
    event_id = _external_text(signal, "event_id")
    if event_id is not None:
        return f"event:{event_id}"
    category = _external_text(signal, "category")
    if category is not None:
        return f"category:{category}"
    return None


def _external_text(signal: MarketSignal, key: str) -> str | None:
    raw_value = signal.external_signal.get(key)
    if raw_value is None:
        return None
    value = str(raw_value).strip()
    return value or None


def _strict_factor_gates(settings: PMSSettings) -> bool:
    return settings.mode == RunMode.LIVE or settings.controller.strict_factor_gates


def _within_decision_cooldown(
    emitted_at_by_key: dict[tuple[str, str, str, str, str], datetime],
    *,
    key: tuple[str, str, str, str, str],
    current_ts: datetime,
    settings: PMSSettings,
) -> bool:
    if settings.mode == RunMode.BACKTEST:
        return False
    cooldown_s = settings.controller.decision_cooldown_s
    if cooldown_s <= 0.0:
        return False
    previous_ts = emitted_at_by_key.get(key)
    if previous_ts is None:
        return False
    return (current_ts - previous_ts).total_seconds() < cooldown_s


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


def _yes_reference_price(
    signal: MarketSignal,
    strategy: ActiveStrategy | None,
) -> float:
    if _strategy_metadata(strategy).get("price_reference") != "best_ask":
        return signal.yes_price
    executable_price = best_ask(signal)
    return signal.yes_price if executable_price is None else executable_price


def _strategy_metadata(strategy: ActiveStrategy | None) -> dict[str, str]:
    if strategy is None:
        return {}
    return dict(strategy.config.metadata)


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
