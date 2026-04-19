"""Immutable strategy projection value objects."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True, slots=True)
class FactorCompositionStep:
    factor_id: str
    role: str
    param: str
    weight: float
    threshold: float | None


@dataclass(frozen=True, slots=True)
class StrategyConfig:
    strategy_id: str
    factor_composition: tuple[FactorCompositionStep, ...]
    metadata: tuple[tuple[str, str], ...]


@dataclass(frozen=True, slots=True)
class RiskParams:
    max_position_notional_usdc: float
    max_daily_drawdown_pct: float
    min_order_size_usdc: float


@dataclass(frozen=True, slots=True)
class EvalSpec:
    metrics: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ForecasterSpec:
    forecasters: tuple[tuple[str, tuple[tuple[str, str], ...]], ...]


@dataclass(frozen=True, slots=True)
class MarketSelectionSpec:
    venue: str
    resolution_time_max_horizon_days: int | None
    volume_min_usdc: float


@dataclass(frozen=True, slots=True)
class StrategyRow:
    strategy_id: str
    active_version_id: str | None
    created_at: datetime


@dataclass(frozen=True, slots=True)
class StrategyVersion:
    strategy_id: str
    strategy_version_id: str
    created_at: datetime


@dataclass(frozen=True, slots=True)
class ActiveStrategy:
    strategy_id: str
    strategy_version_id: str
    config: StrategyConfig
    risk: RiskParams
    eval_spec: EvalSpec
    forecaster: ForecasterSpec
    market_selection: MarketSelectionSpec
