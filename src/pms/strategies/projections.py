"""Immutable strategy projection value objects."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class StrategyConfig:
    strategy_id: str
    factor_composition: tuple[tuple[str, float], ...]
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
