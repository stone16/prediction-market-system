"""Research-side backtest models and execution utilities."""

from .entities import (
    PortfolioTarget,
    PortfolioTargetKey,
    deserialize_portfolio_target_json,
    serialize_portfolio_target_json,
)
from .replay import MarketUniverseReplayEngine, ReplayEngineInvariantError
from .runner import BacktestRunner
from .specs import (
    BacktestDataset,
    BacktestExecutionConfig,
    BacktestSpec,
    ExecutionModel,
    RiskPolicy,
)

__all__ = [
    "BacktestDataset",
    "BacktestExecutionConfig",
    "BacktestRunner",
    "BacktestSpec",
    "ExecutionModel",
    "MarketUniverseReplayEngine",
    "PortfolioTarget",
    "PortfolioTargetKey",
    "ReplayEngineInvariantError",
    "RiskPolicy",
    "deserialize_portfolio_target_json",
    "serialize_portfolio_target_json",
]
