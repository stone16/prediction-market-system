"""Research-side backtest models and execution utilities."""

from .entities import (
    EvaluationRankingMetric,
    EvaluationReport,
    PortfolioTarget,
    PortfolioTargetKey,
    RankedStrategy,
    deserialize_portfolio_target_json,
    serialize_portfolio_target_json,
)
from .replay import MarketUniverseReplayEngine, ReplayEngineInvariantError
from .report import EvaluationReportGenerator
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
    "EvaluationRankingMetric",
    "EvaluationReport",
    "EvaluationReportGenerator",
    "ExecutionModel",
    "MarketUniverseReplayEngine",
    "PortfolioTarget",
    "PortfolioTargetKey",
    "RankedStrategy",
    "ReplayEngineInvariantError",
    "RiskPolicy",
    "deserialize_portfolio_target_json",
    "serialize_portfolio_target_json",
]
