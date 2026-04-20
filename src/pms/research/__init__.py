"""Research-side backtest models and execution utilities."""

from .comparison import (
    BacktestLiveComparison,
    BacktestLiveComparisonStore,
    BacktestLiveComparisonTool,
)
from .entities import (
    EvaluationRankingMetric,
    EvaluationReport,
    PortfolioTarget,
    PortfolioTargetKey,
    RankedStrategy,
    deserialize_portfolio_target_json,
    serialize_portfolio_target_json,
)
from .policies import (
    SelectionDenominator,
    SelectionSimilarityMetric,
    SymbolNormalizationPolicy,
    TimeAlignmentPolicy,
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
from .sweep import ParameterSweep

__all__ = [
    "BacktestLiveComparison",
    "BacktestLiveComparisonStore",
    "BacktestLiveComparisonTool",
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
    "ParameterSweep",
    "RankedStrategy",
    "ReplayEngineInvariantError",
    "RiskPolicy",
    "SelectionDenominator",
    "SelectionSimilarityMetric",
    "SymbolNormalizationPolicy",
    "TimeAlignmentPolicy",
    "deserialize_portfolio_target_json",
    "serialize_portfolio_target_json",
]
