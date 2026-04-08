"""Protocol interfaces for all pluggable pms modules."""

from .connector import ConnectorProtocol
from .embedding import CorrelationDetectorProtocol, EmbeddingEngineProtocol, Vector
from .evaluation import FeedbackEngineProtocol, MetricsCollectorProtocol
from .execution import ExecutorProtocol, RiskManagerProtocol
from .storage import StorageProtocol
from .strategy import StrategyProtocol

__all__ = [
    "ConnectorProtocol",
    "CorrelationDetectorProtocol",
    "EmbeddingEngineProtocol",
    "ExecutorProtocol",
    "FeedbackEngineProtocol",
    "MetricsCollectorProtocol",
    "RiskManagerProtocol",
    "StorageProtocol",
    "StrategyProtocol",
    "Vector",
]
