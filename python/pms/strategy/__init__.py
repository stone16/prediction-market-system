"""Strategy implementations (CP07).

CP07 introduces ``ArbitrageStrategy`` — the first concrete strategy built on
``StrategyProtocol`` (CP01). Strategies satisfy the Protocol structurally;
``StrategyBase`` is provided as a thin convenience so future strategies have
a consistent ``name`` attribute without forcing inheritance.
"""

from .arbitrage import ArbitragePairOrders, ArbitrageStrategy
from .base import StrategyBase
from .llm_correlation import (
    DEFAULT_ANTHROPIC_MODEL,
    DEFAULT_REFINE_RELATIONS,
    AnthropicCorrelationClassifier,
    LLMClassifierProtocol,
    LLMCorrelationRefiner,
)

__all__ = [
    "AnthropicCorrelationClassifier",
    "ArbitragePairOrders",
    "ArbitrageStrategy",
    "DEFAULT_ANTHROPIC_MODEL",
    "DEFAULT_REFINE_RELATIONS",
    "LLMClassifierProtocol",
    "LLMCorrelationRefiner",
    "StrategyBase",
]
