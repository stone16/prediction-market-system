"""Embedding and correlation-detection protocols.

``Vector`` is aliased to ``Any`` for CP01 so that ``numpy`` is not a hard
dependency at this checkpoint. CP10 will narrow this to
``numpy.typing.NDArray[np.float32]`` once numpy lands as a real dependency.
"""

from __future__ import annotations

from typing import Any, Protocol, TypeAlias, runtime_checkable

from pms.models import CorrelationPair, Market

# Alias kept loose to avoid forcing numpy as a CP01 dependency.
Vector: TypeAlias = Any


@runtime_checkable
class EmbeddingEngineProtocol(Protocol):
    """Vectorizes markets and finds similar pairs by cosine similarity."""

    async def embed_markets(self, markets: list[Market]) -> dict[str, Vector]:
        """Return a mapping of ``market_id`` to its embedding vector."""
        ...

    async def find_similar_pairs(
        self, threshold: float
    ) -> list[tuple[str, str, float]]:
        """Return ``(market_a_id, market_b_id, similarity)`` triples above
        the given cosine similarity threshold."""
        ...


@runtime_checkable
class CorrelationDetectorProtocol(Protocol):
    """Classifies relationships between embedded markets."""

    async def detect(self, markets: list[Market]) -> list[CorrelationPair]:
        """Return all detected correlation pairs from the input markets."""
        ...
