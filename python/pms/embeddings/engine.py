"""Embedding engine — vectorize markets and surface similar pairs.

``EmbeddingEngine`` implements ``EmbeddingEngineProtocol`` (CP01). Design
notes:

* **Injectable encoder.** The engine accepts an arbitrary
  ``Callable[[list[str]], NDArray[np.float32]]`` instead of hard-coding a
  model. Unit tests inject a deterministic hash-bag-of-words encoder so
  they never pull the ~80 MB sentence-transformers model, while production
  wiring passes ``SentenceTransformerEncoder`` from the sibling module.
* **Per-market caching.** Vectors are cached by ``market_id`` so repeated
  ``embed_markets`` calls on an overlapping set only re-encode the net-new
  markets. Callers can (and do in the CP10 tests) rely on this to keep
  O(N²) similarity search cheap across multiple detection passes.
* **Text source.** Each market is represented by ``title`` + ``" "`` +
  ``description`` with leading/trailing whitespace stripped. This keeps
  the encoder focused on semantic signal; metadata like volume or URL
  would only add noise under the bag-of-words fake encoder used in tests.
* **Similarity search.** ``find_similar_pairs`` is an explicit O(N²) loop.
  With the CP10 working set of <1000 markets per pipeline tick this is
  comfortably sub-millisecond even in CPython; production scale should
  swap in an ANN index (faiss, hnswlib). Results are sorted by similarity
  descending so downstream consumers (the detector) surface the strongest
  candidates first — deterministic ordering also makes tests stable.
"""

from __future__ import annotations

from typing import Callable

import numpy as np
from numpy.typing import NDArray

from pms.models import Market

EncodeFunction = Callable[[list[str]], NDArray[np.float32]]


class EmbeddingEngine:
    """Vectorizes markets and finds similar pairs by cosine similarity."""

    def __init__(self, encode_fn: EncodeFunction) -> None:
        self._encode_fn: EncodeFunction = encode_fn
        self._vectors: dict[str, NDArray[np.float32]] = {}

    # ------------------------------------------------------------------
    # Protocol methods
    # ------------------------------------------------------------------

    async def embed_markets(
        self, markets: list[Market]
    ) -> dict[str, NDArray[np.float32]]:
        """Return ``market_id -> vector`` for every input market.

        Markets already present in the cache are reused as-is. Net-new
        markets are batch-encoded once so the underlying encoder sees the
        minimum number of calls.
        """
        to_encode: list[Market] = [
            m for m in markets if m.market_id not in self._vectors
        ]

        if to_encode:
            texts = [self._text_for(m) for m in to_encode]
            vectors = self._encode_fn(texts)
            if vectors.dtype != np.float32:
                vectors = vectors.astype(np.float32)
            for market, vec in zip(to_encode, vectors, strict=True):
                self._vectors[market.market_id] = vec

        return {m.market_id: self._vectors[m.market_id] for m in markets}

    async def find_similar_pairs(
        self, threshold: float
    ) -> list[tuple[str, str, float]]:
        """Return ``(id_a, id_b, similarity)`` triples above ``threshold``.

        Scans every unordered pair of cached vectors. Pairs are returned
        sorted by similarity descending and ties broken by (id_a, id_b)
        lexicographically so callers see a stable order.
        """
        ids = list(self._vectors.keys())
        results: list[tuple[str, str, float]] = []
        for i, id_a in enumerate(ids):
            vec_a = self._vectors[id_a]
            for id_b in ids[i + 1 :]:
                vec_b = self._vectors[id_b]
                sim = _cosine(vec_a, vec_b)
                if sim >= threshold:
                    results.append((id_a, id_b, sim))

        # Primary sort: similarity desc. Secondary: id_a, id_b ascending
        # so equal-similarity ties are deterministic across runs.
        results.sort(key=lambda triple: (-triple[2], triple[0], triple[1]))
        return results

    # ------------------------------------------------------------------
    # Observability helpers (not in protocol — used by tests & callers)
    # ------------------------------------------------------------------

    def get_vector(self, market_id: str) -> NDArray[np.float32] | None:
        """Return the cached vector for ``market_id`` or ``None``."""
        return self._vectors.get(market_id)

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    @staticmethod
    def _text_for(market: Market) -> str:
        return f"{market.title} {market.description}".strip()


def _cosine(a: NDArray[np.float32], b: NDArray[np.float32]) -> float:
    """Cosine similarity of two 1-D float32 vectors.

    Returns ``0.0`` when either vector has zero norm to avoid division by
    zero. Callers treat zero as "no similarity" which is the correct
    fallback for an all-zero (no tokens) fake-encoder embedding.
    """
    norm_a = float(np.linalg.norm(a))
    norm_b = float(np.linalg.norm(b))
    if norm_a == 0.0 or norm_b == 0.0:
        return 0.0
    return float(np.dot(a, b)) / (norm_a * norm_b)
