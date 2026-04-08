"""Embedding engine package (CP10).

CP10 introduces ``EmbeddingEngine`` — the first concrete
``EmbeddingEngineProtocol`` implementation. The engine is intentionally
decoupled from any particular model: it takes a callable ``encode_fn`` so
unit tests can inject a deterministic fake and production callers can wire
up a real sentence-transformers backend via
``pms.embeddings.sentence_transformer.SentenceTransformerEncoder``.
"""

from .engine import EmbeddingEngine, EncodeFunction

__all__ = [
    "EmbeddingEngine",
    "EncodeFunction",
]
