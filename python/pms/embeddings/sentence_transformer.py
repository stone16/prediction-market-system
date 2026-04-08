"""Sentence-transformers backend for ``EmbeddingEngine`` (CP10).

Wrapping ``sentence_transformers.SentenceTransformer`` as a plain
``EncodeFunction`` keeps the core ``EmbeddingEngine`` free of ML
dependencies — unit tests inject a deterministic fake and only callers
wiring up the real model import this module.

The sentence-transformers package (plus torch / transformers) is a large
install, so it is intentionally kept as a **lazy, optional** dependency:

* The import of ``sentence_transformers`` lives inside ``__call__`` so
  importing ``pms.embeddings`` does not pull torch.
* A missing dependency raises ``ImportError`` with a concrete install
  hint instead of a bare ``ModuleNotFoundError``.
* The model is loaded on first call and cached on the instance. The
  ``all-MiniLM-L6-v2`` default is the spec-mandated model — small
  (~80 MB), fast, and deterministic with a fixed random seed.
"""

from __future__ import annotations

from typing import Any

import numpy as np
from numpy.typing import NDArray


class SentenceTransformerEncoder:
    """``EncodeFunction`` implementation backed by sentence-transformers."""

    def __init__(self, model_name: str = "all-MiniLM-L6-v2") -> None:
        self._model_name: str = model_name
        self._model: Any = None

    def __call__(self, texts: list[str]) -> NDArray[np.float32]:
        if self._model is None:
            try:
                from sentence_transformers import (  # type: ignore[import-not-found]
                    SentenceTransformer,
                )
            except ImportError as exc:  # pragma: no cover - exercised in smoke test only
                raise ImportError(
                    "sentence-transformers is required for "
                    "SentenceTransformerEncoder. Install it with: "
                    "uv add sentence-transformers"
                ) from exc
            self._model = SentenceTransformer(self._model_name)

        vectors = self._model.encode(texts, convert_to_numpy=True)
        result: NDArray[np.float32] = np.asarray(vectors, dtype=np.float32)
        return result
