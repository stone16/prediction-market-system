"""Probe for sentence-transformers (UKPLab open semantic embedding lib).

Loads the canonical ``all-MiniLM-L6-v2`` model and encodes a single
short string. The model is auto-downloaded from Hugging Face on first
use, so the probe also exercises the package's network path. No
credentials are required.

Exit codes follow ``candidates/probes/README.md``.
"""

from __future__ import annotations

import json
import sys
import traceback


MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"
SAMPLE_TEXT = "Will it rain in San Francisco tomorrow?"


def main() -> int:
    try:
        from sentence_transformers import SentenceTransformer  # type: ignore[import-not-found]
    except ImportError as exc:
        sys.stderr.write(f"sentence_transformers import failed: {exc}\n")
        return 1

    try:
        model = SentenceTransformer(MODEL_NAME)
        embedding = model.encode(SAMPLE_TEXT)
    except Exception as exc:
        sys.stderr.write(f"sentence_transformers encode failed: {exc}\n")
        traceback.print_exc(file=sys.stderr)
        return 1

    # ``encode`` returns a numpy array; coerce to a list-len for the
    # JSON summary so the probe doesn't depend on numpy in the JSON path.
    try:
        dim = int(len(embedding))
    except TypeError:
        dim = -1

    summary = {
        "ok": True,
        "tool": "sentence-transformers",
        "model": MODEL_NAME,
        "embedding_dim": dim,
    }
    print(json.dumps(summary))
    return 0


if __name__ == "__main__":
    sys.exit(main())
