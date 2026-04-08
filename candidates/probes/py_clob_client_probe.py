"""Probe for py-clob-client (Polymarket official Python SDK).

Fetches one public Polymarket market via the unauthenticated CLOB host
and prints a one-line JSON summary on success. Polymarket's
``get_markets`` endpoint does not require credentials, so this probe is
safe to run in CI without secrets. Exit codes follow
``candidates/probes/README.md``.
"""

from __future__ import annotations

import json
import sys
import traceback
from typing import Any


POLYMARKET_HOST = "https://clob.polymarket.com"


def main() -> int:
    try:
        from py_clob_client.client import ClobClient  # type: ignore[import-not-found]
    except ImportError as exc:
        sys.stderr.write(f"py_clob_client import failed: {exc}\n")
        return 1

    try:
        client = ClobClient(host=POLYMARKET_HOST)
        # ``get_markets`` is paginated; we just want the first page's first
        # market. py-clob-client returns a dict like ``{"data": [...], ...}``.
        response: Any = client.get_markets()
    except Exception as exc:
        sys.stderr.write(f"py_clob_client get_markets failed: {exc}\n")
        traceback.print_exc(file=sys.stderr)
        return 1

    markets: list[Any] = []
    if isinstance(response, dict):
        markets = response.get("data") or response.get("markets") or []
    elif isinstance(response, list):
        markets = response

    if not markets:
        sys.stderr.write("py_clob_client returned no markets\n")
        return 1

    first = markets[0]
    market_id: str | None = None
    question: str | None = None
    if isinstance(first, dict):
        market_id = (
            first.get("condition_id")
            or first.get("market_id")
            or first.get("id")
        )
        question = first.get("question") or first.get("title")

    summary = {
        "ok": True,
        "tool": "py-clob-client",
        "host": POLYMARKET_HOST,
        "market_count": len(markets),
        "market_id": market_id,
        "question": question,
    }
    print(json.dumps(summary))
    return 0


if __name__ == "__main__":
    sys.exit(main())
