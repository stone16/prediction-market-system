"""Probe for kalshi-python-sync (official Kalshi blocking SDK).

Kalshi's authenticated REST endpoints all require an API key ID +
RSA-signed request headers. This probe expects the credentials to be
exposed via environment variables and exits 2 if either is missing so
the harness reports "missing credentials" instead of a generic failure.

Required env vars:
* ``KALSHI_API_KEY_ID``     — UUID of the trading account's API key
* ``KALSHI_PRIVATE_KEY_PATH``— filesystem path to the PEM-encoded private key
* ``KALSHI_API_HOST``       — optional; defaults to the demo endpoint so
                              CI does not hit production by accident.

Exit codes follow ``candidates/probes/README.md``.
"""

from __future__ import annotations

import json
import os
import sys
import traceback
from pathlib import Path
from typing import Any


DEFAULT_HOST = "https://demo-api.kalshi.co/trade-api/v2"


def _missing_credentials() -> str | None:
    if not os.environ.get("KALSHI_API_KEY_ID"):
        return "KALSHI_API_KEY_ID env var is unset"
    key_path = os.environ.get("KALSHI_PRIVATE_KEY_PATH")
    if not key_path:
        return "KALSHI_PRIVATE_KEY_PATH env var is unset"
    if not Path(key_path).is_file():
        return f"KALSHI_PRIVATE_KEY_PATH points at non-existent file: {key_path}"
    return None


def main() -> int:
    cred_error = _missing_credentials()
    if cred_error is not None:
        sys.stderr.write(f"missing credentials: {cred_error}\n")
        return 2

    try:
        # The official package name on PyPI is ``kalshi-python-sync`` but
        # the import path is ``kalshi_python_sync``. We do a defensive try
        # against both common module shapes the package has shipped with.
        try:
            import kalshi_python_sync as kalshi  # type: ignore[import-not-found]
        except ImportError:
            import kalshi_python as kalshi  # type: ignore[import-not-found, no-redef]
    except ImportError as exc:
        sys.stderr.write(f"kalshi-python-sync import failed: {exc}\n")
        return 1

    host = os.environ.get("KALSHI_API_HOST", DEFAULT_HOST)
    try:
        # The exact constructor signature differs across SDK versions.
        # We use a getattr-driven path so the probe at least exits 1
        # (not crash) if the API surface has changed under us.
        ApiClient = getattr(kalshi, "ApiClient", None) or getattr(
            kalshi, "Client", None
        )
        if ApiClient is None:
            sys.stderr.write("kalshi-python-sync exposes neither ApiClient nor Client\n")
            return 1
        client = ApiClient(host=host)
        markets_method = getattr(client, "get_markets", None) or getattr(
            client, "GetMarkets", None
        )
        if markets_method is None:
            sys.stderr.write(
                "kalshi-python-sync client has no get_markets/GetMarkets method\n"
            )
            return 1
        response: Any = markets_method()
    except Exception as exc:
        sys.stderr.write(f"kalshi-python-sync fetch failed: {exc}\n")
        traceback.print_exc(file=sys.stderr)
        return 1

    # Normalize the response shape into a count + sample id.
    markets: list[Any]
    if isinstance(response, dict):
        markets = response.get("markets") or response.get("data") or []
    elif isinstance(response, list):
        markets = response
    else:
        markets = list(getattr(response, "markets", []) or [])

    if not markets:
        sys.stderr.write("kalshi-python-sync returned no markets\n")
        return 1

    first = markets[0]
    ticker: str | None = None
    if isinstance(first, dict):
        ticker = first.get("ticker") or first.get("market_ticker")
    else:
        ticker = getattr(first, "ticker", None)

    summary = {
        "ok": True,
        "tool": "kalshi-python-sync",
        "host": host,
        "market_count": len(markets),
        "sample_ticker": ticker,
    }
    print(json.dumps(summary))
    return 0


if __name__ == "__main__":
    sys.exit(main())
