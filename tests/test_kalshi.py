"""Tests for the Kalshi connector (CP05).

Covers the acceptance criteria:
- KalshiConnector implements all ConnectorProtocol methods
- get_active_markets returns Market objects with all fields populated
- get_orderbook returns OrderBook with bid/ask PriceLevel lists
- stream_prices raises NotImplementedError (v1 limitation)
- get_historical_prices raises NotImplementedError
- raw field preserves the original Kalshi API response dict
- Kalshi cent prices are normalized to Decimal dollars (0-99 cents -> 0.00-0.99)
- Binary yes/no outcomes are parsed correctly
- Tests use recorded fixtures in tests/fixtures/kalshi/ — no live HTTP
- Auth headers stub returns empty dict when credentials are absent

All HTTP is intercepted via httpx.MockTransport so no real network calls
ever leave the test process.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import httpx
import pytest

from pms.connectors.kalshi import KalshiConnector
from pms.models import Market, OrderBook, PriceLevel
from pms.protocols import ConnectorProtocol

# ---------------------------------------------------------------------------
# Fixture paths
# ---------------------------------------------------------------------------

FIXTURES_DIR = Path(__file__).parent / "fixtures" / "kalshi"
MARKETS_FIXTURE = FIXTURES_DIR / "markets.json"
ORDERBOOK_FIXTURE = FIXTURES_DIR / "orderbook.json"


def _load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# Mock transport helpers
# ---------------------------------------------------------------------------


def _make_connector_with_markets(payload: Any) -> KalshiConnector:
    """Return a KalshiConnector whose HTTP client serves ``payload`` for
    GET /trade-api/v2/markets."""

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path.endswith("/markets"), (
            f"unexpected path {request.url.path}"
        )
        return httpx.Response(200, json=payload)

    transport = httpx.MockTransport(handler)
    client = httpx.AsyncClient(
        transport=transport,
        base_url="https://api.elections.kalshi.com/trade-api/v2",
    )
    return KalshiConnector(http_client=client)


def _make_connector_with_orderbook(payload: Any) -> KalshiConnector:
    """Return a KalshiConnector whose HTTP client serves ``payload`` for
    GET /trade-api/v2/markets/{ticker}/orderbook."""

    def handler(request: httpx.Request) -> httpx.Response:
        assert "/orderbook" in request.url.path, (
            f"unexpected path {request.url.path}"
        )
        return httpx.Response(200, json=payload)

    transport = httpx.MockTransport(handler)
    client = httpx.AsyncClient(
        transport=transport,
        base_url="https://api.elections.kalshi.com/trade-api/v2",
    )
    return KalshiConnector(http_client=client)


# ---------------------------------------------------------------------------
# Structural / protocol compatibility
# ---------------------------------------------------------------------------


def test_kalshi_connector_has_platform_attr() -> None:
    conn = KalshiConnector()
    assert conn.platform == "kalshi"


def test_kalshi_connector_is_structural_connector_protocol() -> None:
    """KalshiConnector must satisfy ConnectorProtocol at runtime."""
    conn = KalshiConnector()
    assert isinstance(conn, ConnectorProtocol)


# ---------------------------------------------------------------------------
# get_active_markets
# ---------------------------------------------------------------------------


async def test_get_active_markets_returns_list_of_markets() -> None:
    payload = _load_json(MARKETS_FIXTURE)
    conn = _make_connector_with_markets(payload)
    try:
        markets = await conn.get_active_markets()
    finally:
        await conn.close()

    assert isinstance(markets, list)
    assert len(markets) == 2
    for m in markets:
        assert isinstance(m, Market)


async def test_get_active_markets_populates_all_fields() -> None:
    payload = _load_json(MARKETS_FIXTURE)
    conn = _make_connector_with_markets(payload)
    try:
        markets = await conn.get_active_markets()
    finally:
        await conn.close()

    first = markets[0]
    assert first.platform == "kalshi"
    assert first.market_id == "SAMPLE-EVENT-X-2026"
    assert first.title == (
        "Will a sanitized sample event X resolve YES by 2026-07-31?"
    )
    assert "sanitized sample" in first.description.lower()
    assert first.volume == Decimal("12345")
    assert first.end_date == datetime(
        2026, 7, 31, 12, 0, 0, tzinfo=timezone.utc
    )
    assert first.category == "Politics"
    assert first.status == "active"
    assert isinstance(first.url, str)


async def test_get_active_markets_prices_converted_from_cents_to_dollars() -> (
    None
):
    """Kalshi returns prices in 0-99 cents; connector must normalize to
    Decimal dollars (0.00-0.99)."""
    payload = _load_json(MARKETS_FIXTURE)
    conn = _make_connector_with_markets(payload)
    try:
        markets = await conn.get_active_markets()
    finally:
        await conn.close()

    first = markets[0]
    assert len(first.outcomes) == 2
    yes_outcome = first.outcomes[0]
    no_outcome = first.outcomes[1]

    # Yes outcome: use yes_bid (52 cents) -> 0.52 dollars
    assert yes_outcome.title == "Yes"
    assert isinstance(yes_outcome.price, Decimal)
    assert yes_outcome.price == Decimal("0.52")

    # No outcome: use no_bid (46 cents) -> 0.46 dollars
    assert no_outcome.title == "No"
    assert isinstance(no_outcome.price, Decimal)
    assert no_outcome.price == Decimal("0.46")

    # Second market: yes_bid=10 -> 0.10, no_bid=88 -> 0.88
    second = markets[1]
    assert second.outcomes[0].price == Decimal("0.10")
    assert second.outcomes[1].price == Decimal("0.88")


async def test_get_active_markets_binary_outcomes_use_sub_titles() -> None:
    """Binary markets: yes_sub_title/no_sub_title must map to Outcome.title."""
    payload = _load_json(MARKETS_FIXTURE)
    conn = _make_connector_with_markets(payload)
    try:
        markets = await conn.get_active_markets()
    finally:
        await conn.close()

    first = markets[0]
    assert len(first.outcomes) == 2
    assert first.outcomes[0].title == "Yes"
    assert first.outcomes[1].title == "No"
    # Outcome ids must be ticker-scoped so downstream consumers can
    # distinguish yes/no legs within the same market.
    assert first.outcomes[0].outcome_id == "SAMPLE-EVENT-X-2026-YES"
    assert first.outcomes[1].outcome_id == "SAMPLE-EVENT-X-2026-NO"


async def test_get_active_markets_preserves_raw_dict() -> None:
    payload = _load_json(MARKETS_FIXTURE)
    conn = _make_connector_with_markets(payload)
    try:
        markets = await conn.get_active_markets()
    finally:
        await conn.close()

    first_raw_in = payload["markets"][0]
    first_raw_out = markets[0].raw
    assert first_raw_out == first_raw_in
    # Nested Kalshi-specific fields still reachable
    assert first_raw_out["event_ticker"] == "SAMPLE-EVENT-X"
    assert first_raw_out["yes_bid"] == 52
    assert first_raw_out["market_type"] == "binary"


async def test_get_active_markets_end_date_is_timezone_aware() -> None:
    payload = _load_json(MARKETS_FIXTURE)
    conn = _make_connector_with_markets(payload)
    try:
        markets = await conn.get_active_markets()
    finally:
        await conn.close()

    for m in markets:
        assert m.end_date is not None
        assert m.end_date.tzinfo is not None
        assert m.end_date.utcoffset() is not None


async def test_get_active_markets_sends_status_open_query_param() -> None:
    """The connector must request only open/active markets from Kalshi."""
    payload = _load_json(MARKETS_FIXTURE)

    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(200, json=payload)

    transport = httpx.MockTransport(handler)
    client = httpx.AsyncClient(
        transport=transport,
        base_url="https://api.elections.kalshi.com/trade-api/v2",
    )
    conn = KalshiConnector(http_client=client)
    try:
        await conn.get_active_markets()
    finally:
        await conn.close()

    assert len(captured) == 1
    req = captured[0]
    assert req.url.path.endswith("/markets")
    # Kalshi uses status=open for tradable markets
    assert req.url.params.get("status") == "open"


# ---------------------------------------------------------------------------
# get_orderbook
# ---------------------------------------------------------------------------


async def test_get_orderbook_returns_orderbook_with_price_levels() -> None:
    payload = _load_json(ORDERBOOK_FIXTURE)
    conn = _make_connector_with_orderbook(payload)
    try:
        ob = await conn.get_orderbook("SAMPLE-EVENT-X-2026")
    finally:
        await conn.close()

    assert isinstance(ob, OrderBook)
    assert ob.platform == "kalshi"
    assert ob.market_id == "SAMPLE-EVENT-X-2026"
    assert ob.timestamp is not None
    assert ob.timestamp.tzinfo is not None


async def test_get_orderbook_yes_side_maps_to_bids_in_decimal_dollars() -> None:
    """Kalshi yes side -> bids. Prices converted from cents to Decimal dollars."""
    payload = _load_json(ORDERBOOK_FIXTURE)
    conn = _make_connector_with_orderbook(payload)
    try:
        ob = await conn.get_orderbook("SAMPLE-EVENT-X-2026")
    finally:
        await conn.close()

    assert len(ob.bids) == 3
    for b in ob.bids:
        assert isinstance(b, PriceLevel)
        assert isinstance(b.price, Decimal)
        assert isinstance(b.size, Decimal)

    # First yes level: [52 cents, size 100] -> (0.52, 100)
    assert ob.bids[0].price == Decimal("0.52")
    assert ob.bids[0].size == Decimal("100")
    assert ob.bids[1].price == Decimal("0.51")
    assert ob.bids[1].size == Decimal("250")
    assert ob.bids[2].price == Decimal("0.50")
    assert ob.bids[2].size == Decimal("500")


async def test_get_orderbook_no_side_maps_to_yes_asks_via_price_transformation() -> None:
    """Kalshi NO bids transform into YES asks via ``price = 1 - no_price``.

    Review-loop fix f5: Kalshi's orderbook ``no`` side is composed of bids
    on the NO leg of a binary contract, not asks on the YES leg. A NO bid
    at 48 cents implies a willingness to *sell* the YES leg at
    ``1 - 0.48 = 0.52`` (because YES + NO must equal 1.0 on settlement).
    Copying NO directly into ``asks`` (the previous v1 behaviour) put a
    bid into the ask field, which downstream strategies would treat as a
    crossed book or, worse, as free arbitrage.

    Sizes are unchanged by the transformation; only price is mirrored
    around 1. The original Kalshi response remains accessible via
    :attr:`OrderBook` consumers that re-fetch from the platform.
    """
    payload = _load_json(ORDERBOOK_FIXTURE)
    conn = _make_connector_with_orderbook(payload)
    try:
        ob = await conn.get_orderbook("SAMPLE-EVENT-X-2026")
    finally:
        await conn.close()

    assert len(ob.asks) == 3
    for a in ob.asks:
        assert isinstance(a, PriceLevel)
        assert isinstance(a.price, Decimal)
        assert isinstance(a.size, Decimal)

    # NO levels in the fixture (cents): [[48, 80], [47, 200], [46, 150]].
    # After ``ask = 1 - no_bid``: [(0.52, 80), (0.53, 200), (0.54, 150)].
    assert ob.asks[0].price == Decimal("0.52")
    assert ob.asks[0].size == Decimal("80")
    assert ob.asks[1].price == Decimal("0.53")
    assert ob.asks[1].size == Decimal("200")
    assert ob.asks[2].price == Decimal("0.54")
    assert ob.asks[2].size == Decimal("150")


async def test_get_orderbook_sends_request_to_ticker_orderbook_path() -> None:
    payload = _load_json(ORDERBOOK_FIXTURE)

    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(200, json=payload)

    transport = httpx.MockTransport(handler)
    client = httpx.AsyncClient(
        transport=transport,
        base_url="https://api.elections.kalshi.com/trade-api/v2",
    )
    conn = KalshiConnector(http_client=client)
    try:
        await conn.get_orderbook("SAMPLE-EVENT-X-2026")
    finally:
        await conn.close()

    assert len(captured) == 1
    req = captured[0]
    assert req.url.path.endswith("/markets/SAMPLE-EVENT-X-2026/orderbook")


async def test_get_orderbook_handles_missing_side() -> None:
    """Kalshi may return null for an empty side — connector must degrade
    gracefully to an empty list rather than crash."""
    conn = _make_connector_with_orderbook(
        {"orderbook": {"yes": None, "no": [[48, 80]]}}
    )
    try:
        ob = await conn.get_orderbook("SAMPLE-EVENT-X-2026")
    finally:
        await conn.close()

    assert ob.bids == []
    assert len(ob.asks) == 1
    # NO bid at 48 cents → YES ask at 1 - 0.48 = 0.52 (review-loop fix f5).
    assert ob.asks[0].price == Decimal("0.52")
    assert ob.asks[0].size == Decimal("80")


# ---------------------------------------------------------------------------
# stream_prices / get_historical_prices — v1 deferred
# ---------------------------------------------------------------------------


def test_stream_prices_raises_not_implemented() -> None:
    conn = KalshiConnector()
    with pytest.raises(NotImplementedError) as exc_info:
        # stream_prices is a regular (sync) method per ConnectorProtocol.
        conn.stream_prices(["SAMPLE-EVENT-X-2026"])
    message = str(exc_info.value)
    assert "v1" in message.lower()
    assert "stream_prices" in message


async def test_get_historical_prices_raises_not_implemented() -> None:
    conn = KalshiConnector()
    with pytest.raises(NotImplementedError) as exc_info:
        await conn.get_historical_prices(
            "SAMPLE-EVENT-X-2026",
            datetime(2026, 1, 1, tzinfo=timezone.utc),
        )
    assert "get_historical_prices" in str(exc_info.value)


# ---------------------------------------------------------------------------
# Auth signing stub
# ---------------------------------------------------------------------------


def test_sign_request_headers_empty_without_credentials() -> None:
    """Without api_key_id + private_key_pem, auth must no-op to empty dict."""
    conn = KalshiConnector()
    headers = conn._sign_request_headers("GET", "/markets")
    assert headers == {}


def test_sign_request_headers_with_credentials_returns_placeholder() -> None:
    """When credentials are configured, the stub returns a placeholder
    header so the auth code path exists without shipping unfinished RSA
    signing. Full implementation is deferred per CP05 scope note."""
    conn = KalshiConnector(
        api_key_id="fake-key-id-for-tests",
        private_key_pem="-----BEGIN TEST PLACEHOLDER-----",
    )
    headers = conn._sign_request_headers("GET", "/markets")
    assert "KALSHI-ACCESS-KEY" in headers
    assert headers["KALSHI-ACCESS-KEY"] == "fake-key-id-for-tests"


# ---------------------------------------------------------------------------
# close() lifecycle
# ---------------------------------------------------------------------------


async def test_close_closes_owned_http_client() -> None:
    conn = KalshiConnector()
    assert conn._http.is_closed is False
    await conn.close()
    assert conn._http.is_closed is True


async def test_close_does_not_close_injected_client() -> None:
    transport = httpx.MockTransport(
        lambda req: httpx.Response(200, json={"markets": [], "cursor": ""})
    )
    client = httpx.AsyncClient(transport=transport)
    conn = KalshiConnector(http_client=client)
    await conn.close()
    assert client.is_closed is False
    await client.aclose()
