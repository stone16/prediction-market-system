"""Tests for the Polymarket connector (CP04).

Covers the acceptance criteria:
- PolymarketConnector implements all ConnectorProtocol methods
- get_active_markets returns Market objects with all fields populated
- get_orderbook returns OrderBook with bid/ask PriceLevel lists
- stream_prices raises NotImplementedError (v1 limitation)
- get_historical_prices raises NotImplementedError
- raw field preserves the original Gamma API response dict
- Tests use recorded fixtures in tests/fixtures/polymarket/ — no live HTTP

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

from pms.connectors.polymarket import PolymarketConnector
from pms.models import Market, OrderBook, PriceLevel
from pms.protocols import ConnectorProtocol

# ---------------------------------------------------------------------------
# Fixture paths
# ---------------------------------------------------------------------------

FIXTURES_DIR = Path(__file__).parent / "fixtures" / "polymarket"
GAMMA_MARKETS_FIXTURE = FIXTURES_DIR / "gamma_markets.json"
CLOB_ORDERBOOK_FIXTURE = FIXTURES_DIR / "clob_orderbook.json"


def _load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# Mock transport helpers
# ---------------------------------------------------------------------------


def _make_gamma_transport(
    payload: Any, expected_path: str = "/markets"
) -> httpx.MockTransport:
    """Return a MockTransport that serves ``payload`` for any request whose
    path matches ``expected_path``."""

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == expected_path, (
            f"unexpected path {request.url.path}"
        )
        return httpx.Response(200, json=payload)

    return httpx.MockTransport(handler)


def _make_clob_transport(
    payload: Any, expected_path: str = "/book"
) -> httpx.MockTransport:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == expected_path, (
            f"unexpected path {request.url.path}"
        )
        return httpx.Response(200, json=payload)

    return httpx.MockTransport(handler)


def _make_connector_with_gamma(payload: Any) -> PolymarketConnector:
    transport = _make_gamma_transport(payload)
    client = httpx.AsyncClient(
        transport=transport, base_url="https://gamma-api.polymarket.com"
    )
    return PolymarketConnector(http_client=client)


def _make_connector_with_clob(payload: Any) -> PolymarketConnector:
    transport = _make_clob_transport(payload)
    client = httpx.AsyncClient(
        transport=transport, base_url="https://clob.polymarket.com"
    )
    return PolymarketConnector(http_client=client)


# ---------------------------------------------------------------------------
# Structural / protocol compatibility
# ---------------------------------------------------------------------------


def test_polymarket_connector_has_platform_attr() -> None:
    conn = PolymarketConnector()
    assert conn.platform == "polymarket"


def test_polymarket_connector_is_structural_connector_protocol() -> None:
    """PolymarketConnector must satisfy ConnectorProtocol at runtime."""
    conn = PolymarketConnector()
    assert isinstance(conn, ConnectorProtocol)


# ---------------------------------------------------------------------------
# get_active_markets
# ---------------------------------------------------------------------------


async def test_get_active_markets_returns_list_of_markets() -> None:
    payload = _load_json(GAMMA_MARKETS_FIXTURE)
    conn = _make_connector_with_gamma(payload)
    try:
        markets = await conn.get_active_markets()
    finally:
        await conn.close()

    assert isinstance(markets, list)
    assert len(markets) == 2
    for m in markets:
        assert isinstance(m, Market)


async def test_get_active_markets_populates_all_fields() -> None:
    payload = _load_json(GAMMA_MARKETS_FIXTURE)
    conn = _make_connector_with_gamma(payload)
    try:
        markets = await conn.get_active_markets()
    finally:
        await conn.close()

    first = markets[0]
    assert first.platform == "polymarket"
    assert first.market_id == "999001"
    assert (
        first.title
        == "Will a public event X happen before another public event Y?"
    )
    assert first.description.startswith("This is a sanitized sample")
    assert first.volume == Decimal("1425235.734902996")
    assert first.end_date == datetime(2026, 7, 31, 12, 0, 0, tzinfo=timezone.utc)
    assert first.url == "https://polymarket.com/event/sample-market-x-before-y"
    assert first.status == "active"
    # category falls back to "" when not present in gamma response (v1 API has
    # category at the event level, not the market level); connector must not
    # crash and must return a string for structural consistency.
    assert isinstance(first.category, str)


async def test_get_active_markets_preserves_raw_dict() -> None:
    payload = _load_json(GAMMA_MARKETS_FIXTURE)
    conn = _make_connector_with_gamma(payload)
    try:
        markets = await conn.get_active_markets()
    finally:
        await conn.close()

    first_raw_in = payload[0]
    first_raw_out = markets[0].raw
    # Same content preserved losslessly
    assert first_raw_out == first_raw_in
    # Nested keys still reachable
    assert first_raw_out["conditionId"] == first_raw_in["conditionId"]
    assert first_raw_out["clobTokenIds"] == first_raw_in["clobTokenIds"]


async def test_get_active_markets_parses_outcomes_correctly() -> None:
    payload = _load_json(GAMMA_MARKETS_FIXTURE)
    conn = _make_connector_with_gamma(payload)
    try:
        markets = await conn.get_active_markets()
    finally:
        await conn.close()

    first = markets[0]
    assert len(first.outcomes) == 2

    yes_outcome = first.outcomes[0]
    no_outcome = first.outcomes[1]

    # Names and token IDs
    assert yes_outcome.title == "Yes"
    assert no_outcome.title == "No"
    assert (
        yes_outcome.outcome_id
        == "1111111111111111111111111111111111111111111111111111111111111111"
    )
    assert (
        no_outcome.outcome_id
        == "2222222222222222222222222222222222222222222222222222222222222222"
    )

    # Prices must be Decimal
    assert isinstance(yes_outcome.price, Decimal)
    assert isinstance(no_outcome.price, Decimal)
    assert yes_outcome.price == Decimal("0.525")
    assert no_outcome.price == Decimal("0.475")


async def test_get_active_markets_end_date_is_timezone_aware() -> None:
    payload = _load_json(GAMMA_MARKETS_FIXTURE)
    conn = _make_connector_with_gamma(payload)
    try:
        markets = await conn.get_active_markets()
    finally:
        await conn.close()

    for m in markets:
        assert m.end_date is not None
        assert m.end_date.tzinfo is not None
        assert m.end_date.utcoffset() is not None


async def test_get_active_markets_sends_active_query_params() -> None:
    """The connector must request only active+non-closed markets."""
    payload = _load_json(GAMMA_MARKETS_FIXTURE)

    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(200, json=payload)

    transport = httpx.MockTransport(handler)
    client = httpx.AsyncClient(
        transport=transport, base_url="https://gamma-api.polymarket.com"
    )
    conn = PolymarketConnector(http_client=client)
    try:
        await conn.get_active_markets()
    finally:
        await conn.close()

    assert len(captured) == 1
    req = captured[0]
    assert req.url.path == "/markets"
    # Both query params present, values lowercase "true"/"false"
    assert req.url.params.get("active") == "true"
    assert req.url.params.get("closed") == "false"


async def test_get_active_markets_handles_missing_end_date() -> None:
    payload = _load_json(GAMMA_MARKETS_FIXTURE)
    # Remove endDate from one market — connector must tolerate.
    del payload[1]["endDate"]

    conn = _make_connector_with_gamma(payload)
    try:
        markets = await conn.get_active_markets()
    finally:
        await conn.close()

    assert markets[1].end_date is None


# ---------------------------------------------------------------------------
# get_orderbook
# ---------------------------------------------------------------------------


async def test_get_orderbook_returns_orderbook_with_price_levels() -> None:
    payload = _load_json(CLOB_ORDERBOOK_FIXTURE)
    conn = _make_connector_with_clob(payload)
    try:
        ob = await conn.get_orderbook(
            "1111111111111111111111111111111111111111111111111111111111111111"
        )
    finally:
        await conn.close()

    assert isinstance(ob, OrderBook)
    assert ob.platform == "polymarket"
    assert (
        ob.market_id
        == "1111111111111111111111111111111111111111111111111111111111111111"
    )
    assert ob.timestamp is not None
    assert ob.timestamp.tzinfo is not None

    # Bids
    assert len(ob.bids) == 3
    for b in ob.bids:
        assert isinstance(b, PriceLevel)
        assert isinstance(b.price, Decimal)
        assert isinstance(b.size, Decimal)
    assert ob.bids[0].price == Decimal("0.52")
    assert ob.bids[0].size == Decimal("100.0")

    # Asks
    assert len(ob.asks) == 3
    for a in ob.asks:
        assert isinstance(a, PriceLevel)
        assert isinstance(a.price, Decimal)
        assert isinstance(a.size, Decimal)
    assert ob.asks[0].price == Decimal("0.53")
    assert ob.asks[0].size == Decimal("80.0")


async def test_get_orderbook_sends_token_id_query_param() -> None:
    payload = _load_json(CLOB_ORDERBOOK_FIXTURE)

    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(200, json=payload)

    transport = httpx.MockTransport(handler)
    client = httpx.AsyncClient(
        transport=transport, base_url="https://clob.polymarket.com"
    )
    conn = PolymarketConnector(http_client=client)
    try:
        await conn.get_orderbook("token-abc-123")
    finally:
        await conn.close()

    assert len(captured) == 1
    req = captured[0]
    assert req.url.path == "/book"
    assert req.url.params.get("token_id") == "token-abc-123"


# ---------------------------------------------------------------------------
# stream_prices / get_historical_prices — v1 deferred
# ---------------------------------------------------------------------------


def test_stream_prices_raises_not_implemented() -> None:
    conn = PolymarketConnector()
    with pytest.raises(NotImplementedError) as exc_info:
        # Calling stream_prices must raise immediately — it's a sync method
        # per ConnectorProtocol signature.
        conn.stream_prices(["token-1"])
    message = str(exc_info.value)
    assert "v1" in message.lower()
    assert "stream_prices" in message


async def test_get_historical_prices_raises_not_implemented() -> None:
    conn = PolymarketConnector()
    with pytest.raises(NotImplementedError) as exc_info:
        await conn.get_historical_prices(
            "token-1", datetime(2026, 1, 1, tzinfo=timezone.utc)
        )
    assert "get_historical_prices" in str(exc_info.value)


# ---------------------------------------------------------------------------
# close() lifecycle
# ---------------------------------------------------------------------------


async def test_close_closes_owned_http_client() -> None:
    conn = PolymarketConnector()
    # Reach into the private client to verify lifecycle management.
    assert conn._http.is_closed is False
    await conn.close()
    assert conn._http.is_closed is True


async def test_close_does_not_close_injected_client() -> None:
    transport = httpx.MockTransport(lambda req: httpx.Response(200, json=[]))
    client = httpx.AsyncClient(transport=transport)
    conn = PolymarketConnector(http_client=client)
    await conn.close()
    assert client.is_closed is False
    await client.aclose()
