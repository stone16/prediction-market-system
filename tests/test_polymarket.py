"""Tests for the Polymarket connector (CP04 + Phase 3A).

Covers the acceptance criteria:
- PolymarketConnector implements all ConnectorProtocol methods
- get_active_markets returns Market objects with all fields populated
- get_orderbook returns OrderBook with bid/ask PriceLevel lists
- stream_prices polls get_orderbook and yields PriceUpdate objects (Phase 3A)
- get_historical_prices fetches /prices-history and returns PriceUpdates (Phase 3A)
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
from pms.models import Market, OrderBook, PriceLevel, PriceUpdate
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
# Phase 3A: stream_prices polling fallback over get_orderbook
# ---------------------------------------------------------------------------


def _make_book_payload(
    bid_price: str, bid_size: str, ask_price: str, ask_size: str
) -> dict[str, Any]:
    return {
        "bids": [{"price": bid_price, "size": bid_size}],
        "asks": [{"price": ask_price, "size": ask_size}],
        "timestamp": "1759900000000",
    }


async def test_stream_prices_yields_price_updates_from_book_polls() -> None:
    """Polling stream calls get_orderbook for each market each iteration."""
    requests: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/book"
        token = request.url.params.get("token_id") or ""
        requests.append(token)
        return httpx.Response(
            200, json=_make_book_payload("0.40", "10", "0.60", "5")
        )

    transport = httpx.MockTransport(handler)
    client = httpx.AsyncClient(transport=transport)
    conn = PolymarketConnector(http_client=client)

    # Bound the iterator with max_iterations so the test terminates.
    iterator = conn.stream_prices(
        ["token-a", "token-b"],
        poll_interval_seconds=0.0,
        max_iterations=2,
    )
    updates: list[PriceUpdate] = []
    async for update in iterator:
        updates.append(update)

    await conn.close()

    # Two markets * two iterations = four updates.
    assert len(updates) == 4
    # Each update preserves bid/ask and computes a mid-price.
    for u in updates:
        assert u.platform == "polymarket"
        assert u.bid == Decimal("0.40")
        assert u.ask == Decimal("0.60")
        assert u.last == Decimal("0.50")  # mid of 0.40/0.60
    # Both markets were polled in both iterations.
    assert requests == ["token-a", "token-b", "token-a", "token-b"]


async def test_stream_prices_skips_failing_markets_but_continues() -> None:
    """One bad book must not silence the rest of the stream."""

    def handler(request: httpx.Request) -> httpx.Response:
        token = request.url.params.get("token_id") or ""
        if token == "broken":
            return httpx.Response(500, json={"error": "boom"})
        return httpx.Response(
            200, json=_make_book_payload("0.45", "10", "0.55", "5")
        )

    transport = httpx.MockTransport(handler)
    client = httpx.AsyncClient(transport=transport)
    conn = PolymarketConnector(http_client=client)

    iterator = conn.stream_prices(
        ["broken", "token-good"],
        poll_interval_seconds=0.0,
        max_iterations=1,
    )
    updates: list[PriceUpdate] = []
    async for update in iterator:
        updates.append(update)

    await conn.close()

    # Only the working market produces an update.
    assert len(updates) == 1
    assert updates[0].market_id == "token-good"


async def test_stream_prices_handles_empty_book_sides() -> None:
    """A book with only bids (or only asks) yields a PriceUpdate where
    ``last`` falls back to whichever side has data."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "bids": [{"price": "0.30", "size": "5"}],
                "asks": [],  # empty ask side
                "timestamp": "1759900000000",
            },
        )

    transport = httpx.MockTransport(handler)
    client = httpx.AsyncClient(transport=transport)
    conn = PolymarketConnector(http_client=client)

    iterator = conn.stream_prices(
        ["token-1"], poll_interval_seconds=0.0, max_iterations=1
    )
    updates = [u async for u in iterator]
    await conn.close()

    assert len(updates) == 1
    update = updates[0]
    assert update.bid == Decimal("0.30")
    assert update.ask == Decimal("0")
    # When only one side has data, ``last`` falls back to it.
    assert update.last == Decimal("0.30")


# ---------------------------------------------------------------------------
# Phase 3A: get_historical_prices over /prices-history
# ---------------------------------------------------------------------------


async def test_get_historical_prices_returns_price_update_series() -> None:
    """The /prices-history response must be parsed into a list of PriceUpdates."""

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/prices-history"
        assert request.url.params.get("market") == "token-x"
        # startTs is converted from datetime → unix seconds.
        assert request.url.params.get("startTs") == "1735689600"  # 2025-01-01 UTC
        return httpx.Response(
            200,
            json={
                "history": [
                    {"t": 1735689600, "p": 0.42},
                    {"t": 1735693200, "p": 0.45},
                    {"t": 1735696800, "p": 0.50},
                ]
            },
        )

    transport = httpx.MockTransport(handler)
    client = httpx.AsyncClient(transport=transport)
    conn = PolymarketConnector(http_client=client)

    updates = await conn.get_historical_prices(
        "token-x", datetime(2025, 1, 1, tzinfo=timezone.utc)
    )
    await conn.close()

    assert len(updates) == 3
    assert all(u.platform == "polymarket" for u in updates)
    assert all(u.market_id == "token-x" for u in updates)
    # Single-price endpoint → bid == ask == last.
    assert updates[0].bid == updates[0].ask == updates[0].last == Decimal("0.42")
    assert updates[1].last == Decimal("0.45")
    assert updates[2].last == Decimal("0.50")
    # Timestamps come back as UTC datetimes.
    assert updates[0].timestamp.tzinfo == timezone.utc


async def test_get_historical_prices_normalizes_naive_datetime_to_utc() -> None:
    """A naive ``since`` is interpreted as UTC, not local time."""

    def handler(request: httpx.Request) -> httpx.Response:
        # 2025-01-01 00:00:00 UTC is unix 1735689600 — naive input must
        # produce the same value, not the local-time conversion.
        assert request.url.params.get("startTs") == "1735689600"
        return httpx.Response(200, json={"history": []})

    transport = httpx.MockTransport(handler)
    client = httpx.AsyncClient(transport=transport)
    conn = PolymarketConnector(http_client=client)

    await conn.get_historical_prices(
        "token-x", datetime(2025, 1, 1)  # naive — no tzinfo
    )
    await conn.close()


async def test_get_historical_prices_skips_malformed_ticks() -> None:
    """Ticks missing ``t`` or ``p``, or with non-numeric values, are skipped."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "history": [
                    {"t": 1735689600, "p": 0.42},
                    {"t": None, "p": 0.45},  # missing t
                    {"t": 1735693200},  # missing p
                    "garbage",  # non-dict tick
                    {"t": "not-a-number", "p": 0.50},  # bad t
                    {"t": 1735696800, "p": 0.50},
                ]
            },
        )

    transport = httpx.MockTransport(handler)
    client = httpx.AsyncClient(transport=transport)
    conn = PolymarketConnector(http_client=client)

    updates = await conn.get_historical_prices(
        "token-x", datetime(2025, 1, 1, tzinfo=timezone.utc)
    )
    await conn.close()

    assert len(updates) == 2
    assert [u.last for u in updates] == [Decimal("0.42"), Decimal("0.50")]


async def test_get_historical_prices_returns_empty_for_empty_history() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"history": []})

    transport = httpx.MockTransport(handler)
    client = httpx.AsyncClient(transport=transport)
    conn = PolymarketConnector(http_client=client)

    updates = await conn.get_historical_prices(
        "token-x", datetime(2025, 1, 1, tzinfo=timezone.utc)
    )
    await conn.close()

    assert updates == []


async def test_get_historical_prices_raises_on_non_dict_root() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=["this", "is", "wrong"])

    transport = httpx.MockTransport(handler)
    client = httpx.AsyncClient(transport=transport)
    conn = PolymarketConnector(http_client=client)

    with pytest.raises(ValueError, match="non-dict payload"):
        await conn.get_historical_prices(
            "token-x", datetime(2025, 1, 1, tzinfo=timezone.utc)
        )
    await conn.close()


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
