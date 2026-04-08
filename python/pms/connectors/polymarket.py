"""Polymarket CLOB + Gamma API connector.

Implements :class:`pms.protocols.ConnectorProtocol` against the public
Polymarket APIs:

- Gamma API (``gamma-api.polymarket.com``) for market metadata
- CLOB API  (``clob.polymarket.com``) for order book snapshots

Design notes
------------

* The connector depends on :mod:`httpx` rather than ``py-clob-client``.
  The official client pulls in web3/eth-account dependencies and wires in
  credentialed order signing, neither of which is required for read-only
  market discovery and order book snapshots. Keeping the wire layer behind
  a plain :class:`httpx.AsyncClient` also lets tests inject
  :class:`httpx.MockTransport` for deterministic, offline fixtures.

* ``stream_prices`` and ``get_historical_prices`` raise
  :class:`NotImplementedError` in v1. WebSocket streaming and historical
  price APIs are deferred to a later checkpoint per the approved spec.

* The full Gamma API response dict for each market is preserved verbatim
  on :attr:`pms.models.Market.raw` so downstream code can recover any
  platform-specific field without re-fetching.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from datetime import datetime
from decimal import Decimal
from typing import Any, cast

import httpx

from pms.models import Market, OrderBook, Outcome, PriceLevel, PriceUpdate


class PolymarketConnector:
    """Polymarket adapter implementing :class:`ConnectorProtocol`."""

    platform: str = "polymarket"

    GAMMA_BASE: str = "https://gamma-api.polymarket.com"
    CLOB_BASE: str = "https://clob.polymarket.com"

    def __init__(
        self,
        http_client: httpx.AsyncClient | None = None,
        gamma_base: str | None = None,
        clob_base: str | None = None,
    ) -> None:
        """Create a connector.

        Parameters
        ----------
        http_client:
            Optional pre-configured :class:`httpx.AsyncClient`. When
            supplied, the connector does **not** own its lifecycle and
            :meth:`close` will not aclose it. Tests inject a client backed
            by :class:`httpx.MockTransport` here.
        gamma_base:
            Override the Gamma API base URL. Defaults to the public host.
        clob_base:
            Override the CLOB API base URL. Defaults to the public host.
        """
        self._http: httpx.AsyncClient = http_client or httpx.AsyncClient(
            timeout=10.0
        )
        self._owns_http: bool = http_client is None
        self._gamma_base: str = gamma_base or self.GAMMA_BASE
        self._clob_base: str = clob_base or self.CLOB_BASE

    async def close(self) -> None:
        """Release the underlying HTTP client if it was created by us."""
        if self._owns_http:
            await self._http.aclose()

    # ------------------------------------------------------------------
    # ConnectorProtocol — market discovery
    # ------------------------------------------------------------------

    async def get_active_markets(self) -> list[Market]:
        """Return all currently tradable markets from the Gamma API."""
        resp = await self._http.get(
            f"{self._gamma_base}/markets",
            params={"active": "true", "closed": "false"},
        )
        resp.raise_for_status()
        data = resp.json()
        if not isinstance(data, list):
            raise ValueError(
                "Polymarket Gamma /markets endpoint returned a non-list "
                f"payload: {type(data).__name__}"
            )
        return [self._normalize_market(cast(dict[str, Any], raw)) for raw in data]

    # ------------------------------------------------------------------
    # ConnectorProtocol — order book
    # ------------------------------------------------------------------

    async def get_orderbook(self, market_id: str) -> OrderBook:
        """Return a snapshot of the order book for a CLOB token id.

        ``market_id`` here is the Polymarket CLOB ``token_id`` — Polymarket
        identifies tradable outcomes by their ERC-1155 token id rather than
        by a market-level id. Callers should pass the token id returned in
        :attr:`Outcome.outcome_id` from :meth:`get_active_markets`.
        """
        resp = await self._http.get(
            f"{self._clob_base}/book",
            params={"token_id": market_id},
        )
        resp.raise_for_status()
        data = resp.json()
        if not isinstance(data, dict):
            raise ValueError(
                "Polymarket CLOB /book endpoint returned a non-dict "
                f"payload: {type(data).__name__}"
            )
        return self._normalize_orderbook(market_id, cast(dict[str, Any], data))

    # ------------------------------------------------------------------
    # ConnectorProtocol — streaming / history (deferred to future CP)
    # ------------------------------------------------------------------

    def stream_prices(
        self, market_ids: list[str]
    ) -> AsyncIterator[PriceUpdate]:
        """Deferred in v1 — raises :class:`NotImplementedError`.

        Implementation via Polymarket's websocket gateway is tracked as a
        follow-up checkpoint.
        """
        raise NotImplementedError(
            "PolymarketConnector.stream_prices is not implemented in v1. "
            "WebSocket streaming is deferred to a future checkpoint."
        )

    async def get_historical_prices(
        self, market_id: str, since: datetime
    ) -> list[PriceUpdate]:
        """Deferred in v1 — raises :class:`NotImplementedError`."""
        raise NotImplementedError(
            "PolymarketConnector.get_historical_prices is not implemented "
            "in v1. Historical price retrieval is deferred to a future "
            "checkpoint."
        )

    # ------------------------------------------------------------------
    # Normalization helpers
    # ------------------------------------------------------------------

    def _normalize_market(self, raw: dict[str, Any]) -> Market:
        """Convert a Gamma API market dict to a normalized :class:`Market`."""
        market_id = str(raw.get("id") or raw.get("conditionId") or "")
        title = str(raw.get("question", ""))
        description = str(raw.get("description", ""))
        category = str(raw.get("category", ""))
        slug = raw.get("slug")
        url = (
            f"https://polymarket.com/event/{slug}"
            if isinstance(slug, str) and slug
            else ""
        )
        status = "active" if raw.get("active") else "inactive"
        volume = self._to_decimal(raw.get("volume"))
        end_date = self._parse_date(raw.get("endDate"))
        outcomes = self._parse_outcomes(raw)

        return Market(
            platform=self.platform,
            market_id=market_id,
            title=title,
            description=description,
            outcomes=outcomes,
            volume=volume,
            end_date=end_date,
            category=category,
            url=url,
            status=status,
            raw=raw,
        )

    def _parse_outcomes(self, raw: dict[str, Any]) -> list[Outcome]:
        """Parse Polymarket's JSON-string-encoded outcome arrays.

        Gamma returns ``outcomes``, ``outcomePrices``, and ``clobTokenIds``
        as JSON-encoded strings of parallel arrays. They may also be
        returned as already-decoded lists in some edge cases.
        """
        outcomes_raw = raw.get("outcomes", "[]")
        prices_raw = raw.get("outcomePrices", "[]")
        token_ids_raw = raw.get("clobTokenIds", "[]")

        outcomes_list = self._parse_json_array(outcomes_raw)
        prices_list = self._parse_json_array(prices_raw)
        token_ids_list = self._parse_json_array(token_ids_raw)

        result: list[Outcome] = []
        for i, name in enumerate(outcomes_list):
            outcome_id = (
                str(token_ids_list[i])
                if i < len(token_ids_list)
                else f"outcome-{i}"
            )
            price_value = prices_list[i] if i < len(prices_list) else "0"
            result.append(
                Outcome(
                    outcome_id=outcome_id,
                    title=str(name),
                    price=self._to_decimal(price_value),
                )
            )
        return result

    @staticmethod
    def _parse_json_array(value: Any) -> list[Any]:
        """Decode a Polymarket JSON-encoded list, tolerating already-decoded lists."""
        if isinstance(value, list):
            return value
        if isinstance(value, str):
            if not value:
                return []
            try:
                decoded = json.loads(value)
            except json.JSONDecodeError:
                return []
            if isinstance(decoded, list):
                return decoded
            return []
        return []

    @staticmethod
    def _parse_date(raw_date: Any) -> datetime | None:
        """Parse an ISO-8601 date. ``Z`` suffix is normalized to ``+00:00``."""
        if not raw_date or not isinstance(raw_date, str):
            return None
        try:
            return datetime.fromisoformat(raw_date.replace("Z", "+00:00"))
        except ValueError:
            return None

    @staticmethod
    def _to_decimal(value: Any) -> Decimal:
        """Coerce a Gamma numeric field (string or number) to :class:`Decimal`."""
        if value is None:
            return Decimal("0")
        if isinstance(value, Decimal):
            return value
        try:
            return Decimal(str(value))
        except (ArithmeticError, ValueError):
            return Decimal("0")

    def _normalize_orderbook(
        self, market_id: str, raw: dict[str, Any]
    ) -> OrderBook:
        """Convert a CLOB /book response dict to a normalized :class:`OrderBook`."""
        bids_raw = raw.get("bids", []) or []
        asks_raw = raw.get("asks", []) or []

        bids = [self._parse_level(level) for level in bids_raw]
        asks = [self._parse_level(level) for level in asks_raw]

        timestamp = self._parse_clob_timestamp(raw.get("timestamp"))

        return OrderBook(
            platform=self.platform,
            market_id=market_id,
            bids=bids,
            asks=asks,
            timestamp=timestamp,
        )

    @staticmethod
    def _parse_level(level: Any) -> PriceLevel:
        if not isinstance(level, dict):
            raise ValueError(f"Invalid CLOB book level (not a dict): {level!r}")
        return PriceLevel(
            price=PolymarketConnector._to_decimal(level.get("price")),
            size=PolymarketConnector._to_decimal(level.get("size")),
        )

    @staticmethod
    def _parse_clob_timestamp(value: Any) -> datetime:
        """Parse a CLOB ``timestamp`` field (milliseconds since epoch as str).

        Falls back to ``datetime.now(UTC)`` when the field is missing or
        unparseable — the CLOB API is not guaranteed to return one.
        """
        from datetime import timezone

        if value is not None:
            try:
                ms = int(value)
                return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)
            except (TypeError, ValueError):
                pass
        return datetime.now(tz=timezone.utc)
