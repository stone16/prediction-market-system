"""Kalshi REST API connector.

Implements :class:`pms.protocols.ConnectorProtocol` against the public
Kalshi ``trade-api/v2`` REST endpoints:

- ``GET /markets`` for market discovery
- ``GET /markets/{ticker}/orderbook`` for order book snapshots

Design notes
------------

* The connector depends on :mod:`httpx` rather than a first-party Kalshi
  SDK. The public discovery and order book endpoints are keyless, so a
  plain :class:`httpx.AsyncClient` is all that is required — and injecting
  :class:`httpx.MockTransport` in tests makes every HTTP call
  deterministic and offline. This mirrors the CP04 Polymarket adapter.

* Kalshi prices are transmitted as **integer cents** in the range 0-99
  (a 100-cent contract pays out \\$1 on YES). The connector normalizes
  every price to :class:`decimal.Decimal` **dollars** (``cents / 100``)
  before placing it into the CP01 :class:`Market`, :class:`Outcome`, and
  :class:`PriceLevel` models. The original integer cent values remain
  available verbatim on :attr:`Market.raw` for anyone who needs them.

* Kalshi's order book has two named sides, ``yes`` and ``no``, not
  ``bids``/``asks``. Both sides are *bids* on opposite legs of a binary
  contract: a NO bid at 48¢ is *not* an offer to sell YES at 48¢ — it
  implies a willingness to sell YES at ``1 - 0.48 = 0.52`` because
  YES + NO settle to exactly \\$1. The connector therefore:

    - maps ``yes`` → :attr:`OrderBook.bids` directly (already YES-side
      bids, sorted high → low),
    - mirrors ``no`` into YES-side asks via ``ask_price = 1 - no_bid``
      (review-loop fix f5). Sizes are unchanged.

  The original ``yes``/``no`` arrays remain accessible to consumers that
  re-fetch from Kalshi directly. This makes :class:`OrderBook` behave
  consistently with the Polymarket connector — both expose YES-leg
  best-bid/best-ask in the same field — at the cost of dropping the
  raw NO-side numbers from the normalized object.

* ``stream_prices`` and ``get_historical_prices`` raise
  :class:`NotImplementedError` in v1 per the approved spec. WebSocket
  streaming and historical trade retrieval are deferred to a later
  checkpoint.

* **Auth signing is intentionally stubbed.** Kalshi signs authenticated
  requests with an RSA private key + API key ID. CP05 only uses public
  endpoints through a mocked transport, so the full signing
  implementation is deferred. :meth:`_sign_request_headers` returns an
  empty dict when credentials are absent and a minimal placeholder
  header dict when they are supplied — enough to keep the code path
  reachable for a future checkpoint without shipping unfinished crypto.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, cast

import httpx

from pms.models import Market, OrderBook, Outcome, PriceLevel, PriceUpdate


class KalshiConnector:
    """Kalshi adapter implementing :class:`ConnectorProtocol`."""

    platform: str = "kalshi"

    BASE_URL: str = "https://api.elections.kalshi.com/trade-api/v2"

    #: Hard cap on the number of pages :meth:`get_active_markets` will
    #: walk through Kalshi's cursor pagination. Acts as a safety brake
    #: against a server-side bug returning a non-terminating cursor.
    #: 20 pages × 100 markets/page = 2000 markets, comfortably above
    #: Kalshi's current open-market count.
    MAX_PAGES: int = 20

    def __init__(
        self,
        http_client: httpx.AsyncClient | None = None,
        base_url: str | None = None,
        api_key_id: str | None = None,
        private_key_pem: str | None = None,
    ) -> None:
        """Create a connector.

        Parameters
        ----------
        http_client:
            Optional pre-configured :class:`httpx.AsyncClient`. When
            supplied, the connector does **not** own its lifecycle and
            :meth:`close` will not ``aclose`` it. Tests inject a client
            backed by :class:`httpx.MockTransport` here.
        base_url:
            Override the Kalshi API base URL. Defaults to
            :attr:`BASE_URL`. Pass the demo host
            ``https://demo-api.kalshi.co/trade-api/v2`` for testnet.
        api_key_id:
            Optional Kalshi API key identifier. Only used when Kalshi
            requires authentication for a given endpoint (none of the
            v1 code paths do). Full RSA signing is deferred — see
            :meth:`_sign_request_headers`.
        private_key_pem:
            Optional PEM-encoded RSA private key string. Paired with
            ``api_key_id`` to enable signed requests. Not exercised by
            v1 tests.
        """
        self._http: httpx.AsyncClient = http_client or httpx.AsyncClient(
            timeout=10.0
        )
        self._owns_http: bool = http_client is None
        self._base_url: str = base_url or self.BASE_URL
        self._api_key_id: str | None = api_key_id
        self._private_key_pem: str | None = private_key_pem

    async def close(self) -> None:
        """Release the underlying HTTP client if it was created by us."""
        if self._owns_http:
            await self._http.aclose()

    # ------------------------------------------------------------------
    # ConnectorProtocol — market discovery
    # ------------------------------------------------------------------

    async def get_active_markets(self) -> list[Market]:
        """Return all currently open Kalshi markets.

        Uses ``status=open`` so the connector only returns tradable
        markets. Walks Kalshi's ``cursor`` field across pages until
        either an empty/missing cursor terminates the walk or the
        :attr:`MAX_PAGES` safety cap is reached (review-loop fix f6
        round 2).
        """
        all_markets: list[Market] = []
        cursor: str | None = None
        for _ in range(self.MAX_PAGES):
            params: dict[str, Any] = {"status": "open", "limit": 100}
            if cursor:
                params["cursor"] = cursor

            headers = self._sign_request_headers("GET", "/markets")
            resp = await self._http.get(
                f"{self._base_url}/markets",
                params=params,
                headers=headers or None,
            )
            resp.raise_for_status()
            data = resp.json()
            if not isinstance(data, dict):
                raise ValueError(
                    "Kalshi /markets endpoint returned a non-dict payload: "
                    f"{type(data).__name__}"
                )

            markets_raw = data.get("markets", [])
            if not isinstance(markets_raw, list):
                raise ValueError(
                    "Kalshi /markets 'markets' field is not a list: "
                    f"{type(markets_raw).__name__}"
                )

            for raw in markets_raw:
                all_markets.append(
                    self._normalize_market(cast(dict[str, Any], raw))
                )

            next_cursor = data.get("cursor")
            if not isinstance(next_cursor, str) or not next_cursor:
                break
            cursor = next_cursor

        return all_markets

    # ------------------------------------------------------------------
    # ConnectorProtocol — order book
    # ------------------------------------------------------------------

    async def get_orderbook(self, market_id: str) -> OrderBook:
        """Return a snapshot of the order book for a Kalshi market ticker.

        ``market_id`` here is the Kalshi market **ticker**
        (e.g. ``"PRES-2028-DEM"``), which is the primary identifier
        Kalshi uses for markets in its REST API. The Kalshi ticker
        equals the normalized :attr:`Market.market_id` returned by
        :meth:`get_active_markets`, so callers can pass that field
        through directly — unlike the Polymarket connector.

        See :meth:`pms.protocols.connector.ConnectorProtocol.get_orderbook`
        for the per-platform parameter contract — review-loop fix f3
        documents the cross-connector mismatch.
        """
        headers = self._sign_request_headers(
            "GET", f"/markets/{market_id}/orderbook"
        )
        resp = await self._http.get(
            f"{self._base_url}/markets/{market_id}/orderbook",
            headers=headers or None,
        )
        resp.raise_for_status()
        data = resp.json()
        if not isinstance(data, dict):
            raise ValueError(
                "Kalshi orderbook endpoint returned a non-dict payload: "
                f"{type(data).__name__}"
            )
        return self._normalize_orderbook(market_id, cast(dict[str, Any], data))

    # ------------------------------------------------------------------
    # ConnectorProtocol — streaming / history (deferred to future CP)
    # ------------------------------------------------------------------

    def stream_prices(
        self, market_ids: list[str]
    ) -> AsyncIterator[PriceUpdate]:
        """Deferred in v1 — raises :class:`NotImplementedError`.

        Implementation via Kalshi's websocket gateway is tracked as a
        follow-up checkpoint.
        """
        raise NotImplementedError(
            "KalshiConnector.stream_prices is not implemented in v1. "
            "WebSocket streaming is deferred to a future checkpoint."
        )

    async def get_historical_prices(
        self, market_id: str, since: datetime
    ) -> list[PriceUpdate]:
        """Deferred in v1 — raises :class:`NotImplementedError`."""
        raise NotImplementedError(
            "KalshiConnector.get_historical_prices is not implemented in "
            "v1. Historical price retrieval is deferred to a future "
            "checkpoint."
        )

    # ------------------------------------------------------------------
    # Auth signing stub (not exercised in v1, but code path must exist)
    # ------------------------------------------------------------------

    def _sign_request_headers(
        self, method: str, path: str, body: str = ""
    ) -> dict[str, str]:
        """Generate Kalshi signed headers.

        Only called when ``api_key_id`` and ``private_key_pem`` are both
        configured. Returns an empty dict otherwise so unauthenticated
        public endpoints work without any header overhead.

        **This is a deliberate stub.** A full Kalshi auth flow signs the
        request with RSA-PSS + SHA-256 over the timestamp + method + path,
        then attaches ``KALSHI-ACCESS-KEY``, ``KALSHI-ACCESS-SIGNATURE``,
        and ``KALSHI-ACCESS-TIMESTAMP`` headers. Implementing it properly
        requires `cryptography` as a new runtime dependency, which is out
        of scope for CP05 (the checkpoint only exercises mocked public
        endpoints). The stub preserves the code path so a later
        checkpoint can fill it in without restructuring the connector.
        """
        if not self._api_key_id or not self._private_key_pem:
            return {}
        # TODO(cp-future): Implement RSA-PSS signing with `cryptography`.
        # For now, emit only the access key header as a placeholder to
        # keep the code path reachable. Any attempt to call a truly
        # authenticated Kalshi endpoint with this stub will be rejected
        # by Kalshi — which is the safer failure mode than silently
        # shipping broken auth. ``method``/``path``/``body`` are unused
        # until the real signing lands; they are kept on the signature
        # so callers can already pass them.
        del method, path, body
        return {"KALSHI-ACCESS-KEY": self._api_key_id}

    # ------------------------------------------------------------------
    # Normalization helpers
    # ------------------------------------------------------------------

    def _normalize_market(self, raw: dict[str, Any]) -> Market:
        """Convert a Kalshi market dict to a normalized :class:`Market`.

        Kalshi markets are binary YES/NO instruments. The connector
        constructs two :class:`Outcome` objects:

        - ``<ticker>-YES`` at ``yes_bid / 100`` dollars
        - ``<ticker>-NO`` at ``no_bid / 100`` dollars

        Using the *bid* side means the outcome price represents the
        current best price at which a resting buyer is willing to take
        the side, which is the convention CP01 expects (``Outcome.price``
        = current market price for that leg).
        """
        ticker = str(raw.get("ticker", ""))
        title = str(raw.get("title", ""))
        description = self._build_description(raw)
        category = str(raw.get("category", ""))
        status = str(raw.get("status", ""))
        volume = self._to_decimal(raw.get("volume"))
        end_date = self._parse_date(
            raw.get("close_time")
            or raw.get("expected_expiration_time")
            or raw.get("expiration_time")
        )
        outcomes = self._parse_binary_outcomes(ticker, raw)
        # Kalshi exposes markets on the consumer site via the event ticker.
        event_ticker = raw.get("event_ticker")
        url = (
            f"https://kalshi.com/markets/{event_ticker}/{ticker}"
            if isinstance(event_ticker, str) and event_ticker and ticker
            else ""
        )

        return Market(
            platform=self.platform,
            market_id=ticker,
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

    def _parse_binary_outcomes(
        self, ticker: str, raw: dict[str, Any]
    ) -> list[Outcome]:
        """Extract YES/NO outcomes from a Kalshi binary market dict."""
        yes_title = str(raw.get("yes_sub_title") or "Yes")
        no_title = str(raw.get("no_sub_title") or "No")
        yes_price = self._cents_to_decimal(raw.get("yes_bid"))
        no_price = self._cents_to_decimal(raw.get("no_bid"))
        return [
            Outcome(
                outcome_id=f"{ticker}-YES" if ticker else "YES",
                title=yes_title,
                price=yes_price,
            ),
            Outcome(
                outcome_id=f"{ticker}-NO" if ticker else "NO",
                title=no_title,
                price=no_price,
            ),
        ]

    @staticmethod
    def _build_description(raw: dict[str, Any]) -> str:
        """Build a human-readable description from Kalshi-specific fields.

        Kalshi does not have a single ``description`` field. The
        ``subtitle`` and ``rules_primary`` fields together give a good
        summary; the connector concatenates whatever is present so
        :attr:`Market.description` is never empty unless Kalshi itself
        returns no prose at all.
        """
        parts: list[str] = []
        subtitle = raw.get("subtitle")
        if isinstance(subtitle, str) and subtitle:
            parts.append(subtitle)
        rules = raw.get("rules_primary")
        if isinstance(rules, str) and rules:
            parts.append(rules)
        return " ".join(parts)

    # ------------------------------------------------------------------
    # Orderbook normalization
    # ------------------------------------------------------------------

    def _normalize_orderbook(
        self, market_id: str, raw: dict[str, Any]
    ) -> OrderBook:
        """Convert a Kalshi orderbook response dict to :class:`OrderBook`.

        The Kalshi shape is ``{"orderbook": {"yes": [[cents, size], ...],
        "no":  [[cents, size], ...]}}``. Either side may be ``null`` if
        empty, in which case the connector emits an empty list rather
        than raising.

        YES bids are passed through directly. NO bids are mirrored into
        YES asks via ``ask_price = 1 - no_bid_price`` (review-loop fix
        f5) — see the class docstring for the binary-contract derivation.
        Sizes are unchanged by the transformation.
        """
        book = raw.get("orderbook")
        if not isinstance(book, dict):
            # A completely missing orderbook is valid when the market has
            # no resting orders on either side; return an empty book with
            # the current timestamp so downstream code has a consistent
            # shape.
            return OrderBook(
                platform=self.platform,
                market_id=market_id,
                bids=[],
                asks=[],
                timestamp=datetime.now(tz=timezone.utc),
            )

        yes_side = book.get("yes") or []
        no_side = book.get("no") or []

        bids = [self._parse_level(level) for level in yes_side]
        asks = [self._no_level_to_yes_ask(level) for level in no_side]

        return OrderBook(
            platform=self.platform,
            market_id=market_id,
            bids=bids,
            asks=asks,
            timestamp=datetime.now(tz=timezone.utc),
        )

    @staticmethod
    def _no_level_to_yes_ask(level: Any) -> PriceLevel:
        """Convert a Kalshi NO-side ``[cents, size]`` into a YES-leg ask.

        Review-loop fix f5: a NO bid at ``cents`` cents implies a YES ask
        at ``Decimal("1") - (cents / 100)``. Size is unchanged.
        """
        if not isinstance(level, (list, tuple)) or len(level) < 2:
            raise ValueError(
                f"Invalid Kalshi orderbook level (expected [cents, size]): "
                f"{level!r}"
            )
        no_price = KalshiConnector._cents_to_decimal(level[0])
        return PriceLevel(
            price=Decimal("1") - no_price,
            size=KalshiConnector._to_decimal(level[1]),
        )

    @staticmethod
    def _parse_level(level: Any) -> PriceLevel:
        """Parse a single ``[price_cents, size]`` tuple from a Kalshi book side."""
        if not isinstance(level, (list, tuple)) or len(level) < 2:
            raise ValueError(
                f"Invalid Kalshi orderbook level (expected [cents, size]): "
                f"{level!r}"
            )
        price_cents, size = level[0], level[1]
        return PriceLevel(
            price=KalshiConnector._cents_to_decimal(price_cents),
            size=KalshiConnector._to_decimal(size),
        )

    # ------------------------------------------------------------------
    # Scalar coercion helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _cents_to_decimal(cents: Any) -> Decimal:
        """Convert a Kalshi integer cent price (0-99) to Decimal dollars.

        Returns ``Decimal("0")`` when the input is ``None`` or not a
        valid number — matching the CP01 contract that :class:`Outcome`
        and :class:`PriceLevel` prices are always a :class:`Decimal` and
        never ``None``.
        """
        if cents is None:
            return Decimal("0")
        try:
            return Decimal(str(cents)) / Decimal("100")
        except (ArithmeticError, ValueError):
            return Decimal("0")

    @staticmethod
    def _to_decimal(value: Any) -> Decimal:
        """Coerce an arbitrary numeric/string field to :class:`Decimal`."""
        if value is None:
            return Decimal("0")
        if isinstance(value, Decimal):
            return value
        try:
            return Decimal(str(value))
        except (ArithmeticError, ValueError):
            return Decimal("0")

    @staticmethod
    def _parse_date(raw_date: Any) -> datetime | None:
        """Parse an ISO-8601 date. ``Z`` suffix is normalized to ``+00:00``."""
        if not raw_date or not isinstance(raw_date, str):
            return None
        try:
            return datetime.fromisoformat(raw_date.replace("Z", "+00:00"))
        except ValueError:
            return None
