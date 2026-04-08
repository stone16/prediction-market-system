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

* Phase 3A wires real implementations for ``stream_prices`` (polling
  fallback over the public CLOB ``/book`` endpoint, with a bounded
  ``max_iterations`` for tests) and ``get_historical_prices`` (CLOB
  ``/prices-history`` endpoint). True WebSocket streaming via
  ``wss://ws-subscriptions-clob.polymarket.com`` is still deferred —
  it would add a websockets dependency and reconnect/heartbeat
  plumbing this connector intentionally does not carry today.

* The full Gamma API response dict for each market is preserved verbatim
  on :attr:`pms.models.Market.raw` so downstream code can recover any
  platform-specific field without re-fetching.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, cast

import httpx

from pms.models import Market, OrderBook, Outcome, PriceLevel, PriceUpdate

logger = logging.getLogger(__name__)


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
        :attr:`Outcome.outcome_id` from :meth:`get_active_markets`. Passing
        the normalized :attr:`Market.market_id` (Gamma id / conditionId)
        will fail with a 4xx from CLOB.

        See :meth:`pms.protocols.connector.ConnectorProtocol.get_orderbook`
        for the per-platform parameter contract — review-loop fix f3
        documents the cross-connector mismatch.
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
    # ConnectorProtocol — streaming / history (Phase 3A)
    # ------------------------------------------------------------------

    def stream_prices(
        self,
        market_ids: list[str],
        *,
        poll_interval_seconds: float = 5.0,
        max_iterations: int | None = None,
    ) -> AsyncIterator[PriceUpdate]:
        """Yield top-of-book :class:`PriceUpdate`\\s for ``market_ids``.

        Phase 3A ships a **polling-based** stream that calls
        :meth:`get_orderbook` for each market every
        ``poll_interval_seconds`` and yields one ``PriceUpdate`` per
        market per iteration. Real WebSocket streaming via
        ``wss://ws-subscriptions-clob.polymarket.com`` is still
        deferred — it would add a ``websockets`` dependency and
        reconnect/heartbeat plumbing the polling fallback does not
        need.

        Parameters
        ----------
        market_ids:
            CLOB token ids (one per outcome leg). Same identifier
            scheme as :meth:`get_orderbook` — see the protocol's
            ``market_id`` contract for the per-platform mapping.
        poll_interval_seconds:
            Wait between iterations across the full market list.
            Default 5s mirrors the CLOB rate-limit guidance for
            unauthenticated polling clients.
        max_iterations:
            If set, stops after this many polling rounds. ``None``
            (the default) streams forever; tests pass an integer to
            keep the iterator bounded.

        The protocol declares this as a regular ``def``; ``async def``
        with ``yield`` is structurally compatible (Python returns the
        async generator on call without an extra ``await``).

        Per-market fetch failures are logged and skipped — a single
        broken book never silences the rest of the stream.
        """
        return self._stream_prices_impl(
            market_ids,
            poll_interval_seconds=poll_interval_seconds,
            max_iterations=max_iterations,
        )

    async def _stream_prices_impl(
        self,
        market_ids: list[str],
        *,
        poll_interval_seconds: float,
        max_iterations: int | None,
    ) -> AsyncIterator[PriceUpdate]:
        iteration = 0
        while max_iterations is None or iteration < max_iterations:
            for market_id in market_ids:
                try:
                    book = await self.get_orderbook(market_id)
                except Exception as exc:  # noqa: BLE001 — defensive
                    logger.warning(
                        "stream_prices: get_orderbook(%s) failed: %s",
                        market_id,
                        exc,
                    )
                    continue
                yield self._book_to_price_update(market_id, book)
            iteration += 1
            if max_iterations is not None and iteration >= max_iterations:
                return
            # Sleep between rounds; tests pass a tiny interval so this
            # is effectively a no-op under their bounded max_iterations.
            await asyncio.sleep(poll_interval_seconds)

    @staticmethod
    def _book_to_price_update(
        market_id: str, book: OrderBook
    ) -> PriceUpdate:
        """Compress an OrderBook snapshot into a single PriceUpdate.

        ``last`` is the mid-price when both sides are populated, else
        whichever side has data — keeps downstream consumers from
        having to special-case empty books.
        """
        top_bid = book.bids[0].price if book.bids else Decimal("0")
        top_ask = book.asks[0].price if book.asks else Decimal("0")
        if top_bid > 0 and top_ask > 0:
            mid = (top_bid + top_ask) / Decimal("2")
        else:
            mid = top_bid if top_bid > 0 else top_ask
        return PriceUpdate(
            platform="polymarket",
            market_id=market_id,
            outcome_id=market_id,  # CLOB token id == outcome id
            bid=top_bid,
            ask=top_ask,
            last=mid,
            timestamp=book.timestamp,
        )

    async def get_historical_prices(
        self, market_id: str, since: datetime
    ) -> list[PriceUpdate]:
        """Fetch historical price ticks via the CLOB ``/prices-history`` endpoint.

        Parameters
        ----------
        market_id:
            CLOB token id (same identifier scheme as
            :meth:`get_orderbook`).
        since:
            Start timestamp. Naive datetimes are interpreted as UTC.
            The endpoint accepts a unix-seconds ``startTs`` query param.

        Returns a list of :class:`PriceUpdate` ordered by timestamp
        ascending. The CLOB endpoint only exposes a single price per
        tick (not bid/ask), so the returned ``PriceUpdate.bid``,
        ``ask`` and ``last`` all carry the same value — downstream
        consumers should treat this as a "last trade" series rather
        than a top-of-book series.
        """
        # Normalize to a UTC timestamp; naive datetimes get UTC.
        since_utc = (
            since
            if since.tzinfo is not None
            else since.replace(tzinfo=timezone.utc)
        )
        start_ts = int(since_utc.timestamp())

        resp = await self._http.get(
            f"{self._clob_base}/prices-history",
            params={
                "market": market_id,
                "startTs": start_ts,
                "fidelity": 1,
            },
        )
        resp.raise_for_status()
        data = resp.json()
        if not isinstance(data, dict):
            raise ValueError(
                "Polymarket CLOB /prices-history endpoint returned a "
                f"non-dict payload: {type(data).__name__}"
            )

        history_raw = data.get("history", []) or []
        if not isinstance(history_raw, list):
            return []

        updates: list[PriceUpdate] = []
        for tick in history_raw:
            if not isinstance(tick, dict):
                continue
            ts_val = tick.get("t")
            price_val = tick.get("p")
            if ts_val is None or price_val is None:
                continue
            try:
                # ``t`` is unix seconds (int); be defensive about strings.
                ts = datetime.fromtimestamp(int(ts_val), tz=timezone.utc)
                price = self._to_decimal(price_val)
            except (TypeError, ValueError, ArithmeticError):
                continue
            updates.append(
                PriceUpdate(
                    platform=self.platform,
                    market_id=market_id,
                    outcome_id=market_id,
                    bid=price,
                    ask=price,
                    last=price,
                    timestamp=ts,
                )
            )
        return updates

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
