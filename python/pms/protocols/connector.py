"""ConnectorProtocol — platform adapter interface.

Concrete implementations live in :mod:`pms.connectors` (CP04+).
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import datetime
from typing import Protocol, runtime_checkable

from pms.models import Market, OrderBook, PriceUpdate


@runtime_checkable
class ConnectorProtocol(Protocol):
    """Adapter interface for a prediction market platform."""

    platform: str

    async def get_active_markets(self) -> list[Market]:
        """Return all currently tradable markets on the platform."""
        ...

    async def get_orderbook(self, market_id: str) -> OrderBook:
        """Return the orderbook for a market.

        The ``market_id`` parameter is the **platform-specific book
        identifier**, not the normalized :class:`pms.models.Market.market_id`
        from :meth:`get_active_markets`. The two only coincide for some
        platforms; in general the caller must pick the right value:

        - **Polymarket**: CLOB ``token_id`` — i.e. an
          :attr:`pms.models.Outcome.outcome_id`. Polymarket identifies
          tradable outcomes by their ERC-1155 token id, so the
          ``Market.market_id`` (Gamma id / conditionId) returned by
          :meth:`get_active_markets` will *not* work here. Use the
          ``outcome_id`` of the leg you want to price.
        - **Kalshi**: market ticker, which equals
          :attr:`pms.models.Market.market_id`. Pass it through directly.

        Review-loop fix f3: this contract was previously implicit in the
        per-connector docstrings, which made it easy for a downstream
        consumer to pass ``market.market_id`` to the Polymarket connector
        and get a 4xx from CLOB. A future checkpoint will unify the call
        site behind a per-outcome orderbook API so callers no longer have
        to know the platform's identifier scheme.
        """
        ...

    def stream_prices(self, market_ids: list[str]) -> AsyncIterator[PriceUpdate]:
        """Stream incremental price updates for the given markets.

        Note: declared as a regular method (not ``async def``) because async
        generators must be invoked synchronously to obtain the iterator.
        """
        ...

    async def get_historical_prices(
        self, market_id: str, since: datetime
    ) -> list[PriceUpdate]:
        """Return historical price ticks for ``market_id`` since ``since``."""
        ...
