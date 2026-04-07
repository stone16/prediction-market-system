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
        """Return a snapshot of the order book for ``market_id``."""
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
