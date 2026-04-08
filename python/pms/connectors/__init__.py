"""Concrete ConnectorProtocol implementations for prediction market platforms.

Connectors are thin adapters that translate platform-specific API responses
into the normalized :mod:`pms.models` types defined in CP01.
"""

from .polymarket import PolymarketConnector

__all__ = ["PolymarketConnector"]
