from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from pms.config import ControllerSettings
from pms.controller.router import Router
from pms.core.models import MarketSignal


def _signal(**overrides: Any) -> MarketSignal:
    now = datetime(2026, 4, 27, 12, 0, tzinfo=UTC)
    data: dict[str, Any] = {
        "market_id": "router-market",
        "token_id": "router-token",
        "venue": "polymarket",
        "title": "Router market",
        "yes_price": 0.5,
        "volume_24h": 100.0,
        "resolves_at": now + timedelta(days=1),
        "orderbook": {"bids": [], "asks": []},
        "external_signal": {},
        "fetched_at": now,
        "market_status": "open",
    }
    data.update(overrides)
    return MarketSignal(**data)


def test_router_rejects_resolved_market() -> None:
    now = datetime(2026, 4, 27, 12, 0, tzinfo=UTC)
    assert not Router().gate(_signal(fetched_at=now, resolves_at=now))


def test_router_rejects_wide_spread_when_available() -> None:
    router = Router(ControllerSettings(max_spread_bps=100.0))
    assert not router.gate(_signal(external_signal={"spread_bps": 101.0}))


def test_router_rejects_stale_book_when_available() -> None:
    router = Router(ControllerSettings(max_book_age_ms=1_000.0))
    assert not router.gate(_signal(external_signal={"book_age_ms": 1_001.0}))


def test_router_rejects_non_open_status_from_signal_or_external_signal() -> None:
    router = Router()
    assert not router.gate(_signal(market_status="closed"))
    assert not router.gate(_signal(external_signal={"market_status": "halted"}))


def test_router_allows_signal_when_optional_quote_fields_are_absent() -> None:
    assert Router().gate(_signal())
