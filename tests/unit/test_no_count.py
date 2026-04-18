from __future__ import annotations

from datetime import UTC, datetime

import pytest

from pms.core.enums import MarketStatus
from pms.core.models import MarketSignal
from pms.factors.base import EMPTY_OUTER_RING
from pms.factors.definitions import NoCount, REGISTERED


def _signal(*, no_count: float | None = None) -> MarketSignal:
    external_signal: dict[str, object] = {}
    if no_count is not None:
        external_signal["no_count"] = no_count
    return MarketSignal(
        market_id="market-no-count",
        token_id="token-no-count",
        venue="polymarket",
        title="Will no_count stay raw?",
        yes_price=0.5,
        volume_24h=1000.0,
        resolves_at=datetime(2026, 4, 30, tzinfo=UTC),
        orderbook={"bids": [], "asks": []},
        external_signal=external_signal,
        fetched_at=datetime(2026, 4, 18, 3, 10, tzinfo=UTC),
        market_status=MarketStatus.OPEN.value,
    )


def test_registered_includes_no_count() -> None:
    assert NoCount in REGISTERED


def test_no_count_returns_raw_value_when_present() -> None:
    factor = NoCount()
    row = factor.compute(_signal(no_count=4.0), EMPTY_OUTER_RING)

    assert row is not None
    assert factor.required_inputs == ("external_signal.no_count",)
    assert row.factor_id == "no_count"
    assert row.param == ""
    assert row.value == pytest.approx(4.0)


def test_no_count_returns_none_when_input_missing() -> None:
    row = NoCount().compute(_signal(), EMPTY_OUTER_RING)

    assert row is None
