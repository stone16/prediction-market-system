from __future__ import annotations

from datetime import UTC, datetime

import pytest

from pms.core.enums import MarketStatus
from pms.core.models import MarketSignal
from pms.factors.base import EMPTY_OUTER_RING
from pms.factors.definitions import REGISTERED, YesCount


def _signal(*, yes_count: float | None = None) -> MarketSignal:
    external_signal: dict[str, object] = {}
    if yes_count is not None:
        external_signal["yes_count"] = yes_count
    return MarketSignal(
        market_id="market-yes-count",
        token_id="token-yes-count",
        venue="polymarket",
        title="Will yes_count stay raw?",
        yes_price=0.5,
        volume_24h=1000.0,
        resolves_at=datetime(2026, 4, 30, tzinfo=UTC),
        orderbook={"bids": [], "asks": []},
        external_signal=external_signal,
        fetched_at=datetime(2026, 4, 18, 3, 5, tzinfo=UTC),
        market_status=MarketStatus.OPEN.value,
    )


def test_registered_includes_yes_count() -> None:
    assert YesCount in REGISTERED


def test_yes_count_returns_raw_value_when_present() -> None:
    factor = YesCount()
    row = factor.compute(_signal(yes_count=7.0), EMPTY_OUTER_RING)

    assert row is not None
    assert factor.required_inputs == ("external_signal.yes_count",)
    assert row.factor_id == "yes_count"
    assert row.param == ""
    assert row.value == pytest.approx(7.0)


def test_yes_count_defaults_to_zero_when_input_missing() -> None:
    row = YesCount().compute(_signal(), EMPTY_OUTER_RING)

    assert row is not None
    assert row.value == 0.0
