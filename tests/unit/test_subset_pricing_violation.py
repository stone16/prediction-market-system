from __future__ import annotations

from datetime import UTC, datetime

import pytest

from pms.core.enums import MarketStatus
from pms.core.models import MarketSignal
from pms.factors.base import EMPTY_OUTER_RING
from pms.factors.definitions import REGISTERED, SubsetPricingViolation


def _signal(
    *,
    subset_price: float | None = None,
    superset_price: float | None = None,
) -> MarketSignal:
    external_signal: dict[str, object] = {}
    if subset_price is not None:
        external_signal["subset_price"] = subset_price
    if superset_price is not None:
        external_signal["superset_price"] = superset_price
    return MarketSignal(
        market_id="market-subset",
        token_id="token-subset",
        venue="polymarket",
        title="Will subset pricing stay raw?",
        yes_price=0.5,
        volume_24h=1000.0,
        resolves_at=datetime(2026, 4, 30, tzinfo=UTC),
        orderbook={"bids": [], "asks": []},
        external_signal=external_signal,
        fetched_at=datetime(2026, 4, 18, 2, 5, tzinfo=UTC),
        market_status=MarketStatus.OPEN.value,
    )


def test_registered_includes_subset_pricing_violation() -> None:
    assert SubsetPricingViolation in REGISTERED


def test_subset_pricing_violation_returns_raw_difference() -> None:
    row = SubsetPricingViolation().compute(
        _signal(subset_price=0.3, superset_price=0.2),
        EMPTY_OUTER_RING,
    )

    assert row is not None
    assert row.factor_id == "subset_pricing_violation"
    assert row.param == ""
    assert row.value == pytest.approx(0.1)


@pytest.mark.parametrize(
    ("subset_price", "superset_price"),
    [
        (None, 0.2),
        (0.3, None),
    ],
)
def test_subset_pricing_violation_returns_none_when_inputs_missing(
    subset_price: float | None,
    superset_price: float | None,
) -> None:
    assert (
        SubsetPricingViolation().compute(
            _signal(subset_price=subset_price, superset_price=superset_price),
            EMPTY_OUTER_RING,
        )
        is None
    )
