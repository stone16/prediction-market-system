from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from pms.core.enums import MarketStatus
from pms.core.models import MarketSignal
from pms.factors.base import EMPTY_OUTER_RING
from pms.factors.definitions import FairValueSpread, REGISTERED


SCHEMA_PATH = Path("schema.sql")


def _signal(*, yes_price: float = 0.5, fair_value: float | None = None) -> MarketSignal:
    external_signal: dict[str, object] = {}
    if fair_value is not None:
        external_signal["fair_value"] = fair_value
    return MarketSignal(
        market_id="market-fair-value",
        token_id="token-fair-value",
        venue="polymarket",
        title="Will fair value spread stay raw?",
        yes_price=yes_price,
        volume_24h=1000.0,
        resolves_at=datetime(2026, 4, 30, tzinfo=UTC),
        orderbook={"bids": [], "asks": []},
        external_signal=external_signal,
        fetched_at=datetime(2026, 4, 18, 2, 0, tzinfo=UTC),
        market_status=MarketStatus.OPEN.value,
    )


def test_registered_includes_fair_value_spread() -> None:
    assert FairValueSpread in REGISTERED


def test_fair_value_spread_returns_raw_edge() -> None:
    factor = FairValueSpread()
    row = factor.compute(_signal(yes_price=0.5, fair_value=0.6), EMPTY_OUTER_RING)

    assert row is not None
    assert factor.required_inputs == ("external_signal.fair_value", "yes_price")
    assert row.factor_id == "fair_value_spread"
    assert row.param == ""
    assert row.value == pytest.approx(0.1)


def test_fair_value_spread_returns_none_when_fair_value_missing() -> None:
    assert FairValueSpread().compute(_signal(), EMPTY_OUTER_RING) is None


def test_fair_value_spread_keeps_sub_min_edge_value_raw() -> None:
    row = FairValueSpread().compute(_signal(yes_price=0.5, fair_value=0.51), EMPTY_OUTER_RING)

    assert row is not None
    assert row.value == pytest.approx(0.01)


def test_schema_trailer_seeds_rules_migration_factors() -> None:
    schema_text = SCHEMA_PATH.read_text()

    assert "fair_value_spread" in schema_text
    assert "subset_pricing_violation" in schema_text
