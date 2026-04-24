from __future__ import annotations

from dataclasses import FrozenInstanceError
from datetime import UTC, datetime

import pytest

from pms.core.models import Market


def test_market_dataclass_accepts_price_fields() -> None:
    price_updated_at = datetime(2026, 4, 24, 9, 30, tzinfo=UTC)

    market = Market(
        condition_id="condition-1",
        slug="will-cp01-land",
        question="Will CP01 land?",
        venue="polymarket",
        resolves_at=None,
        created_at=datetime(2026, 4, 23, 9, 0, tzinfo=UTC),
        last_seen_at=datetime(2026, 4, 24, 9, 0, tzinfo=UTC),
        volume_24h=1200.5,
        yes_price=0.525,
        no_price=0.475,
        best_bid=0.51,
        best_ask=0.54,
        last_trade_price=0.52,
        liquidity=25000.75,
        spread_bps=300,
        price_updated_at=price_updated_at,
    )

    assert market.yes_price == 0.525
    assert market.no_price == 0.475
    assert market.best_bid == 0.51
    assert market.best_ask == 0.54
    assert market.last_trade_price == 0.52
    assert market.liquidity == 25000.75
    assert market.spread_bps == 300
    assert market.price_updated_at == price_updated_at

    with pytest.raises(FrozenInstanceError):
        market.yes_price = 0.5  # type: ignore[misc]
