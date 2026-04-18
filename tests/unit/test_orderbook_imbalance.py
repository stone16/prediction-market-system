from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest

from pms.core.enums import MarketStatus
from pms.core.models import MarketSignal
from pms.factors.base import EMPTY_OUTER_RING
from pms.factors.definitions import REGISTERED
from pms.factors.definitions.orderbook_imbalance import OrderbookImbalance


def _signal(*, orderbook: dict[str, Any]) -> MarketSignal:
    return MarketSignal(
        market_id="market-orderbook",
        token_id="token-orderbook",
        venue="polymarket",
        title="Will orderbook imbalance compute?",
        yes_price=0.47,
        volume_24h=1000.0,
        resolves_at=datetime(2026, 4, 30, tzinfo=UTC),
        orderbook=orderbook,
        external_signal={},
        fetched_at=datetime(2026, 4, 18, 1, 5, tzinfo=UTC),
        market_status=MarketStatus.OPEN.value,
    )


def test_registered_exports_orderbook_imbalance() -> None:
    assert OrderbookImbalance in REGISTERED


def test_orderbook_imbalance_compute_returns_factor_value_row() -> None:
    signal = _signal(
        orderbook={
            "bids": [
                {"price": 0.46, "size": 100.0},
                {"price": 0.45, "size": 50.0},
            ],
            "asks": [
                {"price": 0.48, "size": 30.0},
                {"price": 0.49, "size": 20.0},
            ],
        }
    )

    row = OrderbookImbalance().compute(signal, EMPTY_OUTER_RING)

    assert row is not None
    assert row.factor_id == "orderbook_imbalance"
    assert row.param == ""
    assert row.market_id == signal.market_id
    assert row.ts == signal.timestamp
    assert row.value == pytest.approx(0.5)


def test_orderbook_imbalance_break_points_cover_one_sided_and_empty_books() -> None:
    bid_only_signal = _signal(
        orderbook={
            "bids": [{"price": 0.46, "size": 100.0}],
            "asks": [],
        }
    )
    ask_only_signal = _signal(
        orderbook={
            "bids": [],
            "asks": [{"price": 0.48, "size": 100.0}],
        }
    )
    empty_signal = _signal(orderbook={"bids": [], "asks": []})

    bid_only_row = OrderbookImbalance().compute(bid_only_signal, EMPTY_OUTER_RING)
    ask_only_row = OrderbookImbalance().compute(ask_only_signal, EMPTY_OUTER_RING)

    assert bid_only_row is not None
    assert bid_only_row.value == pytest.approx(1.0)
    assert ask_only_row is not None
    assert ask_only_row.value == pytest.approx(-1.0)
    assert OrderbookImbalance().compute(empty_signal, EMPTY_OUTER_RING) is None
