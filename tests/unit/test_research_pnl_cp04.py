from __future__ import annotations

from datetime import UTC, datetime

import pytest

from pms.core.models import MarketSignal
from pms.research.runner import _pnl_delta
from pms.research.specs import ExecutionModel


def _signal(*, resolved_outcome: float) -> MarketSignal:
    return MarketSignal(
        market_id="market-pnl-no",
        token_id="token-no",
        venue="polymarket",
        title="Will research pnl respect NO fills?",
        yes_price=0.62,
        volume_24h=1_000.0,
        resolves_at=datetime(2026, 4, 30, tzinfo=UTC),
        orderbook={"bids": [], "asks": []},
        external_signal={"resolved_outcome": resolved_outcome},
        fetched_at=datetime(2026, 4, 20, tzinfo=UTC),
        market_status="open",
    )


def test_pnl_delta_for_buy_no_uses_no_fill_price_directly() -> None:
    pnl = _pnl_delta(
        signal=_signal(resolved_outcome=0.0),
        decision_outcome="NO",
        decision_size=10.0,
        fill_price=0.38,
        execution_model=ExecutionModel.polymarket_paper(),
    )

    assert float(pnl) == pytest.approx(16.31578947368421)
