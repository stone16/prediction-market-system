from __future__ import annotations

from datetime import UTC, datetime

import pytest

from pms.core.enums import MarketStatus
from pms.core.models import MarketSignal
from pms.controller.forecasters.statistical import StatisticalForecaster


def _signal() -> MarketSignal:
    return MarketSignal(
        market_id="market-statistical-migration",
        token_id="token-statistical-migration",
        venue="polymarket",
        title="Will the statistical migration stay behaviorally identical?",
        yes_price=0.5,
        volume_24h=1000.0,
        resolves_at=datetime(2026, 4, 30, tzinfo=UTC),
        orderbook={"bids": [], "asks": []},
        external_signal={"metaculus_prob": 0.7, "yes_count": 3, "no_count": 7},
        fetched_at=datetime(2026, 4, 18, 3, 15, tzinfo=UTC),
        market_status=MarketStatus.OPEN.value,
    )


@pytest.mark.skip(reason="unblocked by CP05 composition emulator")
def test_statistical_composition_emulator_matches_today() -> None:
    signal = _signal()
    reference_probability = StatisticalForecaster().predict(signal)[0]

    pytest.fail(
        "CP05 should replace this placeholder by comparing "
        f"the composition emulator to today's StatisticalForecaster output; "
        f"reference_probability={reference_probability}"
    )
