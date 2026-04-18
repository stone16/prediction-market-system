from __future__ import annotations

from datetime import UTC, datetime

import pytest

from pms.core.enums import MarketStatus
from pms.core.models import MarketSignal
from pms.controller.forecasters.statistical import StatisticalForecaster
from pms.factors.base import EMPTY_OUTER_RING
from pms.factors.composition import apply_composition, evaluate_branch_probabilities
from pms.factors.definitions import REGISTERED
from pms.strategies.defaults import DEFAULT_STRATEGY_COMPOSITION


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


def test_statistical_forecaster_is_neutral_while_default_v2_keeps_posterior_branch() -> None:
    signal = _signal()
    reference_probability = StatisticalForecaster().predict(signal)[0]
    factor_values: dict[tuple[str, str], float] = {
        ("yes_price", ""): signal.yes_price,
    }
    for factor_cls in REGISTERED:
        row = factor_cls().compute(signal, EMPTY_OUTER_RING)
        if row is not None:
            factor_values[(row.factor_id, row.param)] = row.value
    branch_probabilities = evaluate_branch_probabilities(
        DEFAULT_STRATEGY_COMPOSITION,
        factor_values,
    )
    composed_probability = apply_composition(DEFAULT_STRATEGY_COMPOSITION, factor_values)

    assert reference_probability == pytest.approx(signal.yes_price, abs=1e-9)
    assert branch_probabilities["statistical"] == pytest.approx(11.0 / 30.0, abs=1e-9)
    assert composed_probability == pytest.approx(11.0 / 30.0, abs=1e-9)
