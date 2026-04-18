from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from pms.core.enums import MarketStatus
from pms.core.models import MarketSignal
from pms.factors.base import EMPTY_OUTER_RING
from pms.factors.definitions import MetaculusPrior, REGISTERED


SCHEMA_PATH = Path(__file__).resolve().parents[2] / "schema.sql"


def _signal(*, metaculus_prob: float | None = None) -> MarketSignal:
    external_signal: dict[str, object] = {}
    if metaculus_prob is not None:
        external_signal["metaculus_prob"] = metaculus_prob
    return MarketSignal(
        market_id="market-metaculus",
        token_id="token-metaculus",
        venue="polymarket",
        title="Will the raw prior stay separate from composition?",
        yes_price=0.5,
        volume_24h=1000.0,
        resolves_at=datetime(2026, 4, 30, tzinfo=UTC),
        orderbook={"bids": [], "asks": []},
        external_signal=external_signal,
        fetched_at=datetime(2026, 4, 18, 3, 0, tzinfo=UTC),
        market_status=MarketStatus.OPEN.value,
    )


def test_registered_includes_metaculus_prior() -> None:
    assert MetaculusPrior in REGISTERED


def test_metaculus_prior_returns_raw_probability() -> None:
    factor = MetaculusPrior()
    row = factor.compute(_signal(metaculus_prob=0.7), EMPTY_OUTER_RING)

    assert row is not None
    assert factor.required_inputs == ("external_signal.metaculus_prob",)
    assert row.factor_id == "metaculus_prior"
    assert row.param == ""
    assert row.value == pytest.approx(0.7)


def test_metaculus_prior_returns_none_when_input_missing() -> None:
    assert MetaculusPrior().compute(_signal(), EMPTY_OUTER_RING) is None


def test_schema_trailer_seeds_statistical_factor_ids() -> None:
    schema_text = SCHEMA_PATH.read_text()

    assert "metaculus_prior" in schema_text
    assert "yes_count" in schema_text
    assert "no_count" in schema_text
