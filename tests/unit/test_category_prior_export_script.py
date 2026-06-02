from __future__ import annotations

import pytest

from scripts.export_category_prior_observations import (
    CategoryPriorCsvRow,
    observation_row_from_gamma_market,
)


def test_observation_row_from_gamma_market_exports_binary_yes_resolution() -> None:
    row = observation_row_from_gamma_market(
        {
            "conditionId": "0xmarket",
            "category": "Politics",
            "closedTime": "2026-06-02 05:14:32+00",
            "outcomes": '["Yes", "No"]',
            "outcomePrices": '["1", "0"]',
        }
    )

    assert row == CategoryPriorCsvRow(
        market_id="0xmarket",
        category="Politics",
        yes_payout="1",
        no_payout="0",
        resolved_at="2026-06-02T05:14:32Z",
    )


def test_observation_row_from_gamma_market_falls_back_to_series_slug() -> None:
    row = observation_row_from_gamma_market(
        {
            "conditionId": "0xmarket",
            "closedTime": "2026-06-02T05:14:32Z",
            "outcomes": '["Yes", "No"]',
            "outcomePrices": '["0", "1"]',
            "events": [
                {
                    "slug": "event-slug",
                    "series": [{"slug": "legislation-signed"}],
                }
            ],
        }
    )

    assert row is not None
    assert row.category == "legislation-signed"
    assert row.yes_payout == "0"
    assert row.no_payout == "1"


def test_observation_row_from_gamma_market_skips_unresolved_prices() -> None:
    row = observation_row_from_gamma_market(
        {
            "conditionId": "0xmarket",
            "category": "Politics",
            "closedTime": "2026-06-02T05:14:32Z",
            "outcomes": '["Yes", "No"]',
            "outcomePrices": '["0.5", "0.5"]',
        }
    )

    assert row is None


def test_observation_row_from_gamma_market_skips_non_binary_markets() -> None:
    row = observation_row_from_gamma_market(
        {
            "conditionId": "0xmarket",
            "category": "Politics",
            "closedTime": "2026-06-02T05:14:32Z",
            "outcomes": '["A", "B", "C"]',
            "outcomePrices": '["1", "0", "0"]',
        }
    )

    assert row is None


@pytest.mark.parametrize(
    "malformed_field",
    [
        {"outcomes": '["Yes",'},
        {"outcomePrices": '["1",'},
    ],
)
def test_observation_row_from_gamma_market_skips_malformed_json_sequences(
    malformed_field: dict[str, str],
) -> None:
    row = observation_row_from_gamma_market(
        {
            "conditionId": "0xmarket",
            "category": "Politics",
            "closedTime": "2026-06-02T05:14:32Z",
            "outcomes": '["Yes", "No"]',
            "outcomePrices": '["1", "0"]',
            **malformed_field,
        }
    )

    assert row is None
