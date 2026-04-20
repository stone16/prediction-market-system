from __future__ import annotations

from dataclasses import FrozenInstanceError, is_dataclass
from datetime import UTC, datetime

import pytest

from pms.research.policies import (
    SelectionSimilarityMetric,
    SymbolNormalizationPolicy,
    TimeAlignmentPolicy,
)


def test_research_policies_are_frozen_dataclasses() -> None:
    for policy_cls in (
        TimeAlignmentPolicy,
        SymbolNormalizationPolicy,
        SelectionSimilarityMetric,
    ):
        assert is_dataclass(policy_cls)

    identity = TimeAlignmentPolicy()
    with pytest.raises(FrozenInstanceError):
        identity.generated_offset_s = 5.0  # type: ignore[misc]


def test_time_alignment_policy_applies_offsets_and_identity_is_noop() -> None:
    ts = datetime(2026, 4, 20, 12, 0, tzinfo=UTC)
    policy = TimeAlignmentPolicy(generated_offset_s=-3600.0)

    assert policy.apply_generated(ts) == datetime(2026, 4, 20, 11, 0, tzinfo=UTC)
    assert TimeAlignmentPolicy().apply_generated(ts) == ts


def test_symbol_normalization_policy_maps_aliases_and_leaves_unknowns_alone() -> None:
    policy = SymbolNormalizationPolicy(
        token_id_aliases={"BTC-USDT": "BTCUSDT"},
        market_id_aliases={"btc-usdt-market": "BTCUSDT-MARKET"},
    )

    assert policy.normalize_token_id("BTC-USDT") == "BTCUSDT"
    assert policy.normalize_token_id("ETHUSDT") == "ETHUSDT"
    assert policy.normalize_market_id("btc-usdt-market") == "BTCUSDT-MARKET"
    assert policy.normalize_market_id("eth-usdt-market") == "eth-usdt-market"


@pytest.mark.parametrize(
    ("denominator", "expected"),
    [
        ("backtest_set", 2.0 / 3.0),
        ("live_set", 2.0 / 4.0),
        ("union", 2.0 / 5.0),
    ],
)
def test_selection_similarity_metric_uses_explicit_denominator(
    denominator: str,
    expected: float,
) -> None:
    metric = SelectionSimilarityMetric(denominator=denominator)  # type: ignore[arg-type]

    overlap = metric.compute(
        backtest_set=frozenset({"A", "B", "C"}),
        live_set=frozenset({"B", "C", "D", "E"}),
    )

    assert overlap == pytest.approx(expected)


def test_selection_similarity_metric_handles_empty_and_identical_sets() -> None:
    backtest_metric = SelectionSimilarityMetric(denominator="backtest_set")
    live_metric = SelectionSimilarityMetric(denominator="live_set")
    union_metric = SelectionSimilarityMetric(denominator="union")
    identical = frozenset({"A", "B"})

    assert backtest_metric.compute(frozenset(), frozenset({"A"})) == 0.0
    assert live_metric.compute(identical, identical) == 1.0
    assert union_metric.compute(identical, identical) == 1.0
