from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from pms.core.models import MarketRelation, MarketRelationType
from pms.factors.base import FactorValueRow
from pms.factors.catalog import FACTOR_CATALOG_ROWS
from pms.factors.market_relations import (
    RELATION_FACTOR_IDS,
    MarketRelationCandidate,
    MarketRelationService,
    detect_market_relation,
)


def _candidate(
    market_id: str,
    title: str,
    *,
    yes_price: float | None = 0.5,
    volume_24h: float | None = 1_000.0,
) -> MarketRelationCandidate:
    return MarketRelationCandidate(market_id, title, yes_price, volume_24h)


@dataclass
class _RelationStoreDouble:
    relations: list[MarketRelation] = field(default_factory=list)

    async def insert_relations(self, relations: Sequence[MarketRelation]) -> None:
        self.relations.extend(relations)


@dataclass
class _FactorSinkDouble:
    rows: list[FactorValueRow] = field(default_factory=list)

    async def write_factor_values(self, rows: Sequence[FactorValueRow]) -> None:
        self.rows.extend(rows)


@dataclass
class _MarketSourceDouble:
    candidates: list[MarketRelationCandidate]
    requested_limits: list[int] = field(default_factory=list)

    async def read_relation_candidates(
        self, *, limit: int
    ) -> Sequence[MarketRelationCandidate]:
        self.requested_limits.append(limit)
        return self.candidates


def test_relation_detector_matches_correlation_fixture_mapping() -> None:
    payload = json.loads(
        (
            Path(__file__).resolve().parents[1]
            / "fixtures"
            / "correlation_test_set.json"
        ).read_text(encoding="utf-8")
    )
    mapping = {
        "subset_or_superset": MarketRelationType.SUBSET,
        "contradictory": MarketRelationType.CONTRADICTION,
        "independent": MarketRelationType.INDEPENDENT,
    }

    for pair in payload["pairs"]:
        expected = str(pair["expected_relation"])
        detected = detect_market_relation(
            _candidate(
                str(pair["market_a"]["market_id"]),
                str(pair["market_a"]["title"]),
                yes_price=0.62 if expected == "contradictory" else 0.5,
            ),
            _candidate(
                str(pair["market_b"]["market_id"]),
                str(pair["market_b"]["title"]),
                yes_price=0.43 if expected == "contradictory" else 0.5,
            ),
        )
        assert detected.relation_type is mapping[expected], pair["id"]


def test_relation_detector_handles_temporal_contradiction_and_similarity() -> None:
    temporal = detect_market_relation(
        _candidate("short", "Bitcoin reaches 100000 dollars by June"),
        _candidate("long", "Bitcoin reaches 100000 dollars by December"),
    )
    violating = detect_market_relation(
        _candidate("over", "Ethereum price more than 5000 dollars", yes_price=0.62),
        _candidate("under", "Ethereum price less than 5000 dollars", yes_price=0.43),
    )
    non_violating = detect_market_relation(
        _candidate("over", "Ethereum price more than 5000 dollars", yes_price=0.52),
        _candidate("under", "Ethereum price less than 5000 dollars", yes_price=0.41),
    )
    similar = detect_market_relation(
        _candidate("btc-a", "Bitcoin reaches 100000 dollars in 2026"),
        _candidate("btc-b", "Bitcoin hits 100000 dollars during 2026"),
    )

    assert temporal.relation_type is MarketRelationType.SUBSET
    assert temporal.metadata["basis"] == "temporal_containment"
    assert violating.relation_type is MarketRelationType.CONTRADICTION
    assert violating.metadata["mispricing"] == pytest.approx(0.05)
    assert non_violating.relation_type is not MarketRelationType.CONTRADICTION
    assert similar.relation_type is MarketRelationType.SIMILAR
    assert similar.confidence >= 0.4


@pytest.mark.asyncio
async def test_relation_service_caps_pairwise_scan_and_persists_subset_rows() -> None:
    source = _MarketSourceDouble(
        [
            _candidate("low", "Unrelated low volume market", volume_24h=1.0),
            _candidate("subset", "Fed raises interest rates by at least 25 basis points", volume_24h=100.0),
            _candidate("superset", "Fed raises interest rates", volume_24h=200.0),
        ]
    )
    relation_store = _RelationStoreDouble()
    factor_sink = _FactorSinkDouble()

    persisted = await MarketRelationService(
        market_source=source,
        relation_store=relation_store,
        factor_sink=factor_sink,
        max_markets=2,
        clock=lambda: datetime(2026, 5, 10, 10, 0, tzinfo=UTC),
    ).compute_once()

    assert source.requested_limits == [2]
    assert persisted == 1
    assert relation_store.relations[0].relation_type is MarketRelationType.SUBSET
    assert {row.factor_id for row in factor_sink.rows} == {"subset_relation"}
    assert {row.market_id for row in factor_sink.rows} == {"subset", "superset"}


@pytest.mark.asyncio
async def test_relation_service_emits_contradiction_and_mispricing_factors() -> None:
    relation_store = _RelationStoreDouble()
    factor_sink = _FactorSinkDouble()
    service = MarketRelationService(
        market_source=_MarketSourceDouble(
            [
                _candidate("over", "Apple stock price more than 200 dollars", yes_price=0.64),
                _candidate("under", "Apple stock price less than 200 dollars", yes_price=0.44),
            ]
        ),
        relation_store=relation_store,
        factor_sink=factor_sink,
    )

    assert service.interval == timedelta(minutes=30)
    assert await service.compute_once() == 1
    assert relation_store.relations[0].relation_type is MarketRelationType.CONTRADICTION
    rows = {(row.factor_id, row.market_id): row.value for row in factor_sink.rows}
    assert rows[("contradiction_relation", "over")] == pytest.approx(1.0)
    assert rows[("contradiction_relation", "under")] == pytest.approx(1.0)
    assert rows[("cross_market_mispricing", "over")] == pytest.approx(0.08)
    assert rows[("cross_market_mispricing", "under")] == pytest.approx(0.08)


def test_relation_factor_catalog_rows_are_registered() -> None:
    assert set(RELATION_FACTOR_IDS) <= {entry.factor_id for entry in FACTOR_CATALOG_ROWS}
