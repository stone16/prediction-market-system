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


FIXTURE_PATH = Path(__file__).resolve().parents[1] / "fixtures" / "correlation_test_set.json"


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
        self,
        *,
        limit: int,
    ) -> Sequence[MarketRelationCandidate]:
        self.requested_limits.append(limit)
        return self.candidates


def _candidate(
    market_id: str,
    title: str,
    *,
    yes_price: float | None = 0.5,
    volume_24h: float | None = 1_000.0,
) -> MarketRelationCandidate:
    return MarketRelationCandidate(
        market_id=market_id,
        title=title,
        yes_price=yes_price,
        volume_24h=volume_24h,
    )


def test_relation_detector_matches_correlation_fixture_mapping() -> None:
    payload = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
    mapping = {
        "subset_or_superset": MarketRelationType.SUBSET,
        "contradictory": MarketRelationType.CONTRADICTION,
        "independent": MarketRelationType.INDEPENDENT,
    }

    for pair in payload["pairs"]:
        expected_relation = str(pair["expected_relation"])
        yes_price_a = 0.62 if expected_relation == "contradictory" else 0.5
        yes_price_b = 0.43 if expected_relation == "contradictory" else 0.5

        detected = detect_market_relation(
            _candidate(
                str(pair["market_a"]["market_id"]),
                str(pair["market_a"]["title"]),
                yes_price=yes_price_a,
            ),
            _candidate(
                str(pair["market_b"]["market_id"]),
                str(pair["market_b"]["title"]),
                yes_price=yes_price_b,
            ),
        )

        assert detected.relation_type is mapping[expected_relation], pair["id"]


def test_relation_detector_uses_temporal_containment_for_subset() -> None:
    detected = detect_market_relation(
        _candidate("short-window", "Bitcoin reaches 100000 dollars by June"),
        _candidate("long-window", "Bitcoin reaches 100000 dollars by December"),
    )

    assert detected.relation_type is MarketRelationType.SUBSET
    assert detected.metadata["basis"] == "temporal_containment"


def test_relation_detector_requires_pricing_violation_for_contradiction() -> None:
    violating = detect_market_relation(
        _candidate("over", "Ethereum price more than 5000 dollars", yes_price=0.62),
        _candidate("under", "Ethereum price less than 5000 dollars", yes_price=0.43),
    )
    non_violating = detect_market_relation(
        _candidate("over", "Ethereum price more than 5000 dollars", yes_price=0.52),
        _candidate("under", "Ethereum price less than 5000 dollars", yes_price=0.41),
    )

    assert violating.relation_type is MarketRelationType.CONTRADICTION
    assert violating.metadata["mispricing"] == pytest.approx(0.05)
    assert non_violating.relation_type is not MarketRelationType.CONTRADICTION


def test_relation_detector_supports_semantic_similarity_relation() -> None:
    detected = detect_market_relation(
        _candidate("btc-a", "Bitcoin reaches 100000 dollars in 2026"),
        _candidate("btc-b", "Bitcoin hits 100000 dollars during 2026"),
    )

    assert detected.relation_type is MarketRelationType.SIMILAR
    assert detected.confidence >= 0.4


@pytest.mark.asyncio
async def test_relation_service_caps_pairwise_scan_and_persists_factor_rows() -> None:
    source = _MarketSourceDouble(
        candidates=[
            _candidate("low", "Unrelated low volume market", volume_24h=1.0),
            _candidate("subset", "Fed raises interest rates by at least 25 basis points", volume_24h=100.0),
            _candidate("superset", "Fed raises interest rates", volume_24h=200.0),
        ]
    )
    relation_store = _RelationStoreDouble()
    factor_sink = _FactorSinkDouble()
    service = MarketRelationService(
        market_source=source,
        relation_store=relation_store,
        factor_sink=factor_sink,
        max_markets=2,
        clock=lambda: datetime(2026, 5, 10, 10, 0, tzinfo=UTC),
    )

    persisted = await service.compute_once()

    assert source.requested_limits == [2]
    assert persisted == 1
    assert [relation.relation_type for relation in relation_store.relations] == [
        MarketRelationType.SUBSET
    ]
    assert {row.factor_id for row in factor_sink.rows} == {"subset_relation"}
    assert {row.market_id for row in factor_sink.rows} == {"subset", "superset"}


@pytest.mark.asyncio
async def test_relation_service_emits_contradiction_and_mispricing_factors() -> None:
    source = _MarketSourceDouble(
        candidates=[
            _candidate("over", "Apple stock price more than 200 dollars", yes_price=0.64),
            _candidate("under", "Apple stock price less than 200 dollars", yes_price=0.44),
        ]
    )
    relation_store = _RelationStoreDouble()
    factor_sink = _FactorSinkDouble()
    service = MarketRelationService(
        market_source=source,
        relation_store=relation_store,
        factor_sink=factor_sink,
    )

    persisted = await service.compute_once()

    assert persisted == 1
    assert relation_store.relations[0].relation_type is MarketRelationType.CONTRADICTION
    factor_rows = {(row.factor_id, row.market_id): row.value for row in factor_sink.rows}
    assert factor_rows[("contradiction_relation", "over")] == pytest.approx(1.0)
    assert factor_rows[("contradiction_relation", "under")] == pytest.approx(1.0)
    assert factor_rows[("cross_market_mispricing", "over")] == pytest.approx(0.08)
    assert factor_rows[("cross_market_mispricing", "under")] == pytest.approx(0.08)


def test_relation_service_defaults_to_fixed_thirty_minute_interval() -> None:
    service = MarketRelationService(
        market_source=_MarketSourceDouble([]),
        relation_store=_RelationStoreDouble(),
    )

    assert service.interval == timedelta(minutes=30)


def test_relation_factor_catalog_rows_are_registered() -> None:
    catalog_ids = {entry.factor_id for entry in FACTOR_CATALOG_ROWS}

    assert set(RELATION_FACTOR_IDS) <= catalog_ids
