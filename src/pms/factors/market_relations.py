from __future__ import annotations

import asyncio
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
import math
import re
from typing import Any, Protocol, cast

import asyncpg

from pms.core.models import MarketRelation, MarketRelationType
from pms.factors.base import FactorValueRow
from pms.factors.catalog import ensure_factor_catalog
from pms.factors.service import persist_factor_value


RELATION_FACTOR_IDS = (
    "subset_relation",
    "contradiction_relation",
    "semantic_similarity",
    "cross_market_mispricing",
)


@dataclass(frozen=True, slots=True)
class MarketRelationCandidate:
    market_id: str
    title: str
    yes_price: float | None
    volume_24h: float | None = None
    rules: str | None = None

    @property
    def text(self) -> str:
        if self.rules is None:
            return self.title
        return f"{self.title} {self.rules}"


@dataclass(frozen=True, slots=True)
class DetectedMarketRelation:
    relation_type: MarketRelationType
    confidence: float
    metadata: Mapping[str, Any] = field(default_factory=dict)


class MarketRelationCandidateSource(Protocol):
    async def read_relation_candidates(
        self,
        *,
        limit: int,
    ) -> Sequence[MarketRelationCandidate]: ...


class MarketRelationWriter(Protocol):
    async def insert_relations(self, relations: Sequence[MarketRelation]) -> None: ...


class FactorValueSink(Protocol):
    async def write_factor_values(self, rows: Sequence[FactorValueRow]) -> None: ...


@dataclass
class PostgresFactorValueSink:
    pool: asyncpg.Pool

    async def write_factor_values(self, rows: Sequence[FactorValueRow]) -> None:
        if not rows:
            return
        await ensure_factor_catalog(self.pool, factor_ids=RELATION_FACTOR_IDS)
        for row in rows:
            await persist_factor_value(self.pool, row)


@dataclass
class MarketRelationService:
    market_source: MarketRelationCandidateSource
    relation_store: MarketRelationWriter
    factor_sink: FactorValueSink | None = None
    interval: timedelta = timedelta(minutes=30)
    max_markets: int = 200
    similarity_threshold: float = 0.42
    include_independent: bool = False
    clock: Callable[[], datetime] = lambda: datetime.now(tz=UTC)

    def __post_init__(self) -> None:
        if self.interval <= timedelta(0):
            msg = "interval must be positive"
            raise ValueError(msg)
        if self.max_markets <= 0:
            msg = "max_markets must be positive"
            raise ValueError(msg)
        if not 0.0 <= self.similarity_threshold <= 1.0:
            msg = "similarity_threshold must be between 0.0 and 1.0"
            raise ValueError(msg)

    async def run(self) -> None:
        while True:
            await self.compute_once()
            await asyncio.sleep(self.interval.total_seconds())

    async def compute_once(self) -> int:
        raw_candidates = await self.market_source.read_relation_candidates(
            limit=self.max_markets
        )
        candidates = _rank_by_volume(raw_candidates)[: self.max_markets]
        detected_at = self.clock()
        relations: list[MarketRelation] = []
        factor_rows: list[FactorValueRow] = []
        for index, market_a in enumerate(candidates):
            for market_b in candidates[index + 1 :]:
                detected = detect_market_relation(
                    market_a,
                    market_b,
                    similarity_threshold=self.similarity_threshold,
                )
                if (
                    detected.relation_type is MarketRelationType.INDEPENDENT
                    and not self.include_independent
                ):
                    continue
                relation = MarketRelation(
                    id=None,
                    market_id_a=market_a.market_id,
                    market_id_b=market_b.market_id,
                    relation_type=detected.relation_type,
                    confidence=detected.confidence,
                    detected_at=detected_at,
                    metadata=detected.metadata,
                )
                relations.append(relation)
                factor_rows.extend(_factor_rows_for_relation(relation))

        if relations:
            await self.relation_store.insert_relations(relations)
        if factor_rows and self.factor_sink is not None:
            await self.factor_sink.write_factor_values(factor_rows)
        return len(relations)


def detect_market_relation(
    market_a: MarketRelationCandidate,
    market_b: MarketRelationCandidate,
    *,
    similarity_threshold: float = 0.42,
) -> DetectedMarketRelation:
    contradiction = _detect_contradiction(market_a, market_b)
    if contradiction is not None:
        return contradiction

    subset = _detect_subset(market_a, market_b)
    if subset is not None:
        return subset

    similarity = _tf_idf_cosine(market_a.text, market_b.text)
    if similarity >= similarity_threshold:
        return DetectedMarketRelation(
            relation_type=MarketRelationType.SIMILAR,
            confidence=similarity,
            metadata={"basis": "tfidf_cosine", "similarity": similarity},
        )

    return DetectedMarketRelation(
        relation_type=MarketRelationType.INDEPENDENT,
        confidence=1.0 - similarity,
        metadata={"basis": "low_similarity", "similarity": similarity},
    )


def _rank_by_volume(
    candidates: Sequence[MarketRelationCandidate],
) -> list[MarketRelationCandidate]:
    return sorted(candidates, key=lambda candidate: candidate.volume_24h or 0.0, reverse=True)


def _factor_rows_for_relation(relation: MarketRelation) -> list[FactorValueRow]:
    rows: list[FactorValueRow] = []
    factor_values: list[tuple[str, float]] = []
    if relation.relation_type is MarketRelationType.SUBSET:
        factor_values.append(("subset_relation", relation.confidence))
    elif relation.relation_type is MarketRelationType.CONTRADICTION:
        factor_values.append(("contradiction_relation", relation.confidence))
        mispricing = relation.metadata.get("mispricing", 0.0)
        factor_values.append(("cross_market_mispricing", float(cast(float, mispricing))))
    elif relation.relation_type is MarketRelationType.SIMILAR:
        factor_values.append(("semantic_similarity", relation.confidence))

    for factor_id, value in factor_values:
        rows.append(
            FactorValueRow(
                factor_id=factor_id,
                param=relation.market_id_b,
                market_id=relation.market_id_a,
                ts=relation.detected_at,
                value=value,
            )
        )
        rows.append(
            FactorValueRow(
                factor_id=factor_id,
                param=relation.market_id_a,
                market_id=relation.market_id_b,
                ts=relation.detected_at,
                value=value,
            )
        )
    return rows


_TOKEN_RE = re.compile(r"[a-z0-9]+")
_MONTHS = {
    "january": 1,
    "february": 2,
    "march": 3,
    "april": 4,
    "may": 5,
    "june": 6,
    "july": 7,
    "august": 8,
    "september": 9,
    "october": 10,
    "november": 11,
    "december": 12,
}
_SUBSET_STOPWORDS = {
    "a",
    "an",
    "by",
    "during",
    "in",
    "the",
    "this",
    "will",
    "year",
}
_HIGH_DIRECTION = (
    "more than",
    "at least",
    "above",
    "over",
    "greater than",
)
_LOW_DIRECTION = (
    "less than",
    "below",
    "under",
    "fewer than",
)
_COMPARISON_RE = re.compile(
    r"\b("
    + "|".join(re.escape(term) for term in (*_HIGH_DIRECTION, *_LOW_DIRECTION))
    + r")\s+(\d+(?:\.\d+)?)\b"
)


def _detect_contradiction(
    market_a: MarketRelationCandidate,
    market_b: MarketRelationCandidate,
) -> DetectedMarketRelation | None:
    comparison_a = _comparison(market_a.text)
    comparison_b = _comparison(market_b.text)
    if comparison_a is None or comparison_b is None:
        return None
    direction_a, threshold_a, subject_a = comparison_a
    direction_b, threshold_b, subject_b = comparison_b
    if direction_a == direction_b or not math.isclose(threshold_a, threshold_b):
        return None
    subject_similarity = _jaccard(subject_a, subject_b)
    if subject_similarity < 0.75:
        return None
    if market_a.yes_price is None or market_b.yes_price is None:
        return None
    mispricing = (market_a.yes_price + market_b.yes_price) - 1.0
    if mispricing <= 0.0:
        return None
    return DetectedMarketRelation(
        relation_type=MarketRelationType.CONTRADICTION,
        confidence=1.0,
        metadata={
            "basis": "opposite_threshold_pricing_violation",
            "mispricing": mispricing,
            "threshold": threshold_a,
            "subject_similarity": subject_similarity,
        },
    )


def _comparison(text: str) -> tuple[str, float, set[str]] | None:
    normalized = text.lower()
    match = _COMPARISON_RE.search(normalized)
    if match is None:
        return None
    phrase = match.group(1)
    threshold = float(match.group(2))
    direction = "high" if phrase in _HIGH_DIRECTION else "low"
    without_comparison = (
        normalized[: match.start()] + normalized[match.end() :]
    ).strip()
    subject = {
        token
        for token in _tokens(without_comparison)
        if token
        not in {
            "dollar",
            "dollars",
            "percent",
            "point",
            "points",
            "basis",
        }
    }
    return direction, threshold, subject


def _detect_subset(
    market_a: MarketRelationCandidate,
    market_b: MarketRelationCandidate,
) -> DetectedMarketRelation | None:
    temporal = _detect_temporal_subset(market_a.text, market_b.text)
    if temporal is not None:
        return temporal

    tokens_a = _subset_terms(market_a.text)
    tokens_b = _subset_terms(market_b.text)
    if not tokens_a or not tokens_b or tokens_a == tokens_b:
        return None
    shorter, longer = (tokens_a, tokens_b) if len(tokens_a) < len(tokens_b) else (tokens_b, tokens_a)
    if shorter <= longer and len(shorter) >= 2:
        return DetectedMarketRelation(
            relation_type=MarketRelationType.SUBSET,
            confidence=0.9,
            metadata={
                "basis": "lexical_containment",
                "shorter_terms": sorted(shorter),
                "longer_terms": sorted(longer),
            },
        )
    return None


def _detect_temporal_subset(
    text_a: str,
    text_b: str,
) -> DetectedMarketRelation | None:
    month_a = _first_month(text_a)
    month_b = _first_month(text_b)
    if month_a is None and month_b is None:
        return None
    base_a = _terms_without_months(text_a)
    base_b = _terms_without_months(text_b)
    if _jaccard(base_a, base_b) < 0.8:
        return None
    return DetectedMarketRelation(
        relation_type=MarketRelationType.SUBSET,
        confidence=0.9,
        metadata={
            "basis": "temporal_containment",
            "month_a": month_a,
            "month_b": month_b,
        },
    )


def _tf_idf_cosine(text_a: str, text_b: str) -> float:
    tokens_a = _tokens(text_a)
    tokens_b = _tokens(text_b)
    if not tokens_a or not tokens_b:
        return 0.0
    counts_a = Counter(tokens_a)
    counts_b = Counter(tokens_b)
    terms = set(counts_a) | set(counts_b)
    weights_a: dict[str, float] = {}
    weights_b: dict[str, float] = {}
    for term in terms:
        document_frequency = int(term in counts_a) + int(term in counts_b)
        inverse_document_frequency = math.log(3.0 / (1.0 + document_frequency)) + 1.0
        weights_a[term] = counts_a[term] * inverse_document_frequency
        weights_b[term] = counts_b[term] * inverse_document_frequency
    return _cosine(weights_a, weights_b)


def _cosine(
    weights_a: Mapping[str, float],
    weights_b: Mapping[str, float],
) -> float:
    terms = set(weights_a) | set(weights_b)
    dot = sum(weights_a.get(term, 0.0) * weights_b.get(term, 0.0) for term in terms)
    norm_a = math.sqrt(sum(value * value for value in weights_a.values()))
    norm_b = math.sqrt(sum(value * value for value in weights_b.values()))
    if norm_a == 0.0 or norm_b == 0.0:
        return 0.0
    return dot / (norm_a * norm_b)


def _subset_terms(text: str) -> set[str]:
    return {
        token
        for token in _tokens(text)
        if token not in _SUBSET_STOPWORDS and token not in _MONTHS
    }


def _terms_without_months(text: str) -> set[str]:
    return {token for token in _subset_terms(text) if token not in _MONTHS}


def _first_month(text: str) -> int | None:
    for token in _tokens(text):
        month = _MONTHS.get(token)
        if month is not None:
            return month
    return None


def _tokens(text: str) -> list[str]:
    return _TOKEN_RE.findall(text.lower())


def _jaccard(terms_a: set[str], terms_b: set[str]) -> float:
    if not terms_a or not terms_b:
        return 0.0
    return len(terms_a & terms_b) / len(terms_a | terms_b)


__all__ = (
    "DetectedMarketRelation",
    "FactorValueSink",
    "MarketRelationCandidate",
    "MarketRelationCandidateSource",
    "MarketRelationService",
    "MarketRelationWriter",
    "PostgresFactorValueSink",
    "RELATION_FACTOR_IDS",
    "detect_market_relation",
)
