from __future__ import annotations

import asyncio
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
import logging
import math
import re
from typing import Any, Protocol, cast

from pms.core.models import MarketRelation, MarketRelationType
from pms.factors.base import FactorValueRow

RELATION_FACTOR_IDS = (
    "subset_relation",
    "contradiction_relation",
    "semantic_similarity",
    "cross_market_mispricing",
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class MarketRelationCandidate:
    market_id: str
    title: str
    yes_price: float | None
    volume_24h: float | None = None
    rules: str | None = None

    @property
    def text(self) -> str:
        return self.title if self.rules is None else f"{self.title} {self.rules}"


@dataclass(frozen=True, slots=True)
class DetectedMarketRelation:
    relation_type: MarketRelationType
    confidence: float
    metadata: Mapping[str, Any] = field(default_factory=dict)


class MarketRelationCandidateSource(Protocol):
    async def read_relation_candidates(
        self, *, limit: int
    ) -> Sequence[MarketRelationCandidate]: ...


class MarketRelationWriter(Protocol):
    async def insert_relations(self, relations: Sequence[MarketRelation]) -> None: ...


class FactorValueSink(Protocol):
    async def write_factor_values(self, rows: Sequence[FactorValueRow]) -> None: ...


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

    async def run(self) -> None:
        while True:
            try:
                await self.compute_once()
            except Exception:
                logger.exception("market relation detection cycle failed")
            await asyncio.sleep(self.interval.total_seconds())

    async def compute_once(self) -> int:
        raw = await self.market_source.read_relation_candidates(limit=self.max_markets)
        candidates = sorted(raw, key=lambda row: row.volume_24h or 0.0, reverse=True)[
            : self.max_markets
        ]
        detected_at = self.clock()
        relations: list[MarketRelation] = []
        rows: list[FactorValueRow] = []
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
                    None,
                    market_a.market_id,
                    market_b.market_id,
                    detected.relation_type,
                    detected.confidence,
                    detected_at,
                    detected.metadata,
                )
                relations.append(relation)
                rows.extend(_factor_rows(relation))
        if relations:
            await self.relation_store.insert_relations(relations)
        if rows and self.factor_sink is not None:
            await self.factor_sink.write_factor_values(rows)
        return len(relations)


def detect_market_relation(
    market_a: MarketRelationCandidate,
    market_b: MarketRelationCandidate,
    *,
    similarity_threshold: float = 0.42,
) -> DetectedMarketRelation:
    return (
        _contradiction(market_a, market_b)
        or _subset(market_a, market_b)
        or _similarity_relation(market_a, market_b, similarity_threshold)
    )


def _similarity_relation(
    market_a: MarketRelationCandidate,
    market_b: MarketRelationCandidate,
    threshold: float,
) -> DetectedMarketRelation:
    similarity = _tfidf_cosine(market_a.text, market_b.text)
    relation_type = (
        MarketRelationType.SIMILAR
        if similarity >= threshold
        else MarketRelationType.INDEPENDENT
    )
    return DetectedMarketRelation(
        relation_type=relation_type,
        confidence=similarity if relation_type is MarketRelationType.SIMILAR else 1.0 - similarity,
        metadata={"basis": "tfidf_cosine" if relation_type is MarketRelationType.SIMILAR else "low_similarity", "similarity": similarity},
    )


def _factor_rows(relation: MarketRelation) -> list[FactorValueRow]:
    values: list[tuple[str, float]] = []
    if relation.relation_type is MarketRelationType.SUBSET:
        values.append(("subset_relation", relation.confidence))
    elif relation.relation_type is MarketRelationType.SIMILAR:
        values.append(("semantic_similarity", relation.confidence))
    elif relation.relation_type is MarketRelationType.CONTRADICTION:
        values.append(("contradiction_relation", relation.confidence))
        values.append(("cross_market_mispricing", float(cast(float, relation.metadata.get("mispricing", 0.0)))))
    return [
        FactorValueRow(
            factor_id=factor_id, param=other_id, market_id=market_id, ts=relation.detected_at, value=value
        )
        for factor_id, value in values
        for market_id, other_id in (
            (relation.market_id_a, relation.market_id_b),
            (relation.market_id_b, relation.market_id_a),
        )
    ]


_TOKEN_RE = re.compile(r"[a-z0-9]+")
_MONTHS = {month: index for index, month in enumerate("january february march april may june july august september october november december".split(), start=1)}
_STOP = {"a", "an", "by", "during", "in", "the", "this", "will", "year"}
_UNITS = {"basis", "dollar", "dollars", "percent", "point", "points"}
_HIGH = ("more than", "at least", "above", "over", "greater than")
_LOW = ("less than", "below", "under", "fewer than")
_COMPARISON_RE = re.compile(r"\b(" + "|".join(re.escape(term) for term in (*_HIGH, *_LOW)) + r")\s+(\d+(?:\.\d+)?)\b")


def _contradiction(
    market_a: MarketRelationCandidate,
    market_b: MarketRelationCandidate,
) -> DetectedMarketRelation | None:
    parsed_a = _comparison(market_a.text)
    parsed_b = _comparison(market_b.text)
    if parsed_a is None or parsed_b is None:
        return None
    direction_a, threshold_a, subject_a = parsed_a
    direction_b, threshold_b, subject_b = parsed_b
    if direction_a == direction_b or not math.isclose(threshold_a, threshold_b):
        return None
    if market_a.yes_price is None or market_b.yes_price is None:
        return None
    subject_similarity = _jaccard(subject_a, subject_b)
    mispricing = market_a.yes_price + market_b.yes_price - 1.0
    if subject_similarity < 0.75 or mispricing <= 0.0:
        return None
    return DetectedMarketRelation(
        MarketRelationType.CONTRADICTION,
        1.0,
        {"basis": "opposite_threshold_pricing_violation", "mispricing": mispricing, "threshold": threshold_a, "subject_similarity": subject_similarity},
    )


def _comparison(text: str) -> tuple[str, float, set[str]] | None:
    match = _COMPARISON_RE.search(text.lower())
    if match is None:
        return None
    direction = "high" if match.group(1) in _HIGH else "low"
    stripped = f"{text[: match.start()]} {text[match.end() :]}"
    subject = {token for token in _tokens(stripped) if token not in _UNITS}
    return direction, float(match.group(2)), subject


def _subset(
    market_a: MarketRelationCandidate,
    market_b: MarketRelationCandidate,
) -> DetectedMarketRelation | None:
    months = (_first_month(market_a.text), _first_month(market_b.text))
    base_a = _terms(market_a.text)
    base_b = _terms(market_b.text)
    if any(month is not None for month in months) and _jaccard(base_a, base_b) >= 0.8:
        return DetectedMarketRelation(MarketRelationType.SUBSET, 0.9, {"basis": "temporal_containment", "month_a": months[0], "month_b": months[1]})
    if not base_a or not base_b or base_a == base_b:
        return None
    shorter, longer = (base_a, base_b) if len(base_a) < len(base_b) else (base_b, base_a)
    if len(shorter) >= 2 and shorter <= longer:
        return DetectedMarketRelation(
            MarketRelationType.SUBSET, 0.9, {"basis": "lexical_containment", "shorter_terms": sorted(shorter), "longer_terms": sorted(longer)}
        )
    return None


def _tfidf_cosine(text_a: str, text_b: str) -> float:
    counts_a = Counter(_tokens(text_a))
    counts_b = Counter(_tokens(text_b))
    if not counts_a or not counts_b:
        return 0.0
    weights: list[dict[str, float]] = []
    for counts in (counts_a, counts_b):
        weights.append(
            {
                term: count * (math.log(3.0 / (1.0 + int(term in counts_a) + int(term in counts_b))) + 1.0)
                for term, count in counts.items()
            }
        )
    dot = sum(weights[0].get(term, 0.0) * weights[1].get(term, 0.0) for term in set(weights[0]) | set(weights[1]))
    norms = [math.sqrt(sum(value * value for value in side.values())) for side in weights]
    return 0.0 if 0.0 in norms else dot / (norms[0] * norms[1])


def _terms(text: str) -> set[str]:
    return {token for token in _tokens(text) if token not in _STOP and token not in _MONTHS}


def _first_month(text: str) -> int | None:
    return next((_MONTHS[token] for token in _tokens(text) if token in _MONTHS), None)


def _tokens(text: str) -> list[str]:
    return _TOKEN_RE.findall(text.lower())


def _jaccard(terms_a: set[str], terms_b: set[str]) -> float:
    return 0.0 if not terms_a or not terms_b else len(terms_a & terms_b) / len(terms_a | terms_b)
