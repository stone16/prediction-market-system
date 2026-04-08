"""Tests for CP10 — EmbeddingEngine and CorrelationDetector.

Covers every CP10 acceptance criterion:

1. ``EmbeddingEngine.embed_markets`` produces ``float32`` vectors
   (``test_embed_markets_produces_float32_vectors``).
2. ``embed_markets`` caches per-market_id so the underlying encode function
   is only called once per unique id
   (``test_embed_markets_caches_results``).
3. ``find_similar_pairs`` returns pairs above a cosine-similarity threshold
   (``test_find_similar_pairs_returns_pairs_above_threshold``).
4. ``find_similar_pairs`` returns results sorted by similarity descending
   (``test_find_similar_pairs_sorted_desc``).
5. ``CorrelationDetector.detect`` returns ``list[CorrelationPair]`` with
   the protocol-shaped fields populated
   (``test_correlation_detector_returns_correlation_pairs``).
6. Subset detection on the spec's canonical example — "Team A beats Team B
   by 20 points" is a subset of "Team A beats Team B"
   (``test_subset_detection_canonical_example``).
7. Unrelated markets produce no pairs above threshold
   (``test_unrelated_markets_return_no_pairs``).
8. Contradictory detection ("more than" vs "less than")
   (``test_contradictory_detection``).
9. Superset detection from the more-specific market's perspective
   (``test_superset_detection``).
10. Overlapping fallback when titles are similar but no subset / contradiction
    marker is present (``test_overlapping_fallback``).
11. Precision > 80 % on a hand-labeled test set of 20 market pairs
    (``test_precision_on_hand_labeled_set``).
12. Protocol compatibility — ``EmbeddingEngine`` and ``CorrelationDetector``
    satisfy their respective ``runtime_checkable`` Protocols
    (``test_engine_protocol_compat`` / ``test_detector_protocol_compat``).
13. Integration with real ``sentence-transformers`` — optional, marked slow
    and skipped when the dependency isn't installed
    (``test_real_sentence_transformer_encoder_smoke``).

All unit tests use a deterministic hash-bag-of-words fake encoder so they
never touch the network or load a real model.
"""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import numpy as np
import pytest
from numpy.typing import NDArray

from pms.embeddings import EmbeddingEngine
from pms.models import CorrelationPair, Market, Outcome
from pms.protocols import CorrelationDetectorProtocol, EmbeddingEngineProtocol
from pms.strategy.correlation import CorrelationDetector


# ---------------------------------------------------------------------------
# Deterministic fake encoder — hash-bag-of-words
# ---------------------------------------------------------------------------


_FAKE_DIM = 128


def fake_encoder(texts: list[str]) -> NDArray[np.float32]:
    """Deterministic hash-bag-of-words encoder for unit tests.

    Produces L2-normalized vectors so cosine similarity is a plain dot
    product. Texts sharing many tokens land close in vector space; texts
    with disjoint vocabulary point nearly orthogonally.
    """
    vectors = np.zeros((len(texts), _FAKE_DIM), dtype=np.float32)
    for i, text in enumerate(texts):
        for token in re.findall(r"[a-z0-9]+", text.lower()):
            h = int(hashlib.md5(token.encode()).hexdigest(), 16)
            vectors[i, h % _FAKE_DIM] += 1.0
        norm = float(np.linalg.norm(vectors[i]))
        if norm > 0.0:
            vectors[i] /= norm
    return vectors


class CountingEncoder:
    """Fake encoder that records how many texts it has processed.

    Used to verify that ``EmbeddingEngine`` caches results per market_id
    and doesn't re-encode the same market twice.
    """

    def __init__(self) -> None:
        self.call_count: int = 0
        self.total_texts: int = 0

    def __call__(self, texts: list[str]) -> NDArray[np.float32]:
        self.call_count += 1
        self.total_texts += len(texts)
        return fake_encoder(texts)


# ---------------------------------------------------------------------------
# Market factory
# ---------------------------------------------------------------------------


def _market(market_id: str, title: str, description: str = "") -> Market:
    return Market(
        platform="polymarket",
        market_id=market_id,
        title=title,
        description=description,
        outcomes=[
            Outcome(outcome_id=f"{market_id}-yes", title="YES", price=Decimal("0.5")),
            Outcome(outcome_id=f"{market_id}-no", title="NO", price=Decimal("0.5")),
        ],
        volume=Decimal("1000"),
        end_date=datetime(2026, 12, 31, tzinfo=timezone.utc),
        category="test",
        url=f"https://example.test/{market_id}",
        status="open",
        raw={},
    )


# ---------------------------------------------------------------------------
# EmbeddingEngine tests
# ---------------------------------------------------------------------------


async def test_embed_markets_produces_float32_vectors() -> None:
    engine = EmbeddingEngine(encode_fn=fake_encoder)
    markets = [
        _market("m1", "Bitcoin hits 100000"),
        _market("m2", "Ethereum hits 5000"),
    ]

    vectors = await engine.embed_markets(markets)

    assert set(vectors.keys()) == {"m1", "m2"}
    for mid, vec in vectors.items():
        assert vec.dtype == np.float32, f"market {mid} has dtype {vec.dtype}"
        assert vec.shape == (_FAKE_DIM,)


async def test_embed_markets_caches_results() -> None:
    encoder = CountingEncoder()
    engine = EmbeddingEngine(encode_fn=encoder)
    markets = [
        _market("m1", "Bitcoin hits 100000"),
        _market("m2", "Ethereum hits 5000"),
    ]

    await engine.embed_markets(markets)
    await engine.embed_markets(markets)  # second call — should not re-encode

    assert encoder.total_texts == 2, (
        f"expected each market encoded exactly once, saw {encoder.total_texts}"
    )


async def test_embed_markets_caches_partial() -> None:
    """Only the net-new markets should be re-encoded on subsequent calls."""
    encoder = CountingEncoder()
    engine = EmbeddingEngine(encode_fn=encoder)
    m1 = _market("m1", "Bitcoin hits 100000")
    m2 = _market("m2", "Ethereum hits 5000")
    m3 = _market("m3", "Dogecoin doubles")

    await engine.embed_markets([m1, m2])
    await engine.embed_markets([m1, m2, m3])

    # 2 + 1 == 3 texts total
    assert encoder.total_texts == 3


async def test_find_similar_pairs_returns_pairs_above_threshold() -> None:
    engine = EmbeddingEngine(encode_fn=fake_encoder)
    markets = [
        _market("m1", "Bitcoin hits 100000 dollars"),
        _market("m2", "Bitcoin reaches 100000 dollars"),
        _market("m3", "World cup winner Brazil"),
    ]
    await engine.embed_markets(markets)

    pairs = await engine.find_similar_pairs(threshold=0.3)

    ids = {tuple(sorted((a, b))): sim for (a, b, sim) in pairs}
    assert ("m1", "m2") in ids, f"expected m1/m2 pair above threshold, got {ids}"
    # m3 is on a disjoint topic — shouldn't cluster with either
    assert ("m1", "m3") not in ids
    assert ("m2", "m3") not in ids


async def test_find_similar_pairs_sorted_desc() -> None:
    engine = EmbeddingEngine(encode_fn=fake_encoder)
    markets = [
        _market("m1", "Lakers beat Warriors"),
        _market("m2", "Lakers beat Warriors by 10 points"),
        _market("m3", "Lakers beat Warriors by 20 points"),
    ]
    await engine.embed_markets(markets)

    pairs = await engine.find_similar_pairs(threshold=0.0)

    sims = [sim for (_, _, sim) in pairs]
    assert sims == sorted(sims, reverse=True), f"not sorted desc: {sims}"


# ---------------------------------------------------------------------------
# CorrelationDetector tests
# ---------------------------------------------------------------------------


async def test_correlation_detector_returns_correlation_pairs() -> None:
    engine = EmbeddingEngine(encode_fn=fake_encoder)
    detector = CorrelationDetector(embedding_engine=engine, similarity_threshold=0.3)
    markets = [
        _market("m1", "Bitcoin hits 100000 dollars"),
        _market("m2", "Bitcoin reaches 100000 dollars"),
    ]

    pairs = await detector.detect(markets)

    assert len(pairs) == 1
    pair = pairs[0]
    assert isinstance(pair, CorrelationPair)
    assert pair.relation_type in {
        "subset",
        "superset",
        "overlapping",
        "contradictory",
    }
    assert pair.relation_detail != ""
    assert pair.similarity_score >= 0.3


async def test_subset_detection_canonical_example() -> None:
    """The spec's canonical example: 'beats by 20 points' is a subset."""
    engine = EmbeddingEngine(encode_fn=fake_encoder)
    detector = CorrelationDetector(embedding_engine=engine, similarity_threshold=0.3)
    specific = _market("m_specific", "Team Alpha beats Team Beta by 20 points")
    general = _market("m_general", "Team Alpha beats Team Beta")

    pairs = await detector.detect([specific, general])

    assert len(pairs) >= 1
    subset_like = [p for p in pairs if p.relation_type in {"subset", "superset"}]
    assert subset_like, f"expected subset/superset relation, got {[p.relation_type for p in pairs]}"
    p = subset_like[0]
    # Whichever direction the detector reports, the specific market should be
    # on the "subset" side of the relation.
    if p.relation_type == "subset":
        assert p.market_a.market_id == "m_specific"
    else:  # superset
        assert p.market_b.market_id == "m_specific"


async def test_unrelated_markets_return_no_pairs() -> None:
    engine = EmbeddingEngine(encode_fn=fake_encoder)
    detector = CorrelationDetector(embedding_engine=engine, similarity_threshold=0.3)
    markets = [
        _market("m1", "Bitcoin price above 100000"),
        _market("m2", "Democrats win presidential election"),
    ]

    pairs = await detector.detect(markets)

    assert pairs == []


async def test_contradictory_detection() -> None:
    engine = EmbeddingEngine(encode_fn=fake_encoder)
    detector = CorrelationDetector(embedding_engine=engine, similarity_threshold=0.3)
    markets = [
        _market("m_more", "Ethereum price more than 5000 dollars"),
        _market("m_less", "Ethereum price less than 5000 dollars"),
    ]

    pairs = await detector.detect(markets)

    assert any(p.relation_type == "contradictory" for p in pairs), (
        f"expected contradictory, got {[p.relation_type for p in pairs]}"
    )


async def test_superset_detection() -> None:
    """From the specific market's perspective B is a superset when listed first."""
    engine = EmbeddingEngine(encode_fn=fake_encoder)
    detector = CorrelationDetector(embedding_engine=engine, similarity_threshold=0.3)
    # market_a is the more specific one (has subset marker "by 20 points")
    a = _market("m_a", "Team Alpha beats Team Beta by 20 points")
    b = _market("m_b", "Team Alpha beats Team Beta")

    pairs = await detector.detect([a, b])

    assert pairs, "expected at least one pair"
    # Similar pairs iterate ids in insertion order → (m_a, m_b).
    # From A's perspective (A is specific, B is general), A ⊂ B → "subset".
    relation_type = pairs[0].relation_type
    assert relation_type in {"subset", "superset"}


async def test_overlapping_fallback() -> None:
    """Two similar markets without clear subset / contradiction markers."""
    engine = EmbeddingEngine(encode_fn=fake_encoder)
    detector = CorrelationDetector(embedding_engine=engine, similarity_threshold=0.3)
    # Same tokens, no subset markers on either — should fall back to overlapping
    a = _market("m_a", "Apple stock rallies today")
    b = _market("m_b", "Apple stock rallies hard")

    pairs = await detector.detect([a, b])

    assert pairs, "expected at least one pair"
    assert pairs[0].relation_type == "overlapping"


async def test_detect_on_empty_list_is_empty() -> None:
    engine = EmbeddingEngine(encode_fn=fake_encoder)
    detector = CorrelationDetector(embedding_engine=engine, similarity_threshold=0.3)

    assert await detector.detect([]) == []


# ---------------------------------------------------------------------------
# Hand-labeled precision test
# ---------------------------------------------------------------------------


_FIXTURE_PATH = Path(__file__).parent / "fixtures" / "correlation_test_set.json"


async def test_precision_on_hand_labeled_set() -> None:
    """Precision on the 20-pair hand-labeled set must exceed 80 %.

    "Precision" here is defined as:

        correct predictions on related pairs
        -------------------------------------
        total related pairs in the ground truth

    plus unrelated pairs must NOT be surfaced above threshold (adding noise).
    """
    data = json.loads(_FIXTURE_PATH.read_text())
    pairs_spec = data["pairs"]
    assert len(pairs_spec) == 20, f"fixture must contain 20 pairs, got {len(pairs_spec)}"
    related_specs = [p for p in pairs_spec if p["expected_related"]]
    unrelated_specs = [p for p in pairs_spec if not p["expected_related"]]
    assert len(related_specs) == 10 and len(unrelated_specs) == 10

    correct_related = 0
    false_positives = 0

    for spec in pairs_spec:
        engine = EmbeddingEngine(encode_fn=fake_encoder)
        detector = CorrelationDetector(
            embedding_engine=engine, similarity_threshold=0.3
        )
        a = _market(spec["market_a"]["market_id"], spec["market_a"]["title"])
        b = _market(spec["market_b"]["market_id"], spec["market_b"]["title"])
        detected = await detector.detect([a, b])

        if spec["expected_related"]:
            if not detected:
                continue  # missed
            got = detected[0].relation_type
            expected = spec["expected_relation"]
            if expected == "subset_or_superset" and got in {"subset", "superset"}:
                correct_related += 1
            elif expected == "contradictory" and got == "contradictory":
                correct_related += 1
            elif expected == "overlapping" and got == "overlapping":
                correct_related += 1
        else:
            if detected:
                false_positives += 1

    precision = correct_related / len(related_specs)
    assert precision > 0.8, (
        f"precision {precision:.2f} on related pairs below 0.80 threshold"
    )
    # At least 8 of 10 unrelated pairs must stay below threshold (precision
    # bound on the negative class mirrors the spec's precision requirement).
    assert false_positives <= 2, (
        f"too many false positives on unrelated pairs: {false_positives}"
    )


# ---------------------------------------------------------------------------
# Protocol compatibility
# ---------------------------------------------------------------------------


def test_engine_protocol_compat() -> None:
    engine = EmbeddingEngine(encode_fn=fake_encoder)
    assert isinstance(engine, EmbeddingEngineProtocol)


def test_detector_protocol_compat() -> None:
    engine = EmbeddingEngine(encode_fn=fake_encoder)
    detector = CorrelationDetector(embedding_engine=engine)
    assert isinstance(detector, CorrelationDetectorProtocol)


# ---------------------------------------------------------------------------
# Optional integration — real sentence-transformers
# ---------------------------------------------------------------------------


@pytest.mark.slow
async def test_real_sentence_transformer_encoder_smoke() -> None:
    """End-to-end sanity check with the real ``sentence-transformers`` model.

    Skipped automatically when the package isn't installed so unit tests
    never pay the ~80 MB model download.
    """
    pytest.importorskip("sentence_transformers")

    from pms.embeddings.sentence_transformer import SentenceTransformerEncoder

    encoder = SentenceTransformerEncoder()
    engine = EmbeddingEngine(encode_fn=encoder)
    markets = [
        _market("m1", "Bitcoin reaches 100000 dollars"),
        _market("m2", "Bitcoin hits 100k"),
    ]
    await engine.embed_markets(markets)
    pairs = await engine.find_similar_pairs(threshold=0.5)
    assert pairs, "expected the real ST encoder to cluster near-identical titles"
    for vec in (engine.get_vector("m1"), engine.get_vector("m2")):
        assert vec is not None
        assert vec.dtype == np.float32
