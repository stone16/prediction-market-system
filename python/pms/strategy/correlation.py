"""Correlation detector — rule-based relation classification on top of
embedding similarity (CP10).

``CorrelationDetector`` implements ``CorrelationDetectorProtocol`` (CP01)
in two stages:

1. **Candidate generation.** Delegate to ``EmbeddingEngine`` to embed
   every input market and surface pairs above a configurable cosine
   similarity threshold. This collapses the O(N²) problem into a short
   list of likely-related markets without a language model.
2. **Rule-based classification.** For each candidate pair, inspect the
   titles to decide whether the pair is a subset, superset, contradictory
   or simply overlapping. Rules look for keyword overlap plus specific
   refinement markers ("by", "more than", "at least", …). This stays
   deterministic, explainable, and LLM-free — CP10 explicitly makes LLM
   refinement optional.

Keeping classification rule-based has two big wins over an LLM:

* **Determinism.** Tests run with no external calls and no stochastic
  model output, so the precision-target test over the hand-labeled
  fixture is reproducible on every CI run.
* **Cost & latency.** The pipeline calls this on every tick; a rule
  scan is microseconds while an LLM round-trip would be seconds and
  dollars per million invocations.

The rules are intentionally conservative — when in doubt, fall back to
``"overlapping"`` rather than emitting a wrong specific label. The
arbitrage strategy (CP07) only acts on ``"subset"``, so misclassifying a
legitimate subset as overlapping just silences a trade, while
misclassifying overlap as subset could trigger a bad order.
"""

from __future__ import annotations

import re

from pms.embeddings.engine import EmbeddingEngine
from pms.models import CorrelationPair, Market, RelationType

# Markers suggesting the containing title is a refinement of a broader claim.
# Ordered by informativeness only for readability — membership is a set op.
_SUBSET_MARKERS: tuple[str, ...] = (
    "by ",
    "more than",
    "less than",
    "at least",
    "at most",
    "exactly",
    "in the first",
    "in the last",
    "within",
    "before",
    "after",
    " points",
    " goals",
    "%",
    " percent",
    "basis points",
)

# Upper-bound and lower-bound quantifier phrases used by
# ``_has_contradiction``. Kept separate from ``_SUBSET_MARKERS`` because
# only a subset of the refinement markers actually carry a direction.
_UPPER_BOUND_MARKERS: tuple[str, ...] = ("more than", "at least", "above", "over")
_LOWER_BOUND_MARKERS: tuple[str, ...] = ("less than", "at most", "below", "under")

# Stopwords stripped before token-set comparison. A hand-curated list is
# enough for CP10's short market titles — pulling nltk just for this would
# be overkill and would add a runtime dependency.
_STOPWORDS: frozenset[str] = frozenset(
    {
        "a",
        "an",
        "and",
        "are",
        "as",
        "at",
        "be",
        "been",
        "being",
        "but",
        "by",
        "for",
        "from",
        "if",
        "in",
        "is",
        "it",
        "its",
        "not",
        "of",
        "on",
        "or",
        "that",
        "the",
        "then",
        "these",
        "this",
        "those",
        "to",
        "was",
        "were",
        "will",
        "with",
    }
)


class CorrelationDetector:
    """Detects cross-market relationships via embeddings + rule classification."""

    def __init__(
        self,
        embedding_engine: EmbeddingEngine,
        similarity_threshold: float = 0.6,
    ) -> None:
        self._engine: EmbeddingEngine = embedding_engine
        self._threshold: float = similarity_threshold

    async def detect(self, markets: list[Market]) -> list[CorrelationPair]:
        """Return every detected correlation pair above the threshold.

        Returns an empty list when the input has fewer than two markets
        or when no candidate pair clears the similarity threshold.
        """
        if len(markets) < 2:
            return []

        await self._engine.embed_markets(markets)
        similar = await self._engine.find_similar_pairs(self._threshold)

        by_id: dict[str, Market] = {m.market_id: m for m in markets}

        results: list[CorrelationPair] = []
        for id_a, id_b, sim in similar:
            market_a = by_id.get(id_a)
            market_b = by_id.get(id_b)
            if market_a is None or market_b is None:
                continue

            relation_type, detail = _classify_relation(market_a, market_b)
            results.append(
                CorrelationPair(
                    market_a=market_a,
                    market_b=market_b,
                    similarity_score=sim,
                    relation_type=relation_type,
                    relation_detail=detail,
                    arbitrage_opportunity=None,
                )
            )

        return results


# ---------------------------------------------------------------------------
# Rule-based classification
# ---------------------------------------------------------------------------


def _classify_relation(
    market_a: Market, market_b: Market
) -> tuple[RelationType, str]:
    """Classify the relationship between two similar markets.

    Rules (first match wins):

    1. **Contradictory.** Both titles reference opposing quantifier
       directions ("more than" vs "less than") on the same underlying
       claim. Checked first because numeric contradiction is the
       strongest signal and beats the subset heuristics.
    2. **Subset.** ``market_a``'s significant tokens are a superset of
       ``market_b``'s AND only ``market_a`` carries a refinement marker
       ("by", "at least", …). Interpreted as "A is a narrower version of
       B" → A ⊂ B → ``subset``.
    3. **Superset.** Mirror of rule 2 with a and b swapped → B is the
       narrower market → from A's perspective it's a superset.
    4. **Overlapping.** Fallback when similarity said "related" but no
       structural rule fires.
    """
    title_a = market_a.title.lower()
    title_b = market_b.title.lower()

    if _has_contradiction(title_a, title_b):
        detail = (
            f"'{market_a.title}' and '{market_b.title}' assert opposing "
            f"quantifier directions"
        )
        return "contradictory", detail

    tokens_a = _significant_tokens(title_a)
    tokens_b = _significant_tokens(title_b)
    a_has_marker = _has_subset_marker(title_a)
    b_has_marker = _has_subset_marker(title_b)

    # Rule 2: A is a narrower refinement of B
    if (
        tokens_b
        and tokens_b.issubset(tokens_a)
        and a_has_marker
        and not b_has_marker
    ):
        detail = f"'{market_a.title}' is a subset of '{market_b.title}'"
        return "subset", detail

    # Rule 3: B is a narrower refinement of A, from A's perspective
    if (
        tokens_a
        and tokens_a.issubset(tokens_b)
        and b_has_marker
        and not a_has_marker
    ):
        detail = f"'{market_a.title}' is a superset of '{market_b.title}'"
        return "superset", detail

    detail = f"'{market_a.title}' overlaps '{market_b.title}'"
    return "overlapping", detail


def _significant_tokens(text: str) -> set[str]:
    """Extract lowercase alphanumeric tokens with stopwords stripped."""
    words = re.findall(r"[a-z0-9]+", text.lower())
    return {w for w in words if w not in _STOPWORDS}


def _has_subset_marker(title: str) -> bool:
    return any(marker in title for marker in _SUBSET_MARKERS)


def _has_contradiction(title_a: str, title_b: str) -> bool:
    """True when the two titles assert opposing quantifier directions.

    Requires the pair to share at least one significant token so we don't
    flag completely unrelated markets just because they happen to include
    "more than" and "less than" somewhere.
    """
    # NOTE: v1 limitation (review-loop f8 — kept as documented limitation,
    # not fixed). Contradiction detection here is keyword-only: we look for
    # "more than" / "less than" / "at least" / "at most" markers and an
    # overlapping significant token between the two titles. We do *not*
    # parse the numeric bounds in either title, so e.g. "ETH > 5000" and
    # "ETH < 7000" are flagged contradictory even though both can be
    # simultaneously true (the feasible interval [5000, 7000] is
    # non-empty). Properly detecting numeric contradictions requires
    # parsing each title into a half-open interval and checking the
    # intersection — typically an LLM-assisted refinement step. CP10
    # explicitly makes LLM refinement optional, and the rule-based
    # classifier is still hitting >80 % precision on the hand-labeled
    # fixture, which is the spec's acceptance bar. A post-v1 checkpoint
    # will add an interval-aware refinement layer behind a feature flag.
    a_tokens = _significant_tokens(title_a)
    b_tokens = _significant_tokens(title_b)
    if not (a_tokens & b_tokens):
        return False

    a_upper = any(marker in title_a for marker in _UPPER_BOUND_MARKERS)
    a_lower = any(marker in title_a for marker in _LOWER_BOUND_MARKERS)
    b_upper = any(marker in title_b for marker in _UPPER_BOUND_MARKERS)
    b_lower = any(marker in title_b for marker in _LOWER_BOUND_MARKERS)

    return (a_upper and b_lower) or (a_lower and b_upper)
