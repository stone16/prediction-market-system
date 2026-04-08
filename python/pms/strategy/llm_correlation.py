"""LLM-based refinement layer over the rule-based CorrelationDetector (Phase 3C).

The v1 :class:`pms.strategy.correlation.CorrelationDetector` is
intentionally rule-based: it uses keyword matching plus token-set
operations to classify market pairs as ``subset``, ``superset``,
``overlapping``, ``contradictory`` or ``independent``. The trade-off
documented in that module is that **contradiction detection is
keyword-only** — pairs like ``"ETH > 5000"`` and ``"ETH < 7000"`` are
flagged contradictory by the rules even though both can simultaneously
be true (the feasible interval ``[5000, 7000]`` is non-empty). The v1
spec marks LLM refinement as optional and defers it to Phase 3.

This module is the deferred refinement layer. It takes the output of
the rule-based detector and reclassifies a configurable subset of
pairs (by default, just the ``contradictory`` ones, since that's the
documented weakness) by routing them through an LLM. The LLM client is
hidden behind a tiny :class:`LLMClassifierProtocol` so:

* The default provider — :class:`AnthropicCorrelationClassifier` —
  uses the official ``anthropic`` SDK, which is an **optional**
  dependency installed via ``uv pip install 'pms[llm]'``. The import
  is lazy so the rest of ``pms.strategy`` does not need anthropic
  installed to import.
* Tests inject a fake implementation that returns canned responses,
  so the unit suite never makes a real API call (and never burns
  tokens / hits rate limits in CI).

Determinism note
----------------

Even with a low ``temperature``, LLM output is not byte-stable across
runs. The refiner therefore returns a *new* :class:`CorrelationPair`
list rather than mutating the input — callers that need
reproducibility for backtests should cache the refined output keyed
by the input pair's ``(market_a.market_id, market_b.market_id)``.
The refiner does not do its own caching to keep the surface
explicitly stateless.
"""

from __future__ import annotations

import logging
from dataclasses import replace
from typing import Any, Protocol, runtime_checkable

from pms.models import CorrelationPair, Market, RelationType

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Provider protocol
# ---------------------------------------------------------------------------


@runtime_checkable
class LLMClassifierProtocol(Protocol):
    """Minimal interface a refinement provider must implement.

    Implementations may be backed by any LLM SDK (Anthropic, OpenAI,
    local Ollama, …) or a deterministic stub for testing. The contract
    is intentionally narrow — return a refined ``RelationType`` and a
    short human-readable explanation that the refiner stamps onto the
    new :class:`CorrelationPair`.
    """

    async def classify_pair(
        self, market_a: Market, market_b: Market
    ) -> tuple[RelationType, str]:
        """Return a refined ``(relation_type, detail)`` for the pair."""
        ...


#: The default set of relation types the refiner will route through
#: the LLM. ``contradictory`` is the v1 documented weakness; the other
#: rule-based labels (subset/superset/overlapping/independent) are
#: high-precision and not worth the LLM cost by default.
DEFAULT_REFINE_RELATIONS: tuple[RelationType, ...] = ("contradictory",)


# ---------------------------------------------------------------------------
# Refiner
# ---------------------------------------------------------------------------


class LLMCorrelationRefiner:
    """Refines selected correlation pairs by routing them through an LLM.

    The refiner is stateless and pure async. Pairs whose
    ``relation_type`` is **not** in ``refine_relations`` pass through
    unchanged so the LLM cost stays bounded to the cases that actually
    need refinement.
    """

    def __init__(
        self,
        classifier: LLMClassifierProtocol,
        refine_relations: tuple[RelationType, ...] = DEFAULT_REFINE_RELATIONS,
    ) -> None:
        self._classifier: LLMClassifierProtocol = classifier
        self._refine_relations: frozenset[RelationType] = frozenset(refine_relations)

    async def refine(
        self, pairs: list[CorrelationPair]
    ) -> list[CorrelationPair]:
        """Return a new list of pairs with eligible ones reclassified.

        Eligibility is determined by ``refine_relations``. The refiner
        catches per-pair classifier failures and falls back to the
        original rule-based label, so a transient LLM error never
        blanks out the whole result list — the failure is logged at
        WARNING and the original pair is preserved.
        """
        refined: list[CorrelationPair] = []
        for pair in pairs:
            if pair.relation_type not in self._refine_relations:
                refined.append(pair)
                continue
            try:
                new_type, new_detail = await self._classifier.classify_pair(
                    pair.market_a, pair.market_b
                )
            except Exception as exc:  # noqa: BLE001 — refiner must not crash
                logger.warning(
                    "LLM classifier failed for pair (%s, %s); "
                    "falling back to rule-based label %s: %s",
                    pair.market_a.market_id,
                    pair.market_b.market_id,
                    pair.relation_type,
                    exc,
                )
                refined.append(pair)
                continue
            refined.append(
                replace(pair, relation_type=new_type, relation_detail=new_detail)
            )
        return refined


# ---------------------------------------------------------------------------
# Anthropic SDK provider (optional dependency)
# ---------------------------------------------------------------------------


#: Default model id used by ``AnthropicCorrelationClassifier``. Haiku is
#: the cheapest current Claude family member; refinement is a small,
#: well-bounded classification task and does not benefit from Sonnet/Opus
#: depth — keeping the cost low matters more than the small accuracy
#: bump on a task this narrow.
DEFAULT_ANTHROPIC_MODEL: str = "claude-haiku-4-5-20251001"


class AnthropicCorrelationClassifier:
    """Anthropic SDK-backed implementation of :class:`LLMClassifierProtocol`.

    Lazy-imports the ``anthropic`` package so the rest of
    :mod:`pms.strategy` works without it installed. Installing the
    optional dep is the price of using this provider:

    .. code-block:: bash

        uv pip install 'pms[llm]'

    Tests should never use this class directly — they should inject a
    deterministic fake implementing :class:`LLMClassifierProtocol`
    instead so the unit suite stays offline.
    """

    SYSTEM_PROMPT: str = (
        "You are a logician helping classify whether two prediction-market "
        "questions are logically related. Return one of: subset, superset, "
        "overlapping, contradictory, independent. A pair is contradictory "
        "ONLY when both claims cannot simultaneously be true. Pairs with "
        "overlapping numeric intervals (e.g. ETH > 5000 and ETH < 7000) "
        "are NOT contradictory — both can be true in [5000, 7000]. Reply "
        "with strict JSON: {\"relation\": \"<label>\", \"detail\": \"<one-sentence rationale>\"}."
    )

    def __init__(
        self,
        client: Any = None,
        model: str = DEFAULT_ANTHROPIC_MODEL,
        max_tokens: int = 200,
    ) -> None:
        if client is None:
            try:
                import anthropic  # type: ignore[import-not-found]
            except ImportError as exc:
                raise ImportError(
                    "Phase 3C LLM correlation refinement requires the "
                    "anthropic SDK. Install with: uv pip install 'pms[llm]'"
                ) from exc
            client = anthropic.AsyncAnthropic()
        self._client = client
        self._model = model
        self._max_tokens = max_tokens

    async def classify_pair(
        self, market_a: Market, market_b: Market
    ) -> tuple[RelationType, str]:
        """Call the Anthropic API and parse a JSON response into the protocol shape."""
        user_prompt = (
            f"Question A: {market_a.title}\n"
            f"Question B: {market_b.title}\n\n"
            "Classify the logical relationship between these two questions."
        )
        response = await self._client.messages.create(
            model=self._model,
            max_tokens=self._max_tokens,
            system=self.SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_prompt}],
        )
        text = self._extract_text(response)
        return _parse_classification_json(text)

    @staticmethod
    def _extract_text(response: Any) -> str:
        """Pull the assistant text out of an Anthropic ``messages.create`` response.

        The shape is ``response.content`` → a list of content blocks
        each with a ``type`` and (for text blocks) a ``text`` field.
        We concatenate every text block, ignoring tool-use or other
        block kinds the SDK might return in future.
        """
        content = getattr(response, "content", None) or []
        chunks: list[str] = []
        for block in content:
            block_type = getattr(block, "type", None)
            if block_type == "text":
                chunks.append(getattr(block, "text", "") or "")
        return "".join(chunks).strip()


# ---------------------------------------------------------------------------
# Response parsing
# ---------------------------------------------------------------------------


_VALID_RELATIONS: frozenset[str] = frozenset(
    {"subset", "superset", "overlapping", "contradictory", "independent"}
)


def _parse_classification_json(text: str) -> tuple[RelationType, str]:
    """Parse the LLM's JSON response into a ``(RelationType, detail)`` tuple.

    Tolerates leading/trailing prose around the JSON object since LLMs
    sometimes wrap the JSON in commentary even when asked not to.
    Falls back to ``("overlapping", text)`` when parsing fails — the
    overlapping label is the rule-based detector's safe default and
    keeps a malformed LLM response from collapsing into an error.
    """
    import json
    import re

    if not text:
        return "overlapping", "LLM returned empty response"

    # Find the first JSON object in the response — accommodates LLMs
    # that prepend a "Sure, here's the classification:" preamble.
    match = re.search(r"\{[^{}]*\}", text, re.DOTALL)
    if match is None:
        return "overlapping", f"LLM returned non-JSON response: {text[:200]}"

    try:
        payload = json.loads(match.group(0))
    except json.JSONDecodeError:
        return "overlapping", f"LLM returned invalid JSON: {text[:200]}"

    if not isinstance(payload, dict):
        return "overlapping", f"LLM JSON is not a mapping: {text[:200]}"

    relation_raw = payload.get("relation")
    detail = payload.get("detail", "")
    if not isinstance(relation_raw, str) or relation_raw not in _VALID_RELATIONS:
        return "overlapping", (
            f"LLM returned unrecognised relation {relation_raw!r}; "
            f"falling back to overlapping"
        )
    if not isinstance(detail, str):
        detail = ""

    # mypy: relation_raw has been narrowed to one of the literal values.
    relation: RelationType = relation_raw  # type: ignore[assignment]
    return relation, detail
