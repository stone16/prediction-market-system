"""Tests for the LLM correlation refinement layer (Phase 3C).

These tests cover the refiner's pass-through and reclassify behaviour
plus the JSON-parsing robustness of the response parser. The
Anthropic-SDK-backed provider is **not** exercised against the real
API — instead a deterministic ``_FakeClassifier`` injected into the
refiner returns canned responses, so the unit suite stays offline and
never burns tokens.

The Anthropic provider's class-level entry points (``_extract_text``,
``_parse_classification_json``) are exercised directly with synthetic
inputs so the JSON parsing path keeps test coverage even though the
SDK call itself is never made.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import pytest

from pms.models import CorrelationPair, Market, Outcome, RelationType
from pms.strategy.llm_correlation import (
    AnthropicCorrelationClassifier,
    LLMClassifierProtocol,
    LLMCorrelationRefiner,
    _parse_classification_json,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _market(
    market_id: str = "m-1",
    title: str = "Will ETH be > $5000 on 2026-12-31?",
) -> Market:
    return Market(
        platform="polymarket",
        market_id=market_id,
        title=title,
        description="",
        outcomes=[
            Outcome(outcome_id="yes", title="Yes", price=Decimal("0.50")),
            Outcome(outcome_id="no", title="No", price=Decimal("0.50")),
        ],
        volume=Decimal("100"),
        end_date=datetime(2026, 12, 31, tzinfo=timezone.utc),
        category="crypto",
        url="https://example.com",
        status="active",
        raw={},
    )


def _pair(
    relation_type: RelationType = "contradictory",
    detail: str = "rule-based detail",
    market_a_title: str = "Will ETH > $5000 by 2026-12-31?",
    market_b_title: str = "Will ETH < $7000 by 2026-12-31?",
) -> CorrelationPair:
    return CorrelationPair(
        market_a=_market("m-a", market_a_title),
        market_b=_market("m-b", market_b_title),
        similarity_score=0.85,
        relation_type=relation_type,
        relation_detail=detail,
        arbitrage_opportunity=None,
    )


class _FakeClassifier:
    """Test double satisfying :class:`LLMClassifierProtocol`."""

    def __init__(
        self,
        verdict: tuple[RelationType, str] = ("overlapping", "LLM said overlapping"),
    ) -> None:
        self.verdict = verdict
        self.calls: list[tuple[str, str]] = []

    async def classify_pair(
        self, market_a: Market, market_b: Market
    ) -> tuple[RelationType, str]:
        self.calls.append((market_a.title, market_b.title))
        return self.verdict


class _RaisingClassifier:
    """Test double that always raises — exercises the fallback path."""

    async def classify_pair(
        self, market_a: Market, market_b: Market
    ) -> tuple[RelationType, str]:
        raise RuntimeError("LLM is on fire")


# ---------------------------------------------------------------------------
# Refiner happy path: refine only the configured relation types
# ---------------------------------------------------------------------------


async def test_refiner_replaces_contradictory_pair_with_llm_verdict() -> None:
    fake = _FakeClassifier(verdict=("overlapping", "intervals can both hold"))
    refiner = LLMCorrelationRefiner(classifier=fake)
    pairs = [_pair(relation_type="contradictory", detail="rules said contradictory")]

    refined = await refiner.refine(pairs)

    assert len(refined) == 1
    assert refined[0].relation_type == "overlapping"
    assert refined[0].relation_detail == "intervals can both hold"
    # The original input is unmodified.
    assert pairs[0].relation_type == "contradictory"
    # The LLM was called exactly once with the pair's titles.
    assert len(fake.calls) == 1


async def test_refiner_passes_through_non_contradictory_pairs() -> None:
    """By default the refiner only touches contradictory pairs — subset,
    superset, overlapping, and independent must pass through untouched."""
    fake = _FakeClassifier(verdict=("contradictory", "should never appear"))
    refiner = LLMCorrelationRefiner(classifier=fake)
    pairs = [
        _pair(relation_type="subset", detail="rules said subset"),
        _pair(relation_type="superset", detail="rules said superset"),
        _pair(relation_type="overlapping", detail="rules said overlapping"),
        _pair(relation_type="independent", detail="rules said independent"),
    ]

    refined = await refiner.refine(pairs)

    assert len(refined) == 4
    assert [p.relation_type for p in refined] == [
        "subset",
        "superset",
        "overlapping",
        "independent",
    ]
    assert [p.relation_detail for p in refined] == [
        "rules said subset",
        "rules said superset",
        "rules said overlapping",
        "rules said independent",
    ]
    # The fake classifier was never called for any of these.
    assert fake.calls == []


async def test_refiner_honors_custom_refine_relations() -> None:
    """``refine_relations`` lets callers route additional types through the LLM."""
    fake = _FakeClassifier(verdict=("subset", "LLM tightened the label"))
    refiner = LLMCorrelationRefiner(
        classifier=fake, refine_relations=("overlapping",)
    )
    pairs = [
        _pair(relation_type="overlapping", detail="rules said overlapping"),
        _pair(relation_type="contradictory", detail="rules said contradictory"),
    ]

    refined = await refiner.refine(pairs)

    # Only overlapping was rerouted; contradictory passed through.
    assert refined[0].relation_type == "subset"
    assert refined[1].relation_type == "contradictory"
    assert len(fake.calls) == 1


async def test_refiner_falls_back_to_original_label_on_llm_error() -> None:
    """A classifier exception must not crash the refiner — preserve the
    original rule-based pair so a transient LLM error never blanks out
    the result list."""
    refiner = LLMCorrelationRefiner(classifier=_RaisingClassifier())
    pairs = [_pair(relation_type="contradictory", detail="rules said contradictory")]

    refined = await refiner.refine(pairs)

    assert len(refined) == 1
    assert refined[0].relation_type == "contradictory"
    assert refined[0].relation_detail == "rules said contradictory"


async def test_refiner_handles_empty_input() -> None:
    refiner = LLMCorrelationRefiner(classifier=_FakeClassifier())
    refined = await refiner.refine([])
    assert refined == []


# ---------------------------------------------------------------------------
# Response parser
# ---------------------------------------------------------------------------


def test_parse_classification_json_happy_path() -> None:
    relation, detail = _parse_classification_json(
        '{"relation": "overlapping", "detail": "intervals can both hold"}'
    )
    assert relation == "overlapping"
    assert detail == "intervals can both hold"


def test_parse_classification_json_tolerates_preamble() -> None:
    """LLMs sometimes prepend prose; the parser must locate the JSON anyway."""
    text = (
        "Sure, here's the classification:\n"
        '{"relation": "subset", "detail": "A is more specific"}\n'
        "Hope that helps!"
    )
    relation, detail = _parse_classification_json(text)
    assert relation == "subset"
    assert detail == "A is more specific"


def test_parse_classification_json_unknown_relation_falls_back() -> None:
    """An unrecognised relation label collapses to the safe ``overlapping`` default."""
    relation, detail = _parse_classification_json(
        '{"relation": "ambiguous", "detail": "unsure"}'
    )
    assert relation == "overlapping"
    assert "ambiguous" in detail


def test_parse_classification_json_invalid_json_falls_back() -> None:
    relation, detail = _parse_classification_json(
        "{this is not valid JSON, the LLM hallucinated"
    )
    assert relation == "overlapping"
    assert detail


def test_parse_classification_json_empty_input_falls_back() -> None:
    relation, detail = _parse_classification_json("")
    assert relation == "overlapping"
    assert "empty" in detail.lower()


def test_parse_classification_json_non_mapping_root_falls_back() -> None:
    relation, detail = _parse_classification_json('["overlapping"]')
    assert relation == "overlapping"
    # The fallback message is generated because no JSON object regex matched.
    assert detail


def test_parse_classification_json_non_string_detail_coerced() -> None:
    relation, detail = _parse_classification_json(
        '{"relation": "subset", "detail": 42}'
    )
    assert relation == "subset"
    assert detail == ""


# ---------------------------------------------------------------------------
# Anthropic provider — text extraction (no real SDK call)
# ---------------------------------------------------------------------------


def test_anthropic_extract_text_concatenates_text_blocks() -> None:
    class _Block:
        def __init__(self, type: str, text: str = "") -> None:
            self.type = type
            self.text = text

    class _Response:
        content = [
            _Block("text", "Hello "),
            _Block("tool_use"),  # ignored
            _Block("text", "world"),
        ]

    text = AnthropicCorrelationClassifier._extract_text(_Response())
    assert text == "Hello world"


def test_anthropic_extract_text_handles_no_content() -> None:
    class _Empty:
        content = None

    assert AnthropicCorrelationClassifier._extract_text(_Empty()) == ""


# ---------------------------------------------------------------------------
# Anthropic provider — classify_pair end to end with a stub client
# ---------------------------------------------------------------------------


async def test_anthropic_classify_pair_with_stub_client() -> None:
    """Inject a stub client implementing ``messages.create`` so the
    classifier round-trips against deterministic SDK output without
    importing or invoking the real anthropic package."""
    captured: dict[str, Any] = {}

    class _Block:
        def __init__(self, text: str) -> None:
            self.type = "text"
            self.text = text

    class _Response:
        def __init__(self, text: str) -> None:
            self.content = [_Block(text)]

    class _Messages:
        async def create(self, **kwargs: Any) -> _Response:
            captured.update(kwargs)
            return _Response(
                '{"relation": "overlapping", "detail": "stub said overlapping"}'
            )

    class _StubClient:
        messages = _Messages()

    classifier = AnthropicCorrelationClassifier(client=_StubClient())
    relation, detail = await classifier.classify_pair(
        _market("m-a", "Will ETH > $5000?"),
        _market("m-b", "Will ETH < $7000?"),
    )

    assert relation == "overlapping"
    assert detail == "stub said overlapping"
    # The stub captured the SDK kwargs — verify the system + user prompt
    # plumbing wired through.
    assert captured["model"]  # model id passed
    assert "logician" in captured["system"]  # system prompt
    user_msg = captured["messages"][0]
    assert user_msg["role"] == "user"
    assert "ETH" in user_msg["content"]


def test_anthropic_classifier_raises_helpful_error_when_sdk_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If the anthropic package is not installed AND no client is injected,
    instantiation must raise an ImportError that names the install command."""
    import builtins

    real_import = builtins.__import__

    def fake_import(
        name: str, globals: Any = None, locals: Any = None, fromlist: Any = (), level: int = 0
    ) -> Any:
        if name == "anthropic":
            raise ImportError("No module named 'anthropic'")
        return real_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", fake_import)

    with pytest.raises(ImportError, match=r"pms\[llm\]"):
        AnthropicCorrelationClassifier()  # no client → triggers the lazy import
