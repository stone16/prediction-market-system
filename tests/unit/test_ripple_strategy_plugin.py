from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast
import tomllib

import pytest

from pms.core.enums import TimeInForce
from pms.core.models import OrderState, TradeDecision
from pms.strategies.base import (
    StrategyAgent,
    StrategyController,
    StrategyModule,
    StrategyObservationSource,
)
from pms.strategies.intents import (
    StrategyCandidate,
    StrategyContext,
    StrategyJudgement,
    StrategyObservation,
    TradeIntent,
)
from pms.strategies.ripple.agent import RippleAgent
from pms.strategies.ripple.controller import RippleController
from pms.strategies.ripple.source import RippleObservationFixture, RippleObservationSource
from pms.strategies.ripple.strategy import RippleStrategyModule


NOW = datetime(2026, 4, 28, 12, 0, tzinfo=UTC)
ROOT = Path(__file__).resolve().parents[2]


def _context() -> StrategyContext:
    return StrategyContext(
        strategy_id="ripple",
        strategy_version_id="ripple-v1",
        as_of=NOW,
    )


def _fixture(**overrides: object) -> RippleObservationFixture:
    data: dict[str, object] = {
        "observation_id": "ripple-observation-1",
        "market_id": "market-ripple-1",
        "title": "Will the deterministic ripple fixture resolve YES?",
        "thesis": "Fixture evidence supports a YES edge.",
        "probability_estimate": 0.63,
        "expected_edge": 0.09,
        "confidence": 0.78,
        "token_id": "token-ripple-yes",
        "limit_price": 0.56,
        "notional_usdc": 25.0,
        "expected_price": 0.64,
        "max_slippage_bps": 40,
        "evidence_refs": ("doc://ripple/a", "doc://ripple/b"),
        "contradiction_refs": (),
    }
    data.update(overrides)
    return RippleObservationFixture(**cast(Any, data))


async def _candidate_from_fixture(
    fixture: RippleObservationFixture,
) -> StrategyCandidate:
    context = _context()
    observations = await RippleObservationSource([fixture]).observe(context)
    candidates = await RippleController().propose(context, observations)
    assert len(candidates) == 1
    return candidates[0]


def test_ripple_components_satisfy_strategy_protocols() -> None:
    source: StrategyObservationSource = RippleObservationSource([_fixture()])
    controller: StrategyController = RippleController()
    agent: StrategyAgent = RippleAgent()
    module: StrategyModule = RippleStrategyModule(
        source=source,
        controller=controller,
        agent=agent,
        strategy_id="ripple",
        strategy_version_id="ripple-v1",
    )

    assert module.strategy_id == "ripple"
    assert module.strategy_version_id == "ripple-v1"


@pytest.mark.asyncio
async def test_ripple_strategy_approves_fixture_candidate_and_emits_trade_intent() -> None:
    module = RippleStrategyModule(
        source=RippleObservationSource([_fixture()]),
        controller=RippleController(),
        agent=RippleAgent(),
        strategy_id="ripple",
        strategy_version_id="ripple-v1",
    )

    intents = await module.run(_context())

    assert len(intents) == 1
    intent = intents[0]
    assert isinstance(intent, TradeIntent)
    assert not isinstance(intent, TradeDecision)
    assert not isinstance(intent, OrderState)
    assert intent.strategy_id == "ripple"
    assert intent.strategy_version_id == "ripple-v1"
    assert intent.candidate_id == "candidate-ripple-observation-1"
    assert intent.token_id == "token-ripple-yes"
    assert intent.time_in_force is TimeInForce.GTC


@pytest.mark.parametrize(
    ("fixture", "expected_reason"),
    [
        (
            _fixture(evidence_refs=("doc://ripple/only-one",)),
            "insufficient_evidence",
        ),
        (
            _fixture(contradiction_refs=("doc://ripple/contradiction",)),
            "contradiction",
        ),
        (
            _fixture(confidence=0.42),
            "low_confidence",
        ),
    ],
)
@pytest.mark.asyncio
async def test_ripple_agent_emits_typed_rejections_without_trade_intents(
    fixture: RippleObservationFixture,
    expected_reason: str,
) -> None:
    context = _context()
    candidate = await _candidate_from_fixture(fixture)
    agent = RippleAgent(min_evidence_refs=2, min_confidence=0.6)

    judgement = await agent.judge(context, candidate)
    intents = await agent.build_intents(context, candidate, judgement)

    assert isinstance(judgement, StrategyJudgement)
    assert judgement.approved is False
    assert judgement.failure_reasons == (expected_reason,)
    assert judgement.strategy_id == "ripple"
    assert judgement.strategy_version_id == "ripple-v1"
    assert intents == ()


@pytest.mark.asyncio
async def test_ripple_observation_source_and_controller_parse_fixture_payload() -> None:
    context = _context()
    source = RippleObservationSource([_fixture(observation_id="obs-custom")])

    observations = await source.observe(context)
    candidates = await RippleController().propose(context, observations)

    assert len(observations) == 1
    observation = observations[0]
    assert isinstance(observation, StrategyObservation)
    assert observation.observation_id == "obs-custom"
    assert observation.source == "ripple-fixture"
    assert observation.evidence_refs == ("doc://ripple/a", "doc://ripple/b")
    assert len(candidates) == 1
    assert candidates[0].candidate_id == "candidate-obs-custom"
    assert candidates[0].metadata["token_id"] == "token-ripple-yes"


def test_ripple_agent_and_prompts_are_fixture_driven_only() -> None:
    checked_paths = [
        ROOT / "src" / "pms" / "strategies" / "ripple" / "agent.py",
        ROOT / "src" / "pms" / "strategies" / "ripple" / "prompts.py",
    ]
    forbidden_fragments = (
        "anthropic",
        "openai",
        "ollama",
        "litellm",
        "py_clob_client_v2",
    )

    for path in checked_paths:
        text = path.read_text(encoding="utf-8").lower()
        for fragment in forbidden_fragments:
            assert fragment not in text


def test_ripple_import_linter_contract_is_declared() -> None:
    pyproject = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    contracts: Sequence[dict[str, object]] = pyproject["tool"]["importlinter"][
        "contracts"
    ]
    contract = next(
        candidate
        for candidate in contracts
        if candidate["name"] == "Strategy plugins: no actuator, controller, or venue adapter imports"
    )

    assert contract["type"] == "forbidden"
    assert contract["source_modules"] == ["pms.strategies.ripple"]
    assert contract["forbidden_modules"] == [
        "pms.actuator",
        "pms.actuator.adapters",
        "pms.controller",
        "py_clob_client_v2",
    ]
