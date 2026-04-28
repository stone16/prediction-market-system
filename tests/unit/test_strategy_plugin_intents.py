from __future__ import annotations

from collections.abc import Sequence
from dataclasses import FrozenInstanceError, is_dataclass, replace
from datetime import UTC, datetime
from typing import Any, cast, get_type_hints

import pytest

from pms.core.enums import TimeInForce
from pms.strategies.base import StrategyModule, StrategyObservationSource
from pms.strategies.intents import (
    BasketIntent,
    StrategyCandidate,
    StrategyContext,
    StrategyJudgement,
    StrategyObservation,
    TradeIntent,
)
from pms.strategies.registry import StrategyModuleRegistry


NOW = datetime(2026, 4, 28, 8, 0, tzinfo=UTC)


class FakeStrategyModule:
    strategy_id = "ripple"
    strategy_version_id = "ripple-v1"

    async def run(
        self,
        context: StrategyContext,
    ) -> Sequence[TradeIntent | BasketIntent]:
        del context
        return ()


def _trade_intent(**overrides: object) -> TradeIntent:
    data: dict[str, object] = {
        "intent_id": "intent-1",
        "strategy_id": "ripple",
        "strategy_version_id": "ripple-v1",
        "candidate_id": "candidate-1",
        "market_id": "market-1",
        "token_id": "token-yes",
        "venue": "polymarket",
        "side": "BUY",
        "outcome": "YES",
        "limit_price": 0.54,
        "notional_usdc": 25.0,
        "expected_price": 0.62,
        "expected_edge": 0.08,
        "max_slippage_bps": 50,
        "time_in_force": TimeInForce.GTC,
        "evidence_refs": ("judgement-1",),
        "created_at": NOW,
    }
    data.update(overrides)
    return TradeIntent(**cast(Any, data))


def _judgement(**overrides: object) -> StrategyJudgement:
    data: dict[str, object] = {
        "judgement_id": "judgement-1",
        "candidate_id": "candidate-1",
        "strategy_id": "ripple",
        "strategy_version_id": "ripple-v1",
        "approved": True,
        "confidence": 0.75,
        "rationale": "fixture evidence supports approval",
        "evidence_refs": ("obs-1",),
        "failure_reasons": (),
        "created_at": NOW,
    }
    data.update(overrides)
    return StrategyJudgement(**cast(Any, data))


def _basket(*legs: TradeIntent, **overrides: object) -> BasketIntent:
    data: dict[str, object] = {
        "basket_id": "basket-1",
        "strategy_id": "ripple",
        "strategy_version_id": "ripple-v1",
        "legs": legs or (_trade_intent(intent_id="leg-a"), _trade_intent(intent_id="leg-b")),
        "execution_policy": "manual_review",
        "evidence_refs": ("judgement-1",),
        "created_at": NOW,
    }
    data.update(overrides)
    return BasketIntent(**cast(Any, data))


def test_strategy_plugin_value_objects_are_frozen_and_float_at_boundary() -> None:
    for value_object in (
        StrategyContext,
        StrategyObservation,
        StrategyCandidate,
        StrategyJudgement,
        TradeIntent,
        BasketIntent,
    ):
        assert is_dataclass(value_object)
        assert cast(Any, value_object).__dataclass_params__.frozen

    hints = get_type_hints(TradeIntent)
    assert hints["limit_price"] is float
    assert hints["notional_usdc"] is float
    assert hints["expected_price"] is float
    assert hints["expected_edge"] is float

    with pytest.raises(FrozenInstanceError):
        setattr(_trade_intent(), "notional_usdc", 50.0)


def test_trade_intent_requires_strategy_identity_and_valid_prices() -> None:
    assert _trade_intent().strategy_version_id == "ripple-v1"
    with pytest.raises(ValueError, match="strategy_id"):
        _trade_intent(strategy_id="")
    with pytest.raises(ValueError, match="limit_price"):
        replace(_trade_intent(), intent_id="bad-price", limit_price=1.0)


@pytest.mark.parametrize("confidence", [-0.01, 1.01])
def test_strategy_judgement_rejects_invalid_confidence_bounds(
    confidence: float,
) -> None:
    with pytest.raises(ValueError, match="confidence"):
        _judgement(confidence=confidence)


def test_strategy_judgement_requires_rejection_reasons_only_for_rejections() -> None:
    assert _judgement(approved=False, confidence=0.25, failure_reasons=("x",))
    with pytest.raises(ValueError, match="failure_reasons"):
        _judgement(approved=False, confidence=0.25, failure_reasons=())
    with pytest.raises(ValueError, match="approved"):
        _judgement(approved=True, failure_reasons=("contradiction",))


def test_basket_intent_validates_policy_and_strategy_identity() -> None:
    leg_a = _trade_intent(intent_id="leg-a")
    leg_b = _trade_intent(intent_id="leg-b")

    assert _basket(leg_a, leg_b, execution_policy="all_or_none").legs == (leg_a, leg_b)
    with pytest.raises(ValueError, match="empty"):
        _basket(legs=())
    with pytest.raises(ValueError, match="single_leg_use_trade_intent"):
        _basket(leg_a, execution_policy="single_leg_use_trade_intent")
    with pytest.raises(ValueError, match="mixed strategy identity"):
        _basket(leg_a, _trade_intent(intent_id="leg-c", strategy_id="other"))
    with pytest.raises(ValueError, match="execution_policy"):
        _basket(leg_a, leg_b, execution_policy=cast(Any, "venue_atomic"))


def test_strategy_registry_registers_and_requires_module_identity() -> None:
    module: StrategyModule = FakeStrategyModule()
    registry = StrategyModuleRegistry([module])

    assert registry.get("ripple", "ripple-v1") is module
    assert registry.get("missing", "missing-v1") is None
    with pytest.raises(KeyError, match="missing"):
        registry.require("missing", "missing-v1")
    with pytest.raises(ValueError, match="already registered"):
        registry.register(module)


def test_strategy_observation_source_is_documented_as_plugin_local() -> None:
    assert StrategyObservationSource.__doc__ is not None
    assert "plugin-local" in StrategyObservationSource.__doc__
    assert "core Sensor" in StrategyObservationSource.__doc__
