from __future__ import annotations

from collections.abc import Sequence
from dataclasses import FrozenInstanceError, is_dataclass, replace
from datetime import UTC, datetime
from typing import get_type_hints

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


def _trade_intent(
    *,
    intent_id: str = "intent-1",
    strategy_id: str = "ripple",
    strategy_version_id: str = "ripple-v1",
) -> TradeIntent:
    return TradeIntent(
        intent_id=intent_id,
        strategy_id=strategy_id,
        strategy_version_id=strategy_version_id,
        candidate_id="candidate-1",
        market_id="market-1",
        token_id="token-yes",
        venue="polymarket",
        side="BUY",
        outcome="YES",
        limit_price=0.54,
        notional_usdc=25.0,
        expected_price=0.62,
        expected_edge=0.08,
        max_slippage_bps=50,
        time_in_force=TimeInForce.GTC,
        evidence_refs=("judgement-1",),
        created_at=NOW,
    )


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
        assert value_object.__dataclass_params__.frozen

    hints = get_type_hints(TradeIntent)
    assert hints["limit_price"] is float
    assert hints["notional_usdc"] is float
    assert hints["expected_price"] is float
    assert hints["expected_edge"] is float

    intent = _trade_intent()
    with pytest.raises(FrozenInstanceError):
        setattr(intent, "notional_usdc", 50.0)


def test_trade_intent_requires_strategy_identity_and_valid_prices() -> None:
    intent = _trade_intent()

    assert intent.strategy_id == "ripple"
    assert intent.strategy_version_id == "ripple-v1"

    with pytest.raises(ValueError, match="strategy_id"):
        _trade_intent(strategy_id="")

    with pytest.raises(ValueError, match="limit_price"):
        replace(_trade_intent(), intent_id="bad-price", limit_price=1.0)


@pytest.mark.parametrize("confidence", [-0.01, 1.01])
def test_strategy_judgement_rejects_invalid_confidence_bounds(
    confidence: float,
) -> None:
    with pytest.raises(ValueError, match="confidence"):
        StrategyJudgement(
            judgement_id="judgement-1",
            candidate_id="candidate-1",
            strategy_id="ripple",
            strategy_version_id="ripple-v1",
            approved=True,
            confidence=confidence,
            rationale="bounds check",
            evidence_refs=("obs-1",),
            failure_reasons=(),
            created_at=NOW,
        )


def test_strategy_judgement_requires_rejection_reasons_only_for_rejections() -> None:
    rejected = StrategyJudgement(
        judgement_id="judgement-1",
        candidate_id="candidate-1",
        strategy_id="ripple",
        strategy_version_id="ripple-v1",
        approved=False,
        confidence=0.25,
        rationale="insufficient evidence",
        evidence_refs=("obs-1",),
        failure_reasons=("insufficient_evidence",),
        created_at=NOW,
    )

    assert rejected.failure_reasons == ("insufficient_evidence",)

    with pytest.raises(ValueError, match="failure_reasons"):
        StrategyJudgement(
            judgement_id="judgement-2",
            candidate_id="candidate-1",
            strategy_id="ripple",
            strategy_version_id="ripple-v1",
            approved=False,
            confidence=0.25,
            rationale="missing reason",
            evidence_refs=("obs-1",),
            failure_reasons=(),
            created_at=NOW,
        )

    with pytest.raises(ValueError, match="approved"):
        StrategyJudgement(
            judgement_id="judgement-3",
            candidate_id="candidate-1",
            strategy_id="ripple",
            strategy_version_id="ripple-v1",
            approved=True,
            confidence=0.75,
            rationale="approval should not carry rejection reasons",
            evidence_refs=("obs-1",),
            failure_reasons=("contradiction",),
            created_at=NOW,
        )


def test_basket_intent_validates_policy_and_strategy_identity() -> None:
    leg_a = _trade_intent(intent_id="leg-a")
    leg_b = _trade_intent(intent_id="leg-b")

    basket = BasketIntent(
        basket_id="basket-1",
        strategy_id="ripple",
        strategy_version_id="ripple-v1",
        legs=(leg_a, leg_b),
        execution_policy="manual_review",
        evidence_refs=("judgement-1",),
        created_at=NOW,
    )

    assert basket.legs == (leg_a, leg_b)

    with pytest.raises(ValueError, match="empty"):
        BasketIntent(
            basket_id="basket-empty",
            strategy_id="ripple",
            strategy_version_id="ripple-v1",
            legs=(),
            execution_policy="manual_review",
            evidence_refs=(),
            created_at=NOW,
        )

    with pytest.raises(ValueError, match="single_leg_use_trade_intent"):
        BasketIntent(
            basket_id="basket-single",
            strategy_id="ripple",
            strategy_version_id="ripple-v1",
            legs=(leg_a,),
            execution_policy="single_leg_use_trade_intent",
            evidence_refs=(),
            created_at=NOW,
        )

    with pytest.raises(ValueError, match="mixed strategy identity"):
        BasketIntent(
            basket_id="basket-mixed",
            strategy_id="ripple",
            strategy_version_id="ripple-v1",
            legs=(leg_a, _trade_intent(intent_id="leg-c", strategy_id="other")),
            execution_policy="manual_review",
            evidence_refs=(),
            created_at=NOW,
        )

    with pytest.raises(ValueError, match="execution_policy"):
        BasketIntent(
            basket_id="basket-policy",
            strategy_id="ripple",
            strategy_version_id="ripple-v1",
            legs=(leg_a, leg_b),
            execution_policy="venue_atomic",
            evidence_refs=(),
            created_at=NOW,
        )


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
