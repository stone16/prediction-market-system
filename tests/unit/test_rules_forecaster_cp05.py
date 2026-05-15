from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any

import pytest

from pms.controller.factor_snapshot import (
    FactorSnapshot,
    required_factor_keys,
    snapshot_factor_keys,
)
from pms.controller.forecasters.rules import RulesForecaster
from pms.core.models import MarketSignal
from pms.strategies.projections import FactorCompositionStep


NOW = datetime(2026, 5, 14, 5, 0, tzinfo=UTC)


def _signal(*, yes_price: float = 0.40) -> MarketSignal:
    return MarketSignal(
        market_id="rules-market",
        token_id="rules-token",
        venue="polymarket",
        title="Will rule deltas move the forecast?",
        yes_price=yes_price,
        volume_24h=1_000.0,
        resolves_at=datetime(2026, 6, 1, tzinfo=UTC),
        orderbook={"bids": [], "asks": []},
        external_signal={},
        fetched_at=NOW,
        market_status="open",
    )


def _rule(
    factor_id: str,
    *,
    weight: float = 1.0,
    required: bool = True,
    enabled: bool = True,
    threshold: float | None = None,
) -> FactorCompositionStep:
    return FactorCompositionStep(
        factor_id=factor_id,
        role="rule_delta",
        param="",
        weight=weight,
        threshold=threshold,
        required=required,
        enabled=enabled,
    )


class StaticFactorReader:
    def __init__(self, values: dict[tuple[str, str], float]) -> None:
        self.values = values
        self.calls: list[Sequence[FactorCompositionStep]] = []

    async def snapshot(
        self,
        *,
        market_id: str,
        as_of: datetime,
        required: Sequence[FactorCompositionStep],
        strategy_id: str,
        strategy_version_id: str,
    ) -> Any:
        del market_id, as_of, strategy_id, strategy_version_id
        self.calls.append(required)
        return FactorSnapshot(values=self.values, missing_factors=())


def test_factor_composition_step_enabled_defaults_true() -> None:
    step = FactorCompositionStep(
        factor_id="fair_value_spread",
        role="rule_delta",
        param="",
        weight=1.0,
        threshold=None,
    )

    assert step.enabled is True


def test_optional_rule_delta_is_fetched_without_becoming_required() -> None:
    required_rule = _rule("fair_value_spread", required=True)
    optional_rule = _rule("metaculus_prior", required=False)
    disabled_rule = _rule("favorite_longshot_bias", required=True, enabled=False)

    assert snapshot_factor_keys((required_rule, optional_rule, disabled_rule)) == (
        ("fair_value_spread", ""),
        ("metaculus_prior", ""),
    )
    assert required_factor_keys((required_rule, optional_rule, disabled_rule)) == (
        ("fair_value_spread", ""),
    )


@pytest.mark.asyncio
async def test_rules_forecaster_single_rule_moves_probability_from_market_price() -> None:
    reader = StaticFactorReader({("fair_value_spread", ""): 0.08})
    forecaster = RulesForecaster(
        factor_reader=reader,
        composition=(_rule("fair_value_spread", weight=0.5),),
    )

    result = await forecaster.apredict(_signal(yes_price=0.40))

    assert result == pytest.approx((0.44, 0.20, "rules-v1"))
    assert reader.calls


@pytest.mark.asyncio
async def test_rules_forecaster_multiple_rules_compose_additively() -> None:
    forecaster = RulesForecaster(
        factor_reader=StaticFactorReader(
            {
                ("fair_value_spread", ""): 0.05,
                ("metaculus_prior", ""): 0.60,
            }
        ),
        composition=(
            _rule("fair_value_spread", weight=0.4),
            _rule("metaculus_prior", weight=0.5),
        ),
    )

    result = await forecaster.apredict(_signal(yes_price=0.40))

    assert result == pytest.approx((0.52, 0.50, "rules-v1"))


@pytest.mark.asyncio
async def test_rules_forecaster_skips_disabled_rules() -> None:
    forecaster = RulesForecaster(
        factor_reader=StaticFactorReader(
            {
                ("fair_value_spread", ""): 0.0,
                ("metaculus_prior", ""): 0.95,
            }
        ),
        composition=(
            _rule("metaculus_prior", weight=1.0, enabled=False),
            _rule("fair_value_spread", weight=1.0),
        ),
    )

    result = await forecaster.apredict(_signal(yes_price=0.40))

    assert result == pytest.approx((0.40, 0.0, "rules-v1"))


@pytest.mark.asyncio
async def test_rules_forecaster_abstains_when_required_factor_missing() -> None:
    forecaster = RulesForecaster(
        factor_reader=StaticFactorReader({}),
        composition=(_rule("fair_value_spread", required=True),),
    )

    assert await forecaster.apredict(_signal()) is None


@pytest.mark.asyncio
async def test_rules_forecaster_optional_missing_factor_preserves_market_price() -> None:
    forecaster = RulesForecaster(
        factor_reader=StaticFactorReader({}),
        composition=(_rule("fair_value_spread", required=False),),
    )

    assert await forecaster.apredict(_signal(yes_price=0.42)) == pytest.approx(
        (0.42, 0.0, "rules-v1")
    )
