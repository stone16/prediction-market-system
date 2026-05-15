from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any

import pytest

from pms.controller.factor_snapshot import FactorSnapshot
from pms.controller.factory import ControllerPipelineFactory
from pms.controller.forecasters.statistical import StatisticalForecaster
from pms.config import PMSSettings
from pms.core.enums import RunMode
from pms.core.models import MarketSignal
from pms.strategies.paper_multifactor import build_paper_multi_factor_strategy
from pms.strategies.projections import FactorCompositionStep


NOW = datetime(2026, 5, 14, 6, 0, tzinfo=UTC)


def _signal(*, yes_price: float = 0.40) -> MarketSignal:
    return MarketSignal(
        market_id="stat-market",
        token_id="stat-token",
        venue="polymarket",
        title="Will statistical fusion move the forecast?",
        yes_price=yes_price,
        volume_24h=1_000.0,
        resolves_at=datetime(2026, 6, 1, tzinfo=UTC),
        orderbook={"bids": [], "asks": []},
        external_signal={},
        fetched_at=NOW,
        market_status="open",
    )


def _step(
    factor_id: str,
    *,
    role: str = "weighted",
    weight: float = 1.0,
    required: bool = True,
) -> FactorCompositionStep:
    return FactorCompositionStep(
        factor_id=factor_id,
        role=role,
        param="",
        weight=weight,
        threshold=None,
        required=required,
    )


class StaticFactorReader:
    def __init__(
        self,
        values: dict[tuple[str, str], float],
        *,
        missing_factors: tuple[tuple[str, str], ...] = (),
    ) -> None:
        self.values = values
        self.missing_factors = missing_factors
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
        return FactorSnapshot(
            values=self.values,
            missing_factors=self.missing_factors,
        )


@pytest.mark.asyncio
async def test_statistical_forecaster_single_probability_factor_equals_mapping() -> None:
    reader = StaticFactorReader({("metaculus_prior", ""): 0.67})
    forecaster = StatisticalForecaster(
        factor_reader=reader,
        composition=(_step("metaculus_prior", role="posterior_prior"),),
    )

    result = await forecaster.apredict(_signal())

    assert result == pytest.approx((0.67, 0.5, "statistical-v1"))
    assert reader.calls


@pytest.mark.asyncio
async def test_statistical_forecaster_equal_weight_factors_average_mappings() -> None:
    forecaster = StatisticalForecaster(
        factor_reader=StaticFactorReader(
            {
                ("metaculus_prior", ""): 0.70,
                ("fair_value_spread", ""): 0.10,
            }
        ),
        composition=(
            _step("metaculus_prior", role="posterior_prior"),
            _step("fair_value_spread", role="weighted"),
        ),
    )

    result = await forecaster.apredict(_signal(yes_price=0.40))

    assert result is not None
    assert result[0] == pytest.approx(0.60)


@pytest.mark.asyncio
async def test_statistical_forecaster_agreement_increases_confidence() -> None:
    forecaster = StatisticalForecaster(
        factor_reader=StaticFactorReader(
            {
                ("metaculus_prior", ""): 0.61,
                ("fair_value_spread", ""): 0.20,
            }
        ),
        composition=(
            _step("metaculus_prior", role="posterior_prior"),
            _step("fair_value_spread", role="weighted"),
        ),
    )

    result = await forecaster.apredict(_signal(yes_price=0.40))

    assert result is not None
    assert result[1] > 0.90


@pytest.mark.asyncio
async def test_statistical_forecaster_disagreement_lowers_confidence() -> None:
    forecaster = StatisticalForecaster(
        factor_reader=StaticFactorReader(
            {
                ("metaculus_prior", ""): 0.80,
                ("fair_value_spread", ""): -0.20,
            }
        ),
        composition=(
            _step("metaculus_prior", role="posterior_prior"),
            _step("fair_value_spread", role="weighted"),
        ),
    )

    result = await forecaster.apredict(_signal(yes_price=0.40))

    assert result is not None
    assert result[0] == pytest.approx(0.50)
    assert result[1] < 0.70


@pytest.mark.asyncio
async def test_statistical_forecaster_missing_required_data_abstains() -> None:
    forecaster = StatisticalForecaster(
        factor_reader=StaticFactorReader(
            {},
            missing_factors=(("metaculus_prior", ""),),
        ),
        composition=(_step("metaculus_prior", role="posterior_prior"),),
    )

    assert await forecaster.apredict(_signal()) is None


@pytest.mark.asyncio
async def test_statistical_forecaster_posterior_counts_use_prior_once() -> None:
    forecaster = StatisticalForecaster(
        factor_reader=StaticFactorReader(
            {
                ("metaculus_prior", ""): 0.70,
                ("yes_count", ""): 3.0,
                ("no_count", ""): 7.0,
            }
        ),
        composition=(
            _step("metaculus_prior", role="posterior_prior", weight=10.0),
            _step("yes_count", role="posterior_success"),
            _step("no_count", role="posterior_failure"),
        ),
    )

    result = await forecaster.apredict(_signal())

    assert result is not None
    assert result[0] == pytest.approx(0.50)


def test_factory_wires_factor_reader_and_composition_into_statistical_forecaster() -> None:
    reader = StaticFactorReader({})
    strategy = build_paper_multi_factor_strategy().to_active(
        strategy_version_id="stats-factory-v1"
    )

    pipeline = ControllerPipelineFactory(
        settings=PMSSettings(mode=RunMode.PAPER),
        factor_reader=reader,
    ).build(strategy)

    stats = next(
        forecaster
        for forecaster in pipeline.forecasters or ()
        if isinstance(forecaster, StatisticalForecaster)
    )
    assert stats.factor_reader is reader
    assert stats.composition == strategy.config.factor_composition
