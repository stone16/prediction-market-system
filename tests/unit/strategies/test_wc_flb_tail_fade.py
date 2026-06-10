from __future__ import annotations

from collections.abc import Sequence
from dataclasses import replace
from datetime import UTC, datetime
from typing import Any

import pytest

from pms.config import PMSSettings
from pms.controller.calibrators.extreme_clamp import ExtremeProbClamp
from pms.controller.factor_snapshot import FactorSnapshot
from pms.controller.factory import ControllerPipelineFactory
from pms.core.enums import RunMode
from pms.core.models import MarketSignal, Portfolio
from pms.strategies.aggregate import Strategy
from pms.strategies.projections import (
    CalibrationContext,
    CalibrationSpec,
    FactorCompositionStep,
)
from pms.strategies.versioning import compute_strategy_version_id
from pms.strategies.wc_flb_tail_fade import (
    WC_FLB_TAIL_FADE_STRATEGY_ID,
    build_wc_flb_tail_fade_strategy,
)


def _signal(
    *,
    yes_price: float,
    orderbook: dict[str, object],
) -> MarketSignal:
    return MarketSignal(
        market_id="wc-2026-market",
        token_id="wc-2026-token",
        venue="polymarket",
        title="Will the longshot win its World Cup 2026 group match?",
        yes_price=yes_price,
        volume_24h=5_000.0,
        resolves_at=datetime(2026, 6, 12, tzinfo=UTC),
        orderbook=orderbook,
        external_signal={"raw_event_type": "book"},
        fetched_at=datetime(2026, 6, 10, 12, 0, tzinfo=UTC),
        market_status="open",
    )


def _portfolio() -> Portfolio:
    return Portfolio(
        total_usdc=1_000.0,
        free_usdc=1_000.0,
        locked_usdc=0.0,
        open_positions=[],
    )


class StaticFactorReader:
    def __init__(self, snapshot: FactorSnapshot) -> None:
        self.snapshot_calls: list[Sequence[FactorCompositionStep]] = []
        self._snapshot = snapshot

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
        self.snapshot_calls.append(required)
        return self._snapshot


def test_wc_flb_tail_fade_composition_weights_and_thresholds() -> None:
    strategy = build_wc_flb_tail_fade_strategy()

    assert strategy.config.strategy_id == WC_FLB_TAIL_FADE_STRATEGY_ID
    assert tuple(
        (
            step.factor_id,
            step.role,
            step.weight,
            step.threshold,
            step.required,
            step.freshness_sla_s,
            step.allow_neutral_fallback,
        )
        for step in strategy.config.factor_composition
    ) == (
        ("favorite_longshot_bias", "rule_delta", 2.0, 0.01, True, 300.0, False),
        ("orderbook_imbalance", "rule_delta", 0.05, 0.50, False, 60.0, True),
        ("rules", "blend_weighted", 1.0, None, False, None, True),
    )
    assert strategy.forecaster.forecasters == (
        ("rules", (("threshold", "0.55"),)),
    )
    metadata = dict(strategy.config.metadata)
    assert metadata["purpose"] == "wc2026_flb_tail_fade_paper_soak"
    assert metadata["price_reference"] == "best_ask"
    assert metadata["live_allowed"] == "false"
    assert strategy.risk.max_position_notional_usdc == pytest.approx(1.0)
    assert strategy.risk.max_daily_drawdown_pct == pytest.approx(20.0)
    assert strategy.risk.min_order_size_usdc == pytest.approx(1.0)
    assert strategy.eval_spec.metrics == ("brier", "pnl", "fill_rate")
    assert strategy.eval_spec.max_brier_score == pytest.approx(0.30)
    assert strategy.eval_spec.min_win_rate == pytest.approx(0.45)
    assert strategy.eval_spec.slippage_threshold_bps == pytest.approx(50.0)
    assert strategy.market_selection.venue == "polymarket"
    assert strategy.market_selection.resolution_time_max_horizon_days == 4
    assert strategy.market_selection.volume_min_usdc == pytest.approx(1_000.0)
    assert strategy.market_selection.yes_price_min == pytest.approx(0.02)
    assert strategy.market_selection.yes_price_max == pytest.approx(0.98)


def test_wc_flb_tail_fade_version_id_is_content_addressed() -> None:
    first = build_wc_flb_tail_fade_strategy()
    second = build_wc_flb_tail_fade_strategy()

    assert compute_strategy_version_id(*first.snapshot()) == (
        compute_strategy_version_id(*second.snapshot())
    )

    config, risk, eval_spec, forecaster, market_selection, calibration = (
        first.snapshot()
    )
    mutated_config = replace(
        config,
        factor_composition=(
            replace(config.factor_composition[0], threshold=0.02),
            *config.factor_composition[1:],
        ),
    )
    assert compute_strategy_version_id(
        mutated_config,
        risk,
        eval_spec,
        forecaster,
        market_selection,
        calibration,
    ) != compute_strategy_version_id(*first.snapshot())


def test_wc_flb_tail_fade_calibration_clamp_preserves_tail_probabilities() -> None:
    # Business invariant: the strategy only trades the tails, and the
    # strategy starts with fewer than min_resolved_for_extreme resolved
    # samples. The clamp window itself must therefore admit initial tail
    # probabilities.
    strategy = build_wc_flb_tail_fade_strategy()

    assert strategy.calibration.enabled is True
    assert strategy.calibration.shrinkage_factor == pytest.approx(1.0)
    assert strategy.calibration.shrinkage_bias == pytest.approx(0.0)
    assert strategy.calibration.extreme_clamp_low == pytest.approx(0.001)
    assert strategy.calibration.extreme_clamp_high == pytest.approx(0.999)
    assert strategy.calibration.min_resolved_for_extreme == 20

    context = CalibrationContext(resolved_sample_count=0, model_id="rules-v1")
    clamp = ExtremeProbClamp(strategy.calibration)
    assert clamp.calibrate(0.03, context=context) == pytest.approx(0.03)
    assert clamp.calibrate(0.97, context=context) == pytest.approx(0.97)
    assert clamp.calibrate(0.0005, context=context) is None

    default_clamp = ExtremeProbClamp(CalibrationSpec(enabled=True))
    assert default_clamp.calibrate(0.03, context=context) is None
    assert default_clamp.calibrate(0.97, context=context) is None


@pytest.mark.asyncio
async def test_wc_flb_tail_fade_emits_capped_tail_decision() -> None:
    strategy = build_wc_flb_tail_fade_strategy()
    factor_reader = StaticFactorReader(
        FactorSnapshot(
            values={("favorite_longshot_bias", ""): 0.02},
            missing_factors=(),
            snapshot_hash="wc-flb-tail-snapshot",
        )
    )
    pipeline = ControllerPipelineFactory(
        settings=PMSSettings(mode=RunMode.PAPER),
        factor_reader=factor_reader,
    ).build(strategy.to_active(strategy_version_id="wc-flb-v1"))

    signal = _signal(
        yes_price=0.93,
        orderbook={
            "bids": [{"price": 0.928, "size": 100.0}],
            "asks": [{"price": 0.932, "size": 100.0}],
        },
    )

    decision = await pipeline.decide(signal, portfolio=_portfolio())

    assert decision is not None
    assert decision.strategy_id == WC_FLB_TAIL_FADE_STRATEGY_ID
    assert decision.outcome == "YES"
    assert decision.prob_estimate == pytest.approx(0.97)
    assert decision.limit_price == pytest.approx(0.932)
    assert decision.notional_usdc == pytest.approx(1.0)
    assert factor_reader.snapshot_calls

    # Counterfactual: the same tail forecast dies under the default
    # [0.08, 0.92] clamp — the wide clamp is what keeps the edge alive.
    config, risk, eval_spec, forecaster, market_selection, calibration = (
        strategy.snapshot()
    )
    narrow_strategy = Strategy(
        config=config,
        risk=risk,
        eval_spec=eval_spec,
        forecaster=forecaster,
        market_selection=market_selection,
        calibration=replace(
            calibration,
            extreme_clamp_low=0.08,
            extreme_clamp_high=0.92,
        ),
    )
    narrow_pipeline = ControllerPipelineFactory(
        settings=PMSSettings(mode=RunMode.PAPER),
        factor_reader=factor_reader,
    ).build(narrow_strategy.to_active(strategy_version_id="wc-flb-v1-narrow"))

    narrow_decision = await narrow_pipeline.decide(signal, portfolio=_portfolio())

    assert narrow_decision is None
    assert narrow_pipeline.last_diagnostic is not None
    assert narrow_pipeline.last_diagnostic.code == "calibration_clamp_rejected"
