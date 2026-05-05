from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any

import pytest

from pms.config import PMSSettings
from pms.controller.factor_snapshot import FactorSnapshot
from pms.controller.factory import ControllerPipelineFactory
from pms.core.enums import RunMode
from pms.core.models import MarketSignal, Portfolio
from pms.strategies.paper_multifactor import (
    PAPER_MULTI_FACTOR_STRATEGY_ID,
    build_paper_multi_factor_strategy,
)
from pms.strategies.projections import FactorCompositionStep


def _signal() -> MarketSignal:
    return MarketSignal(
        market_id="phase-a-market",
        token_id="phase-a-token",
        venue="polymarket",
        title="Can Phase A produce a simple factor-backed decision?",
        yes_price=0.50,
        volume_24h=1_000.0,
        resolves_at=datetime(2026, 6, 1, tzinfo=UTC),
        orderbook={
            "bids": [{"price": 0.49, "size": 100.0}],
            "asks": [{"price": 0.51, "size": 100.0}],
        },
        external_signal={"raw_event_type": "book"},
        fetched_at=datetime(2026, 5, 5, 12, 0, tzinfo=UTC),
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


def test_paper_multi_factor_strategy_matches_phase_a_contract() -> None:
    strategy = build_paper_multi_factor_strategy()

    assert strategy.config.strategy_id == PAPER_MULTI_FACTOR_STRATEGY_ID
    assert dict(strategy.config.metadata) == {
        "owner": "Researcher-Ciga",
        "tier": "paper",
        "phase": "A",
        "purpose": "paper_multi_factor_phase_a",
        "price_reference": "best_ask",
        "live_allowed": "false",
        "requires_strict_factor_gates": "false",
    }
    assert strategy.risk.max_position_notional_usdc == pytest.approx(2.0)
    assert strategy.risk.max_daily_drawdown_pct == pytest.approx(50.0)
    assert strategy.risk.min_order_size_usdc == pytest.approx(0.50)
    assert strategy.eval_spec.metrics == ("brier", "pnl", "fill_rate")
    assert strategy.eval_spec.min_win_rate == pytest.approx(0.45)
    assert strategy.market_selection.venue == "polymarket"
    assert strategy.market_selection.resolution_time_max_horizon_days == 90
    assert strategy.market_selection.volume_min_usdc == pytest.approx(100.0)
    assert strategy.forecaster.forecasters == (
        ("rules", (("threshold", "0.55"),)),
        ("stats", (("window", "15m"),)),
        ("llm", ()),
    )
    assert tuple(
        (step.factor_id, step.role, step.threshold, step.required)
        for step in strategy.config.factor_composition
    ) == (
        ("orderbook_imbalance", "threshold_edge", 0.10, True),
        ("orderbook_imbalance", "weighted", None, False),
        ("rules", "blend_weighted", None, False),
    )


def test_paper_multi_factor_strategy_is_rejected_outside_paper_mode() -> None:
    settings = PMSSettings(mode=RunMode.LIVE)
    strategy = build_paper_multi_factor_strategy()

    with pytest.raises(ValueError, match="paper_multi_factor_v1 is PAPER-only"):
        ControllerPipelineFactory(settings=settings).build(
            strategy.to_active(strategy_version_id="phase-a-v1")
        )


@pytest.mark.asyncio
async def test_paper_multi_factor_can_decide_from_orderbook_imbalance_only() -> None:
    strategy = build_paper_multi_factor_strategy()
    factor_reader = StaticFactorReader(
        FactorSnapshot(
            values={("orderbook_imbalance", ""): 0.12},
            missing_factors=(),
            snapshot_hash="phase-a-snapshot",
        )
    )
    pipeline = ControllerPipelineFactory(
        settings=PMSSettings(mode=RunMode.PAPER),
        factor_reader=factor_reader,
    ).build(strategy.to_active(strategy_version_id="phase-a-v1"))

    decision = await pipeline.decide(_signal(), portfolio=_portfolio())

    assert decision is not None
    assert decision.strategy_id == PAPER_MULTI_FACTOR_STRATEGY_ID
    assert decision.outcome == "YES"
    assert decision.limit_price == pytest.approx(0.51)
    assert decision.prob_estimate == pytest.approx(0.62)
    assert decision.expected_edge == pytest.approx(0.11)
    assert decision.notional_usdc == pytest.approx(2.0)
    assert factor_reader.snapshot_calls
