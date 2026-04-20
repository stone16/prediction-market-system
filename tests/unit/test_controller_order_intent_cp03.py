from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime
import importlib
from typing import Any

import pytest

from pms.config import ControllerSettings, RiskSettings
from pms.controller.calibrators.netcal import NetcalCalibrator
from pms.controller.pipeline import ControllerPipeline
from pms.controller.router import Router
from pms.controller.sizers.kelly import KellySizer
from pms.core.models import MarketSignal, Portfolio
from pms.strategies.projections import (
    ActiveStrategy,
    EvalSpec,
    FactorCompositionStep,
    ForecasterSpec,
    MarketSelectionSpec,
    RiskParams,
    StrategyConfig,
)


def _load_symbol(module_name: str, symbol_name: str) -> Any:
    module = importlib.import_module(module_name)
    return getattr(module, symbol_name)


def _step(
    factor_id: str,
    *,
    role: str,
    weight: float = 1.0,
    param: str = "",
    threshold: float | None = None,
) -> FactorCompositionStep:
    return FactorCompositionStep(
        factor_id=factor_id,
        role=role,
        param=param,
        weight=weight,
        threshold=threshold,
    )


def _active_strategy() -> ActiveStrategy:
    return ActiveStrategy(
        strategy_id="alpha",
        strategy_version_id="alpha-v1",
        config=StrategyConfig(
            strategy_id="alpha",
            factor_composition=(
                _step("snapshot_probability", role="runtime_probability"),
            ),
            metadata=(("owner", "test"),),
        ),
        risk=RiskParams(
            max_position_notional_usdc=100.0,
            max_daily_drawdown_pct=2.5,
            min_order_size_usdc=1.0,
        ),
        eval_spec=EvalSpec(metrics=("brier", "pnl")),
        forecaster=ForecasterSpec(forecasters=(("rules", ()),)),
        market_selection=MarketSelectionSpec(
            venue="polymarket",
            resolution_time_max_horizon_days=7,
            volume_min_usdc=500.0,
        ),
    )


def _signal(*, yes_price: float = 0.65) -> MarketSignal:
    return MarketSignal(
        market_id="market-buy-no",
        token_id="yes-token",
        venue="polymarket",
        title="Should the controller buy NO?",
        yes_price=yes_price,
        volume_24h=1_000.0,
        resolves_at=datetime(2026, 4, 30, tzinfo=UTC),
        orderbook={
            "bids": [{"price": 0.64, "size": 10.0}],
            "asks": [{"price": 0.66, "size": 10.0}],
        },
        external_signal={},
        fetched_at=datetime(2026, 4, 20, 12, 0, tzinfo=UTC),
        market_status="open",
    )


def _portfolio() -> Portfolio:
    return Portfolio(
        total_usdc=1_000.0,
        free_usdc=1_000.0,
        locked_usdc=0.0,
        open_positions=[],
    )


class StaticForecaster:
    def predict(self, signal: MarketSignal) -> tuple[float, float, str]:
        del signal
        return (0.30, 0.9, "static")

    async def forecast(self, signal: MarketSignal) -> float:
        del signal
        return 0.30


class RecordingFactorReader:
    def __init__(self, snapshot: Any) -> None:
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
        del market_id, as_of, required, strategy_id, strategy_version_id
        return self._snapshot


class RecordingOutcomeResolver:
    def __init__(self, tokens: Any) -> None:
        self._tokens = tokens
        self.calls: list[tuple[str, str | None]] = []

    async def resolve(self, *, market_id: str, signal_token_id: str | None) -> Any:
        self.calls.append((market_id, signal_token_id))
        return self._tokens


@pytest.mark.asyncio
async def test_negative_edge_maps_to_buy_no_with_resolved_no_token() -> None:
    factor_snapshot_cls = _load_symbol(
        "pms.controller.factor_snapshot",
        "FactorSnapshot",
    )
    outcome_tokens_cls = _load_symbol(
        "pms.controller.outcome_tokens",
        "OutcomeTokens",
    )
    resolver = RecordingOutcomeResolver(
        outcome_tokens_cls(yes_token_id="yes-token", no_token_id="no-token")
    )
    pipeline = ControllerPipeline(
        strategy=_active_strategy(),
        factor_reader=RecordingFactorReader(
            factor_snapshot_cls(
                values={("snapshot_probability", ""): 0.30},
                missing_factors=(),
                snapshot_hash="snapshot-30",
            )
        ),
        outcome_token_resolver=resolver,
        forecasters=(StaticForecaster(),),
        calibrator=NetcalCalibrator(),
        sizer=KellySizer(risk=RiskSettings(max_position_per_market=500.0)),
        router=Router(ControllerSettings(min_volume=100.0)),
    )

    emission = await pipeline.on_signal(_signal(), portfolio=_portfolio())

    assert emission is not None
    opportunity, decision = emission
    assert opportunity.side == "no"
    assert opportunity.token_id == "no-token"
    assert decision.side == "BUY"
    assert decision.action == "BUY"
    assert decision.outcome == "NO"
    assert decision.token_id == "no-token"
    assert decision.price == pytest.approx(0.35)
    assert decision.limit_price == pytest.approx(0.35)
    assert resolver.calls == [("market-buy-no", "yes-token")]


@pytest.mark.asyncio
async def test_negative_edge_skips_when_no_token_cannot_be_resolved() -> None:
    factor_snapshot_cls = _load_symbol(
        "pms.controller.factor_snapshot",
        "FactorSnapshot",
    )
    outcome_tokens_cls = _load_symbol(
        "pms.controller.outcome_tokens",
        "OutcomeTokens",
    )
    pipeline = ControllerPipeline(
        strategy=_active_strategy(),
        factor_reader=RecordingFactorReader(
            factor_snapshot_cls(
                values={("snapshot_probability", ""): 0.20},
                missing_factors=(),
                snapshot_hash="snapshot-20",
            )
        ),
        outcome_token_resolver=RecordingOutcomeResolver(
            outcome_tokens_cls(yes_token_id="yes-token", no_token_id=None)
        ),
        forecasters=(StaticForecaster(),),
        calibrator=NetcalCalibrator(),
        sizer=KellySizer(risk=RiskSettings(max_position_per_market=500.0)),
        router=Router(ControllerSettings(min_volume=100.0)),
    )

    emission = await pipeline.on_signal(_signal(), portfolio=_portfolio())

    assert emission is None
