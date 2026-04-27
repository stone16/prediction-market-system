from __future__ import annotations

from dataclasses import replace

import pytest

from pms.actuator.risk import RiskDecision, RiskManager
from pms.config import RiskSettings
from pms.controller.pipeline import _default_portfolio
from pms.core.enums import TimeInForce
from pms.core.models import Portfolio, Position, TradeDecision


def _decision(
    *,
    market_id: str = "market-risk",
    notional_usdc: float = 10.0,
    max_slippage_bps: int = 25,
) -> TradeDecision:
    construction_notional = notional_usdc if notional_usdc > 0.0 else 1.0
    decision = TradeDecision(
        decision_id="decision-risk",
        market_id=market_id,
        token_id="token-risk",
        venue="polymarket",
        side="BUY",
        notional_usdc=construction_notional,
        order_type="limit",
        max_slippage_bps=max_slippage_bps,
        stop_conditions=["unit-test"],
        prob_estimate=0.6,
        expected_edge=0.1,
        time_in_force=TimeInForce.GTC,
        opportunity_id="opportunity-risk",
        strategy_id="strategy-risk",
        strategy_version_id="strategy-risk-v1",
        limit_price=0.5,
    )
    if notional_usdc != construction_notional:
        object.__setattr__(decision, "notional_usdc", notional_usdc)
    return decision


def _risk(
    *,
    max_position_per_market: float = 100.0,
    max_total_exposure: float = 1000.0,
    max_drawdown_pct: float | None = None,
    max_open_positions: int | None = None,
    min_order_usdc: float = 1.0,
    slippage_threshold_bps: float = 50.0,
) -> RiskSettings:
    return RiskSettings(
        max_position_per_market=max_position_per_market,
        max_total_exposure=max_total_exposure,
        max_drawdown_pct=max_drawdown_pct,
        max_open_positions=max_open_positions,
        min_order_usdc=min_order_usdc,
        slippage_threshold_bps=slippage_threshold_bps,
    )


def _portfolio(
    *,
    total_usdc: float = 1_000.0,
    free_usdc: float = 1_000.0,
    locked_usdc: float = 0.0,
    open_positions: list[Position] | None = None,
    max_drawdown_pct: float | None = None,
) -> Portfolio:
    # The repo does not expose a narrower portfolio loader. Start from the
    # production default portfolio and overlay the position list needed by the
    # open-position cap test.
    base = _default_portfolio()
    return replace(
        base,
        total_usdc=total_usdc,
        free_usdc=free_usdc,
        locked_usdc=locked_usdc,
        open_positions=[] if open_positions is None else open_positions,
        max_drawdown_pct=max_drawdown_pct,
    )


def _open_positions(count: int) -> list[Position]:
    return [
        Position(
            market_id=f"market-{index}",
            token_id=f"token-{index}",
            venue="polymarket",
            side="BUY",
            shares_held=1.0,
            avg_entry_price=0.5,
            unrealized_pnl=0.0,
            locked_usdc=1.0,
        )
        for index in range(count)
    ]


def test_risk_settings_have_exact_live_fields() -> None:
    assert set(RiskSettings.model_fields) == {
        "max_position_per_market",
        "max_total_exposure",
        "max_drawdown_pct",
        "max_open_positions",
        "min_order_usdc",
        "slippage_threshold_bps",
        "max_quantity_shares",
    }


@pytest.mark.parametrize(
    "decision, risk, portfolio, approved, reason",
    [
        pytest.param(
            _decision(notional_usdc=50.0),
            _risk(max_position_per_market=100.0),
            _portfolio(),
            True,
            "approved",
            id="pass-max_position_per_market",
        ),
        pytest.param(
            _decision(notional_usdc=101.0),
            _risk(max_position_per_market=100.0),
            _portfolio(),
            False,
            "max_position_per_market",
            id="trip-max_position_per_market",
        ),
        pytest.param(
            _decision(notional_usdc=50.0),
            _risk(max_total_exposure=100.0),
            _portfolio(locked_usdc=25.0, free_usdc=975.0),
            True,
            "approved",
            id="pass-max_total_exposure",
        ),
        pytest.param(
            _decision(notional_usdc=60.0),
            _risk(max_total_exposure=100.0),
            _portfolio(locked_usdc=50.0, free_usdc=950.0),
            False,
            "max_total_exposure",
            id="trip-max_total_exposure",
        ),
        pytest.param(
            _decision(notional_usdc=10.0),
            _risk(max_drawdown_pct=0.25),
            _portfolio(max_drawdown_pct=0.2),
            True,
            "approved",
            id="pass-max_drawdown_pct",
        ),
        pytest.param(
            _decision(notional_usdc=10.0),
            _risk(max_drawdown_pct=0.25),
            _portfolio(max_drawdown_pct=0.3),
            False,
            "drawdown_circuit_breaker",
            id="trip-max_drawdown_pct",
        ),
        pytest.param(
            _decision(notional_usdc=10.0),
            _risk(max_open_positions=6),
            _portfolio(open_positions=_open_positions(5)),
            True,
            "approved",
            id="pass-max_open_positions",
        ),
        pytest.param(
            _decision(notional_usdc=10.0),
            _risk(max_open_positions=5),
            _portfolio(open_positions=_open_positions(5)),
            False,
            "max_open_positions",
            id="trip-max_open_positions",
        ),
        pytest.param(
            _decision(notional_usdc=10.0),
            _risk(min_order_usdc=1.0),
            _portfolio(),
            True,
            "approved",
            id="pass-min_order_usdc",
        ),
        pytest.param(
            _decision(notional_usdc=0.5),
            _risk(min_order_usdc=1.0),
            _portfolio(),
            False,
            "min_order_usdc",
            id="trip-min_order_usdc",
        ),
        pytest.param(
            _decision(notional_usdc=10.0, max_slippage_bps=25),
            _risk(slippage_threshold_bps=50.0),
            _portfolio(),
            True,
            "approved",
            id="pass-slippage_threshold_bps",
        ),
        pytest.param(
            _decision(notional_usdc=10.0, max_slippage_bps=100),
            _risk(slippage_threshold_bps=50.0),
            _portfolio(),
            False,
            "slippage_threshold_bps",
            id="trip-slippage_threshold_bps",
        ),
    ],
)
def test_risk_manager_enforces_live_settings(
    decision: TradeDecision,
    risk: RiskSettings,
    portfolio: Portfolio,
    approved: bool,
    reason: str,
) -> None:
    result = RiskManager(risk).check(decision, portfolio)

    assert result == RiskDecision(approved=approved, reason=reason)


def test_risk_manager_rejects_non_positive_notional() -> None:
    decision = _decision(notional_usdc=-1.0)

    result = RiskManager().check(decision, _portfolio())

    assert result == RiskDecision(approved=False, reason="non_positive_size")


def test_risk_manager_rejects_min_order_size() -> None:
    decision = _decision(notional_usdc=0.5)

    result = RiskManager(_risk(min_order_usdc=1.0)).check(decision, _portfolio())

    assert result == RiskDecision(approved=False, reason="min_order_usdc")


def test_risk_manager_rejects_when_open_positions_at_cap() -> None:
    portfolio = _portfolio(
        total_usdc=1_000.0,
        free_usdc=995.0,
        locked_usdc=5.0,
        open_positions=_open_positions(5),
    )
    decision = _decision(notional_usdc=10.0)

    result = RiskManager(_risk(max_open_positions=5)).check(decision, portfolio)

    assert result == RiskDecision(approved=False, reason="max_open_positions")


def test_risk_manager_allows_existing_position_add_at_open_position_cap() -> None:
    positions = _open_positions(4)
    positions.append(
        Position(
            market_id="market-risk",
            token_id="token-risk",
            venue="polymarket",
            side="BUY",
            shares_held=1.0,
            avg_entry_price=0.5,
            unrealized_pnl=0.0,
            locked_usdc=1.0,
        )
    )
    portfolio = _portfolio(
        free_usdc=995.0,
        locked_usdc=5.0,
        open_positions=positions,
    )

    result = RiskManager(_risk(max_open_positions=5)).check(
        _decision(notional_usdc=10.0),
        portfolio,
    )

    assert result == RiskDecision(approved=True, reason="approved")


def test_risk_manager_rejects_slippage_above_threshold() -> None:
    decision = _decision(notional_usdc=10.0, max_slippage_bps=100)

    result = RiskManager(_risk(slippage_threshold_bps=50.0)).check(
        decision,
        _portfolio(),
    )

    assert result == RiskDecision(
        approved=False,
        reason="slippage_threshold_bps",
    )
