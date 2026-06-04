from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from typing import Literal

import pytest

from pms.actuator.risk import RiskDecision, RiskManager
from pms.config import RiskSettings
from pms.controller.pipeline import _default_portfolio
from pms.core.enums import TimeInForce
from pms.core.models import OrderState, Portfolio, Position, TradeDecision


def _decision(
    *,
    market_id: str = "market-risk",
    token_id: str = "token-risk",
    venue: Literal["polymarket", "kalshi"] = "polymarket",
    side: Literal["BUY", "SELL"] = "BUY",
    notional_usdc: float = 10.0,
    max_slippage_bps: int = 25,
    risk_group_id: str | None = None,
) -> TradeDecision:
    construction_notional = notional_usdc if notional_usdc > 0.0 else 1.0
    decision = TradeDecision(
        decision_id="decision-risk",
        market_id=market_id,
        token_id=token_id,
        venue=venue,
        side=side,
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
        risk_group_id=risk_group_id,
    )
    if notional_usdc != construction_notional:
        object.__setattr__(decision, "notional_usdc", notional_usdc)
    return decision


def _risk(
    *,
    max_position_per_market: float = 100.0,
    max_total_exposure: float = 1000.0,
    max_drawdown_pct: float | None = None,
    max_daily_loss_usdc: float | None = None,
    max_open_positions: int | None = None,
    max_exposure_per_risk_group: float | None = None,
    min_order_usdc: float = 1.0,
    slippage_threshold_bps: float = 50.0,
) -> RiskSettings:
    return RiskSettings(
        max_position_per_market=max_position_per_market,
        max_total_exposure=max_total_exposure,
        max_drawdown_pct=max_drawdown_pct,
        max_daily_loss_usdc=max_daily_loss_usdc,
        max_open_positions=max_open_positions,
        max_exposure_per_risk_group=max_exposure_per_risk_group,
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
            strategy_id="strategy-risk",
            strategy_version_id="strategy-risk-v1",
        )
        for index in range(count)
    ]


def _position(
    *,
    market_id: str = "market-existing",
    token_id: str = "token-existing",
    locked_usdc: float = 1.0,
    risk_group_id: str | None = None,
) -> Position:
    return Position(
        market_id=market_id,
        token_id=token_id,
        venue="polymarket",
        side="BUY",
        shares_held=1.0,
        avg_entry_price=0.5,
        unrealized_pnl=0.0,
        locked_usdc=locked_usdc,
        strategy_id="strategy-risk",
        strategy_version_id="strategy-risk-v1",
        risk_group_id=risk_group_id,
    )


def _live_open_order(
    *,
    order_id: str = "live-open-risk-order",
    market_id: str = "market-open-risk",
    token_id: str = "token-open-risk",
    remaining_notional_usdc: float = 10.0,
    risk_group_id: str | None = "event:open-risk",
) -> OrderState:
    return OrderState(
        order_id=order_id,
        decision_id=f"decision-{order_id}",
        status="live",
        market_id=market_id,
        token_id=token_id,
        venue="polymarket",
        requested_notional_usdc=remaining_notional_usdc,
        filled_notional_usdc=0.0,
        remaining_notional_usdc=remaining_notional_usdc,
        fill_price=None,
        submitted_at=datetime(2026, 6, 1, 12, 0, tzinfo=UTC),
        last_updated_at=datetime(2026, 6, 1, 12, 0, tzinfo=UTC),
        raw_status="live",
        strategy_id="strategy-risk",
        strategy_version_id="strategy-risk-v1",
        filled_quantity=0.0,
        risk_group_id=risk_group_id,
    )


def test_risk_settings_have_exact_live_fields() -> None:
    assert set(RiskSettings.model_fields) == {
        "max_position_per_market",
        "max_total_exposure",
        "max_drawdown_pct",
        "max_daily_loss_usdc",
        "max_open_positions",
        "max_exposure_per_risk_group",
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


def test_risk_manager_allows_below_min_order_full_position_reduction() -> None:
    portfolio = _portfolio(
        locked_usdc=0.5,
        open_positions=[
            _position(
                market_id="market-risk",
                token_id="token-risk",
                locked_usdc=0.5,
                risk_group_id="event:risk",
            )
        ],
    )
    decision = _decision(
        side="SELL",
        notional_usdc=0.5,
        risk_group_id="event:risk",
    )

    result = RiskManager(
        _risk(min_order_usdc=1.0, max_exposure_per_risk_group=0.25)
    ).check(decision, portfolio)

    assert result == RiskDecision(approved=True, reason="approved")


def test_risk_manager_counts_live_open_order_risk_group_exposure() -> None:
    manager = RiskManager(_risk(max_exposure_per_risk_group=15.0))
    manager.record_open_order_state(_live_open_order(remaining_notional_usdc=10.0))
    decision = _decision(
        notional_usdc=6.0,
        risk_group_id="event:open-risk",
    )

    result = manager.check(decision, _portfolio())

    assert result == RiskDecision(
        approved=False,
        reason="max_exposure_per_risk_group",
    )


def test_risk_manager_exposes_active_halt_state() -> None:
    checked_at = datetime(2026, 5, 31, tzinfo=UTC)
    manager = RiskManager(_risk(max_drawdown_pct=10.0))
    portfolio = _portfolio(max_drawdown_pct=11.0)

    halt = manager.check_auto_halt(portfolio, now=checked_at)

    assert halt.halted is True
    assert manager.active_halt() == halt


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
            strategy_id="strategy-risk",
            strategy_version_id="strategy-risk-v1",
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


def test_risk_manager_allows_reducing_position_through_exposure_and_cash_caps() -> None:
    portfolio = _portfolio(
        total_usdc=100.0,
        free_usdc=0.0,
        locked_usdc=50.0,
        open_positions=[
            Position(
                market_id="market-risk",
                token_id="token-risk",
                venue="polymarket",
                side="BUY",
                shares_held=100.0,
                avg_entry_price=0.5,
                unrealized_pnl=-16.0,
                locked_usdc=50.0,
                strategy_id="strategy-risk",
                strategy_version_id="strategy-risk-v1",
            )
        ],
    )
    decision = _decision(
        side="SELL",
        notional_usdc=34.0,
        max_slippage_bps=25,
    )
    risk = RiskSettings(
        max_position_per_market=10.0,
        max_total_exposure=10.0,
        max_open_positions=1,
        min_order_usdc=1.0,
        slippage_threshold_bps=50.0,
        max_quantity_shares=1.0,
    )

    result = RiskManager(risk).check(decision, portfolio)

    assert result == RiskDecision(approved=True, reason="approved")


def test_risk_manager_reduction_bypass_requires_same_strategy_version() -> None:
    portfolio = _portfolio(
        total_usdc=100.0,
        free_usdc=0.0,
        locked_usdc=50.0,
        open_positions=[
            Position(
                market_id="market-risk",
                token_id="token-risk",
                venue="polymarket",
                side="BUY",
                shares_held=100.0,
                avg_entry_price=0.5,
                unrealized_pnl=-16.0,
                locked_usdc=50.0,
                strategy_id="other-strategy",
                strategy_version_id="other-v1",
            )
        ],
    )
    decision = _decision(side="SELL", notional_usdc=34.0, max_slippage_bps=25)

    result = RiskManager(
        _risk(max_position_per_market=10.0, max_total_exposure=10.0)
    ).check(decision, portfolio)

    assert result == RiskDecision(approved=False, reason="max_position_per_market")


def test_risk_manager_reduction_bypass_rejects_oversized_sell() -> None:
    portfolio = _portfolio(
        total_usdc=100.0,
        free_usdc=0.0,
        locked_usdc=5.0,
        open_positions=[
            Position(
                market_id="market-risk",
                token_id="token-risk",
                venue="polymarket",
                side="BUY",
                shares_held=10.0,
                avg_entry_price=0.5,
                unrealized_pnl=-1.0,
                locked_usdc=5.0,
                strategy_id="strategy-risk",
                strategy_version_id="strategy-risk-v1",
            )
        ],
    )
    decision = _decision(side="SELL", notional_usdc=10.0, max_slippage_bps=25)

    result = RiskManager(
        _risk(max_position_per_market=10.0, max_total_exposure=10.0)
    ).check(decision, portfolio)

    assert result == RiskDecision(approved=False, reason="partial_reduction_unsupported")


@pytest.mark.parametrize(
    "position_venue, position_side, approved, reason",
    [
        ("kalshi", "BUY", False, "max_open_positions"),
        ("polymarket", "SELL", True, "approved"),
    ],
)
def test_risk_manager_open_position_cap_bypass_distinguishes_adds_from_reductions(
    position_venue: Literal["polymarket", "kalshi"],
    position_side: Literal["BUY", "SELL"],
    approved: bool,
    reason: str,
) -> None:
    positions = _open_positions(4)
    positions.append(
        Position(
            market_id="market-risk",
            token_id="token-risk",
            venue=position_venue,
            side=position_side,
            shares_held=100.0,
            avg_entry_price=0.5,
            unrealized_pnl=0.0,
            locked_usdc=50.0,
            strategy_id="strategy-risk",
            strategy_version_id="strategy-risk-v1",
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

    assert result == RiskDecision(approved=approved, reason=reason)


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


def test_risk_manager_rejects_correlated_group_exposure_above_cap() -> None:
    portfolio = _portfolio(
        free_usdc=980.0,
        locked_usdc=20.0,
        open_positions=[
            _position(
                market_id="market-election-a",
                locked_usdc=20.0,
                risk_group_id="event:2028-us-presidential-election",
            )
        ],
    )
    decision = _decision(
        market_id="market-election-b",
        notional_usdc=10.0,
        risk_group_id="event:2028-us-presidential-election",
    )

    result = RiskManager(_risk(max_exposure_per_risk_group=25.0)).check(
        decision,
        portfolio,
    )

    assert result == RiskDecision(
        approved=False,
        reason="max_exposure_per_risk_group",
    )


def test_risk_manager_rejects_missing_decision_group_when_group_cap_is_configured() -> None:
    portfolio = _portfolio(
        free_usdc=980.0,
        locked_usdc=20.0,
        open_positions=[
            _position(
                market_id="market-election-a",
                locked_usdc=20.0,
                risk_group_id="event:2028-us-presidential-election",
            )
        ],
    )

    result = RiskManager(_risk(max_exposure_per_risk_group=25.0)).check(
        _decision(market_id="market-election-b", notional_usdc=10.0),
        portfolio,
    )

    assert result == RiskDecision(approved=False, reason="missing_risk_group_id")
