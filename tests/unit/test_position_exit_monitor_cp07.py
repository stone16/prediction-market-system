from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
import yaml

from pms.config import PMSSettings, PositionExitSettings
from pms.core.models import OrderState, Portfolio, Position
from pms.core.models import MarketSignal, TradeDecision
from pms.runner import Runner


NOW = datetime(2026, 5, 14, 7, 0, tzinfo=UTC)


def _position(
    *,
    pnl_pct: float = 0.0,
    opened_days_ago: int = 1,
    current_price: float = 0.50,
    token_id: str = "exit-token",
    strategy_version_id: str = "exit-v1",
) -> Position:
    locked_usdc = 50.0
    return Position(
        market_id="exit-market",
        token_id=token_id,
        venue="polymarket",
        side="BUY",
        shares_held=100.0,
        avg_entry_price=0.50,
        unrealized_pnl=locked_usdc * pnl_pct / 100.0,
        locked_usdc=locked_usdc,
        current_price=current_price,
        opened_at=NOW - timedelta(days=opened_days_ago),
        strategy_id="exit-strategy",
        strategy_version_id=strategy_version_id,
    )


def _signal(*, yes_price: float = 0.50, token_id: str = "exit-token") -> MarketSignal:
    return MarketSignal(
        market_id="exit-market",
        token_id=token_id,
        venue="polymarket",
        title="Will the exit monitor fire?",
        yes_price=yes_price,
        volume_24h=1_000.0,
        resolves_at=datetime(2026, 6, 1, tzinfo=UTC),
        orderbook={
            "bids": [{"price": yes_price, "size": 250.0}],
            "asks": [{"price": min(0.99, yes_price + 0.01), "size": 250.0}],
        },
        external_signal={},
        fetched_at=NOW,
        market_status="open",
    )


class OneShotSensor:
    def __init__(self, signal: MarketSignal) -> None:
        self.signal = signal

    def __aiter__(self) -> AsyncIterator[MarketSignal]:
        return self._iterate()

    async def _iterate(self) -> AsyncIterator[MarketSignal]:
        yield self.signal


class CapturingExecutor:
    def __init__(self) -> None:
        self.decisions: list[TradeDecision] = []

    async def execute(
        self,
        decision: TradeDecision,
        portfolio: Portfolio,
    ) -> OrderState:
        del portfolio
        self.decisions.append(decision)
        return OrderState(
            order_id=f"order-{decision.decision_id}",
            decision_id=decision.decision_id,
            status="matched",
            market_id=decision.market_id,
            token_id=decision.token_id,
            venue=decision.venue,
            requested_notional_usdc=decision.notional_usdc,
            filled_notional_usdc=decision.notional_usdc,
            remaining_notional_usdc=0.0,
            fill_price=decision.limit_price,
            submitted_at=NOW,
            last_updated_at=NOW,
            raw_status="matched",
            strategy_id=decision.strategy_id,
            strategy_version_id=decision.strategy_version_id,
            filled_quantity=decision.notional_usdc / decision.limit_price,
        )


def _monitor() -> Any:
    from pms.actuator.exit_monitor import PositionExitMonitor

    return PositionExitMonitor(
        PositionExitSettings(
            enabled=True,
            stop_loss_pct=30.0,
            profit_take_pct=50.0,
            max_holding_days=7,
        )
    )


@pytest.mark.parametrize(
    ("pnl_pct", "expected"),
    [(-31.0, "stop_loss"), (-29.0, None), (51.0, "profit_take"), (49.0, None)],
)
def test_position_exit_monitor_thresholds(pnl_pct: float, expected: str | None) -> None:
    signal = _monitor().evaluate(_position(pnl_pct=pnl_pct), now=NOW)

    assert (None if signal is None else signal.trigger) == expected


@pytest.mark.parametrize(
    ("opened_days_ago", "expected"),
    [(8, "time_decay"), (6, None)],
)
def test_position_exit_monitor_time_decay(opened_days_ago: int, expected: str | None) -> None:
    signal = _monitor().evaluate(_position(opened_days_ago=opened_days_ago), now=NOW)

    assert (None if signal is None else signal.trigger) == expected


def test_position_exit_monitor_disabled_triggers_never_fire() -> None:
    from pms.actuator.exit_monitor import PositionExitMonitor

    monitor = PositionExitMonitor(
        PositionExitSettings(
            enabled=False,
            stop_loss_pct=30.0,
            profit_take_pct=50.0,
            max_holding_days=7,
        )
    )

    assert monitor.evaluate(_position(pnl_pct=-99.0, opened_days_ago=30), now=NOW) is None


def test_mark_position_from_yes_signal_marks_no_token_position() -> None:
    from pms.actuator.exit_monitor import mark_position_from_signal

    position = _position(token_id="exit-token-no", current_price=0.40)
    signal = _signal(yes_price=0.75, token_id="exit-token-yes")

    marked = mark_position_from_signal(position, signal)

    assert marked is not None
    assert marked.token_id == "exit-token-no"
    assert marked.current_price == pytest.approx(0.25)
    assert marked.unrealized_pnl == pytest.approx((0.25 - 0.50) * 100.0)
    assert marked.mark_source == "signal"


def test_position_exit_monitor_priority_order() -> None:
    signal = _monitor().evaluate(_position(pnl_pct=-31.0, opened_days_ago=8), now=NOW)

    assert signal is not None
    assert signal.trigger == "stop_loss"


def test_position_exit_dedupe_key_includes_strategy_version() -> None:
    from pms.actuator.exit_monitor import exit_key

    first = _monitor().evaluate(
        _position(pnl_pct=-31.0, strategy_version_id="exit-v1"),
        now=NOW,
    )
    second = _monitor().evaluate(
        _position(pnl_pct=-31.0, strategy_version_id="exit-v2"),
        now=NOW,
    )

    assert first is not None
    assert second is not None
    assert exit_key(first) != exit_key(second)


@pytest.mark.parametrize(("current_price", "expected"), [(0.0, 0.001), (1.0, 0.999)])
def test_build_exit_decision_clamps_boundary_mark_prices(
    current_price: float,
    expected: float,
) -> None:
    from pms.actuator.exit_monitor import PositionExitSignal, build_exit_decision
    from pms.core.enums import TimeInForce

    position = _position(pnl_pct=-31.0, current_price=current_price)
    decision = build_exit_decision(
        _signal(yes_price=current_price),
        PositionExitSignal(
            trigger="stop_loss",
            position=position,
            pnl_pct=-31.0,
            held_days=1.0,
            current_price=current_price,
        ),
        max_slippage_bps=50,
        time_in_force=TimeInForce.IOC,
    )

    assert decision.limit_price == pytest.approx(expected)
    assert decision.expected_edge == pytest.approx(0.0)


def test_build_exit_decision_preserves_no_token_outcome() -> None:
    from pms.actuator.exit_monitor import PositionExitSignal, build_exit_decision
    from pms.core.enums import TimeInForce

    signal = _signal(yes_price=0.75, token_id="exit-token-yes")
    position = _position(token_id="exit-token-no", current_price=0.25)
    decision = build_exit_decision(
        signal,
        PositionExitSignal(
            trigger="stop_loss",
            position=position,
            pnl_pct=-50.0,
            held_days=1.0,
            current_price=0.25,
        ),
        max_slippage_bps=50,
        time_in_force=TimeInForce.IOC,
    )

    assert decision.token_id == "exit-token-no"
    assert decision.outcome == "NO"
    assert decision.prob_estimate == pytest.approx(0.25)
    assert decision.limit_price == pytest.approx(0.25)
    assert decision.side == "SELL"


def test_live_soak_config_sets_position_exit_policy() -> None:
    payload = yaml.safe_load(Path("config.live-soak.yaml").read_text(encoding="utf-8"))
    position_exit = payload["position_exit"]

    assert position_exit["enabled"] is True
    assert position_exit["stop_loss_pct"] == pytest.approx(30.0)
    assert position_exit["profit_take_pct"] == pytest.approx(50.0)
    assert position_exit["max_holding_days"] == 7


@pytest.mark.asyncio
async def test_runner_converts_exit_signal_to_opposing_trade_decision() -> None:
    executor = CapturingExecutor()
    runner = Runner(
        config=PMSSettings(
            position_exit=PositionExitSettings(
                enabled=True,
                stop_loss_pct=30.0,
                profit_take_pct=50.0,
                max_holding_days=7,
            ),
        ),
        sensors=[OneShotSensor(_signal(yes_price=0.34))],
        portfolio=Portfolio(
            total_usdc=1000.0,
            free_usdc=10.0,
            locked_usdc=50.0,
            open_positions=[_position(pnl_pct=-32.0, current_price=0.34)],
        ),
    )
    runner.actuator_executor = executor  # type: ignore[assignment]

    await runner.start()
    await runner.wait_until_idle()
    await runner.stop()

    assert len(executor.decisions) == 1
    decision = executor.decisions[0]
    assert decision.side == "SELL"
    assert decision.strategy_id == "exit-strategy"
    assert decision.strategy_version_id == "exit-v1"
    assert decision.stop_conditions == ["position_exit:stop_loss"]
