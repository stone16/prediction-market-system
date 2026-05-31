from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest

from pms.config import PMSSettings, RiskSettings
from pms.core.enums import TimeInForce
from pms.core.models import Position, TradeDecision, Venue
from pms.runner import Runner


FIXTURE_PATH = Path("tests/fixtures/polymarket_7day_synthetic.jsonl")


def _position(
    *,
    market_id: str = "market-a",
    token_id: str = "token-a",
    venue: Venue = "polymarket",
) -> Position:
    return Position(
        market_id=market_id,
        token_id=token_id,
        venue=venue,
        side="BUY",
        shares_held=10.0,
        avg_entry_price=0.5,
        unrealized_pnl=0.0,
        locked_usdc=5.0,
    )


def _decision(
    *,
    market_id: str = "market-b",
    token_id: str = "token-b",
    venue: Venue = "polymarket",
) -> TradeDecision:
    return TradeDecision(
        decision_id="decision-cap",
        market_id=market_id,
        token_id=token_id,
        venue=venue,
        side="BUY",
        action="BUY",
        notional_usdc=5.0,
        order_type="limit",
        max_slippage_bps=25,
        stop_conditions=[],
        prob_estimate=0.6,
        expected_edge=0.1,
        time_in_force=TimeInForce.IOC,
        opportunity_id="opp-cap",
        strategy_id="default",
        strategy_version_id="default-v1",
        limit_price=0.5,
        outcome="YES",
    )


def _runner(*, max_open_positions: int | None = 2) -> Runner:
    settings = PMSSettings(
        risk=RiskSettings(max_open_positions=max_open_positions),
    )
    return Runner(config=settings, historical_data_path=FIXTURE_PATH)


class TestWouldExceedPositionCapacity:
    def test_no_limit_set(self) -> None:
        runner = _runner(max_open_positions=None)
        runner.portfolio = replace(
            runner.portfolio,
            open_positions=[_position(), _position(market_id="market-c")],
        )
        assert not runner._would_exceed_position_capacity(_decision())

    def test_under_limit(self) -> None:
        runner = _runner(max_open_positions=3)
        runner.portfolio = replace(
            runner.portfolio,
            open_positions=[_position()],
        )
        assert not runner._would_exceed_position_capacity(_decision())

    def test_at_limit_new_market(self) -> None:
        runner = _runner(max_open_positions=2)
        runner.portfolio = replace(
            runner.portfolio,
            open_positions=[
                _position(),
                _position(market_id="market-c", token_id="token-c"),
            ],
        )
        assert runner._would_exceed_position_capacity(_decision())

    def test_at_limit_existing_market(self) -> None:
        runner = _runner(max_open_positions=2)
        runner.portfolio = replace(
            runner.portfolio,
            open_positions=[
                _position(),
                _position(market_id="market-b", token_id="token-b"),
            ],
        )
        assert not runner._would_exceed_position_capacity(_decision())

    def test_at_limit_same_market_different_token(self) -> None:
        runner = _runner(max_open_positions=2)
        runner.portfolio = replace(
            runner.portfolio,
            open_positions=[
                _position(),
                _position(market_id="market-b", token_id="token-OTHER"),
            ],
        )
        assert runner._would_exceed_position_capacity(_decision())

    def test_pending_new_position_counts_against_limit(self) -> None:
        runner = _runner(max_open_positions=2)
        runner.portfolio = replace(
            runner.portfolio,
            open_positions=[_position()],
        )
        first = _decision(market_id="market-b", token_id="token-b")
        second = replace(
            _decision(market_id="market-c", token_id="token-c"),
            decision_id="decision-cap-2",
        )

        assert not runner._would_exceed_position_capacity(first)
        assert runner._reserve_position_capacity(first) is not None

        assert runner._would_exceed_position_capacity(second)

    def test_released_pending_position_no_longer_counts_against_limit(self) -> None:
        runner = _runner(max_open_positions=2)
        runner.portfolio = replace(
            runner.portfolio,
            open_positions=[_position()],
        )
        first = _decision(market_id="market-b", token_id="token-b")
        second = replace(
            _decision(market_id="market-c", token_id="token-c"),
            decision_id="decision-cap-2",
        )

        assert runner._reserve_position_capacity(first) is not None
        runner._release_position_capacity_reservation(first.decision_id)

        assert not runner._would_exceed_position_capacity(second)

    def test_reserve_position_capacity_is_atomic_with_capacity_check(self) -> None:
        runner = _runner(max_open_positions=2)
        runner.portfolio = replace(
            runner.portfolio,
            open_positions=[_position()],
        )
        first = _decision(market_id="market-b", token_id="token-b")
        second = replace(
            _decision(market_id="market-c", token_id="token-c"),
            decision_id="decision-cap-2",
        )

        assert runner._reserve_position_capacity(first) is not None

        assert runner._reserve_position_capacity(second) is None

    @pytest.mark.asyncio
    async def test_enqueue_rechecks_capacity_before_queuing(self) -> None:
        runner = _runner(max_open_positions=2)
        runner.portfolio = replace(
            runner.portfolio,
            open_positions=[_position()],
        )
        first = _decision(market_id="market-b", token_id="token-b")
        second = replace(
            _decision(market_id="market-c", token_id="token-c"),
            decision_id="decision-cap-2",
        )
        assert runner._reserve_position_capacity(first) is not None

        await runner._enqueue_decision(second, signal=None)

        assert runner._decision_queue.empty()
