from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import pytest

from pms.config import PMSSettings, RiskSettings
from pms.core.enums import TimeInForce
from pms.core.models import MarketSignal, Position, TradeDecision, Venue
from pms.runner import Runner, _estimated_decision_quantity


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


def _signal(
    *,
    market_id: str = "market-b",
    token_id: str = "token-b",
) -> MarketSignal:
    return MarketSignal(
        market_id=market_id,
        token_id=token_id,
        venue="polymarket",
        title="Will runner reject oversize share quantity?",
        yes_price=0.001,
        volume_24h=1_000.0,
        resolves_at=datetime(2026, 6, 30, tzinfo=UTC),
        orderbook={"bids": [], "asks": []},
        external_signal={},
        fetched_at=datetime(2026, 6, 1, 12, 0, tzinfo=UTC),
        market_status="open",
    )


def _runner(*, max_open_positions: int | None = 2) -> Runner:
    settings = PMSSettings(
        risk=RiskSettings(
            max_open_positions=max_open_positions,
            max_quantity_shares=500.0,
        ),
    )
    return Runner(config=settings, historical_data_path=FIXTURE_PATH)


class _RecordingDecisionStore:
    def __init__(self) -> None:
        self.transitions: list[tuple[str, str, str, datetime]] = []

    async def update_status(
        self,
        decision_id: str,
        *,
        current_status: str,
        next_status: str,
        updated_at: datetime,
    ) -> bool:
        self.transitions.append(
            (decision_id, current_status, next_status, updated_at)
        )
        return True


class _RecordingDedupStore:
    def __init__(self) -> None:
        self.release_calls: list[tuple[str, str]] = []

    async def release(self, decision_id: str, outcome: str) -> None:
        self.release_calls.append((decision_id, outcome))


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

    def test_pre_actuator_diagnostic_rejects_max_quantity_before_queue(self) -> None:
        runner = _runner(max_open_positions=50)
        signal = _signal()
        runner._remember_paper_orderbook(signal)
        decision = replace(
            _decision(),
            notional_usdc=1.0,
            limit_price=0.001,
        )

        diagnostic = runner._pre_actuator_diagnostic(decision, signal)

        assert diagnostic is not None
        assert diagnostic.code == "max_quantity_shares"
        assert diagnostic.metadata["estimated_quantity_shares"] == pytest.approx(
            1000.0
        )
        assert diagnostic.metadata["max_quantity_shares"] == pytest.approx(500.0)

    def test_estimated_quantity_uses_decimal_internal_arithmetic(self) -> None:
        decision = replace(
            _decision(),
            notional_usdc=0.3,
            limit_price=0.1,
        )

        assert _estimated_decision_quantity(decision) == 3.0

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

        enqueued = await runner._enqueue_decision(second, signal=None)

        assert enqueued is False
        assert runner._decision_queue.empty()

    @pytest.mark.asyncio
    async def test_enqueue_rejects_and_releases_accepted_decision_when_capacity_fills(
        self,
    ) -> None:
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
        queued_at = datetime(2026, 6, 1, 12, 0, tzinfo=UTC)
        decision_store = _RecordingDecisionStore()
        dedup_store = _RecordingDedupStore()
        runner.decision_store = decision_store  # type: ignore[assignment]
        runner.actuator_executor.dedup_store = dedup_store  # type: ignore[assignment]
        assert runner._reserve_position_capacity(first) is not None

        enqueued = await runner._enqueue_decision(
            second,
            signal=None,
            dedup_acquired=True,
            queued_at=queued_at,
        )

        assert enqueued is False
        assert runner._decision_queue.empty()
        assert decision_store.transitions == [
            ("decision-cap-2", "accepted", "rejected", queued_at)
        ]
        assert dedup_store.release_calls == [("decision-cap-2", "rejected")]

    @pytest.mark.asyncio
    async def test_enqueue_rejects_and_releases_risk_rejected_decision(self) -> None:
        runner = _runner(max_open_positions=50)
        queued_at = datetime(2026, 6, 1, 12, 0, tzinfo=UTC)
        decision_store = _RecordingDecisionStore()
        dedup_store = _RecordingDedupStore()
        runner.decision_store = decision_store  # type: ignore[assignment]
        runner.actuator_executor.dedup_store = dedup_store  # type: ignore[assignment]
        decision = replace(
            _decision(),
            notional_usdc=1.0,
            limit_price=0.001,
        )

        enqueued = await runner._enqueue_decision(
            decision,
            signal=None,
            dedup_acquired=True,
            queued_at=queued_at,
        )

        assert enqueued is False
        assert runner._decision_queue.empty()
        assert decision_store.transitions == [
            ("decision-cap", "accepted", "rejected", queued_at)
        ]
        assert dedup_store.release_calls == [("decision-cap", "rejected")]
