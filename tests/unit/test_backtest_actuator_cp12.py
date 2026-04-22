from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Literal, cast

import pytest

from pms.actuator.adapters.backtest import BacktestActuator
from pms.actuator.adapters.paper import PaperActuator
from pms.actuator.risk import InsufficientLiquidityError
from pms.core.enums import OrderStatus, TimeInForce
from pms.core.models import OrderState, Portfolio, TradeDecision


def _portfolio() -> Portfolio:
    return Portfolio(
        total_usdc=1_000.0,
        free_usdc=1_000.0,
        locked_usdc=0.0,
        open_positions=[],
    )


def _decision(
    *,
    market_id: str = "market-cp12",
    token_id: str = "token-yes",
    notional_usdc: float = 100.0,
    limit_price: float = 0.25,
    outcome: Literal["YES", "NO"] = "YES",
) -> TradeDecision:
    return TradeDecision(
        decision_id=f"decision-{market_id}",
        market_id=market_id,
        token_id=token_id,
        venue="polymarket",
        side="BUY",
        notional_usdc=notional_usdc,
        order_type="limit",
        max_slippage_bps=100,
        stop_conditions=["cp12"],
        prob_estimate=0.7,
        expected_edge=0.2,
        time_in_force=TimeInForce.GTC,
        opportunity_id=f"opportunity-{market_id}",
        strategy_id="alpha",
        strategy_version_id="alpha-v1",
        action="BUY",
        limit_price=limit_price,
        outcome=outcome,
        model_id="rules",
    )


def _invalid_decision(*, notional_usdc: float) -> TradeDecision:
    return cast(
        TradeDecision,
        SimpleNamespace(
            decision_id="invalid-decision",
            market_id="market-cp12",
            token_id="token-yes",
            venue="polymarket",
            side="BUY",
            notional_usdc=notional_usdc,
            order_type="limit",
            max_slippage_bps=100,
            stop_conditions=["cp12"],
            prob_estimate=0.7,
            expected_edge=0.2,
            time_in_force="GTC",
            opportunity_id="opportunity-invalid",
            strategy_id="alpha",
            strategy_version_id="alpha-v1",
            action="BUY",
            limit_price=0.25,
            outcome="YES",
            model_id="rules",
        ),
    )


def _fixture(
    tmp_path: Path,
    *,
    price: float = 0.25,
    size: float = 1_000.0,
    token_id: str = "token-yes",
    timestamp: str | None = None,
) -> Path:
    fixture = tmp_path / "cp12-orderbook.jsonl"
    row: dict[str, Any] = {
        "market_id": "market-cp12",
        "token_id": token_id,
        "orderbook": {
            "bids": [{"price": 0.24, "size": size}],
            "asks": [{"price": price, "size": size}],
        },
    }
    if timestamp is not None:
        row["ts"] = timestamp
    fixture.write_text(
        json.dumps(row)
        + "\n",
        encoding="utf-8",
    )
    return fixture


class _RecordingSimulator:
    def __init__(self) -> None:
        self.signals: list[Any] = []

    async def execute(
        self,
        *,
        signal: Any,
        decision: TradeDecision,
        portfolio: Portfolio | None = None,
        execution_model: Any,
    ) -> OrderState:
        del portfolio, execution_model
        self.signals.append(signal)
        return OrderState(
            order_id=f"order-{decision.token_id}",
            decision_id=decision.decision_id,
            status=OrderStatus.MATCHED.value,
            market_id=decision.market_id,
            token_id=decision.token_id,
            venue=decision.venue,
            requested_notional_usdc=decision.notional_usdc,
            filled_notional_usdc=decision.notional_usdc,
            remaining_notional_usdc=0.0,
            fill_price=decision.limit_price,
            submitted_at=signal.fetched_at,
            last_updated_at=signal.fetched_at,
            raw_status="matched",
            strategy_id=decision.strategy_id,
            strategy_version_id=decision.strategy_version_id,
            filled_quantity=decision.notional_usdc / decision.limit_price,
        )


@pytest.mark.asyncio
async def test_backtest_actuator_matches_notional_and_quantity(tmp_path: Path) -> None:
    fixture_path = _fixture(tmp_path)
    actuator = BacktestActuator(fixture_path)
    paper = PaperActuator(
        orderbooks={
            "market-cp12": {
                "bids": [{"price": 0.24, "size": 1_000.0}],
                "asks": [{"price": 0.25, "size": 1_000.0}],
            }
        }
    )

    decision = _decision()
    expected = await paper.execute(decision, _portfolio())
    state = await actuator.execute(decision, _portfolio())

    assert state.fill_price == pytest.approx(0.25)
    assert state.requested_notional_usdc == pytest.approx(100.0)
    assert state.filled_notional_usdc == pytest.approx(100.0)
    assert state.remaining_notional_usdc == pytest.approx(0.0)
    assert state.filled_quantity == pytest.approx(400.0)
    assert state.fill_price == pytest.approx(expected.fill_price)
    assert state.filled_notional_usdc == pytest.approx(expected.filled_notional_usdc)
    assert state.filled_quantity == pytest.approx(expected.filled_quantity)


@pytest.mark.asyncio
async def test_backtest_actuator_rejects_zero_notional_bypass(tmp_path: Path) -> None:
    actuator = BacktestActuator(_fixture(tmp_path))

    with pytest.raises(InsufficientLiquidityError):
        await actuator.execute(_invalid_decision(notional_usdc=0.0), _portfolio())


@pytest.mark.asyncio
async def test_backtest_actuator_rejects_zero_fill_price(tmp_path: Path) -> None:
    actuator = BacktestActuator(_fixture(tmp_path, price=0.0))

    with pytest.raises(InsufficientLiquidityError):
        await actuator.execute(_decision(limit_price=0.01), _portfolio())


@pytest.mark.asyncio
async def test_backtest_actuator_uses_token_scoped_orderbooks_and_fixture_timestamps(
    tmp_path: Path,
) -> None:
    fixture = tmp_path / "cp12-multi-token.jsonl"
    fixture.write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "market_id": "market-cp12",
                        "token_id": "token-yes",
                        "ts": "2026-04-20T12:00:00Z",
                        "orderbook": {
                            "bids": [{"price": 0.24, "size": 1_000.0}],
                            "asks": [{"price": 0.25, "size": 1_000.0}],
                        },
                    }
                ),
                json.dumps(
                    {
                        "market_id": "market-cp12",
                        "token_id": "token-no",
                        "ts": "2026-04-20T12:05:00Z",
                        "orderbook": {
                            "bids": [{"price": 0.71, "size": 1_000.0}],
                            "asks": [{"price": 0.73, "size": 1_000.0}],
                        },
                    }
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    simulator = _RecordingSimulator()
    actuator = BacktestActuator(fixture, simulator=cast(Any, simulator))

    await actuator.execute(_decision(token_id="token-yes", limit_price=0.25), _portfolio())
    await actuator.execute(
        _decision(token_id="token-no", limit_price=0.73, outcome="NO"),
        _portfolio(),
    )

    assert simulator.signals[0].orderbook["asks"][0]["price"] == pytest.approx(0.25)
    assert simulator.signals[0].fetched_at == datetime(2026, 4, 20, 12, 0, tzinfo=UTC)
    assert simulator.signals[1].orderbook["asks"][0]["price"] == pytest.approx(0.73)
    assert simulator.signals[1].fetched_at == datetime(2026, 4, 20, 12, 5, tzinfo=UTC)
