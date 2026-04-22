from __future__ import annotations

import json
from pathlib import Path
import re

import pytest

from pms.actuator.adapters.backtest import BacktestActuator
from pms.actuator.executor import ActuatorAdapter
from pms.core.enums import TimeInForce
from pms.core.models import Portfolio, TradeDecision


ROOT = Path(__file__).resolve().parents[2]
BACKTEST_ACTUATOR_PATH = ROOT / "src" / "pms" / "actuator" / "adapters" / "backtest.py"


def _fixture(tmp_path: Path) -> Path:
    fixture = tmp_path / "backtest-orderbook.jsonl"
    fixture.write_text(
        json.dumps(
            {
                "market_id": "delegation-market",
                "orderbook": {
                    "bids": [{"price": 0.24, "size": 500.0}],
                    "asks": [{"price": 0.25, "size": 500.0}],
                },
            }
        )
        + "\n",
        encoding="utf-8",
    )
    return fixture


def _decision() -> TradeDecision:
    return TradeDecision(
        decision_id="delegation-decision",
        market_id="delegation-market",
        token_id="delegation-token",
        venue="polymarket",
        side="BUY",
        notional_usdc=50.0,
        order_type="limit",
        max_slippage_bps=25,
        stop_conditions=[],
        prob_estimate=0.6,
        expected_edge=0.1,
        time_in_force=TimeInForce.IOC,
        opportunity_id="delegation-opp",
        strategy_id="alpha",
        strategy_version_id="alpha-v1",
        limit_price=0.30,
        action="BUY",
    )


def _portfolio() -> Portfolio:
    return Portfolio(
        total_usdc=1_000.0,
        free_usdc=1_000.0,
        locked_usdc=0.0,
        open_positions=[],
    )


def test_backtest_actuator_source_stays_thin() -> None:
    source = BACKTEST_ACTUATOR_PATH.read_text(encoding="utf-8")
    stripped_lines = [
        line
        for line in source.splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]
    assert len(stripped_lines) <= 60
    assert len(re.findall(r"^\s*(?:async\s+)?def\s+", source, flags=re.MULTILINE)) <= 3
    for forbidden in ("walk", "accumulate", "level_notional", "fill_quantity"):
        assert forbidden not in source


@pytest.mark.asyncio
async def test_backtest_actuator_is_runtime_checkable_and_delegates(tmp_path: Path) -> None:
    actuator = BacktestActuator(_fixture(tmp_path))

    assert isinstance(actuator, ActuatorAdapter)

    state = await actuator.execute(_decision(), _portfolio())

    assert state.status == "matched"
    assert state.filled_notional_usdc == pytest.approx(50.0)
