from __future__ import annotations

import json
import os
from pathlib import Path
import re

import pytest

from pms.actuator.adapters.backtest import BacktestActuator
from pms.actuator.adapters.backtest_fixtures import load_orderbook_snapshots
from pms.actuator.executor import ActuatorAdapter
from pms.core.enums import TimeInForce
from pms.core.models import Portfolio, TradeDecision


ROOT = Path(__file__).resolve().parents[2]
BACKTEST_ACTUATOR_PATH = ROOT / "src" / "pms" / "actuator" / "adapters" / "backtest.py"


def _fixture(tmp_path: Path) -> Path:
    fixture = tmp_path / "backtest-orderbook.jsonl"
    _write_fixture(fixture)
    return fixture


def _write_fixture(fixture: Path) -> None:
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


def test_load_orderbook_snapshots_rejects_symlink_fixture(tmp_path: Path) -> None:
    target_path = tmp_path / "target-orderbook.jsonl"
    _write_fixture(target_path)
    fixture = tmp_path / "backtest-orderbook.jsonl"
    fixture.symlink_to(target_path)

    with pytest.raises(ValueError, match="backtest fixture cannot be read safely"):
        load_orderbook_snapshots(fixture)


def test_load_orderbook_snapshots_opens_fixture_with_no_follow_when_available(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    no_follow_flag = getattr(os, "O_NOFOLLOW", 0)
    if no_follow_flag == 0:
        pytest.skip("os.O_NOFOLLOW is unavailable on this platform")

    fixture = _fixture(tmp_path)
    observed: list[tuple[Path, int]] = []
    real_open = os.open

    def recording_open(
        path_arg: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        flags: int,
        mode: int = 0o777,
    ) -> int:
        observed.append((Path(os.fsdecode(os.fspath(path_arg))), flags))
        return real_open(path_arg, flags, mode)

    monkeypatch.setattr(os, "open", recording_open)

    snapshots = load_orderbook_snapshots(fixture)

    observed_by_path = {observed_path: flags for observed_path, flags in observed}
    assert ("delegation-market", "") in snapshots
    assert observed_by_path[fixture] & no_follow_flag


def test_load_orderbook_snapshots_rejects_hardlink_swap_during_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _fixture(tmp_path)
    replacement_source = tmp_path / "replacement-orderbook.jsonl"
    _write_fixture(replacement_source)
    real_open = os.open
    swapped = False

    def swapping_open(
        path_arg: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        flags: int,
        mode: int = 0o777,
    ) -> int:
        nonlocal swapped
        observed_path = Path(os.fsdecode(os.fspath(path_arg)))
        if observed_path == fixture and not swapped:
            swapped = True
            fixture.unlink()
            os.link(replacement_source, fixture)
        return real_open(path_arg, flags, mode)

    monkeypatch.setattr(os, "open", swapping_open)

    with pytest.raises(ValueError, match="backtest fixture cannot be read safely"):
        load_orderbook_snapshots(fixture)

    assert swapped is True


@pytest.mark.asyncio
async def test_backtest_actuator_is_runtime_checkable_and_delegates(tmp_path: Path) -> None:
    actuator = BacktestActuator(_fixture(tmp_path))

    assert isinstance(actuator, ActuatorAdapter)

    state = await actuator.execute(_decision(), _portfolio())

    assert state.status == "matched"
    assert state.filled_notional_usdc == pytest.approx(50.0)
