from __future__ import annotations

import os
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast
import tomllib

import pytest

from pms.core.models import Portfolio
from pms.strategies.base import (
    StrategyAgent,
    StrategyController,
    StrategyModule,
    StrategyObservationSource,
)
from pms.strategies.flb.agent import FlbAgent
from pms.strategies.flb.controller import FlbController
from pms.strategies.flb.source import (
    FLB_RESEARCH_REF,
    FlbCalibrationModel,
    FlbPositionSizer,
    FlbSignalCalibration,
    LiveFlbSource,
    FlbMarketSnapshot,
    load_flb_calibration_csv,
)
from pms.strategies.flb.strategy import FlbStrategyModule
from pms.strategies.intents import StrategyContext, TradeIntent


NOW = datetime(2026, 5, 3, 12, 0, tzinfo=UTC)
ROOT = Path(__file__).resolve().parents[2]


def _valid_flb_calibration_csv() -> str:
    return "\n".join(
        (
            "signal_name,probability_estimate,sample_count,source_label",
            "longshot_yes_overpriced_buy_no,0.99,150,warehouse-flb-v1",
            "favorite_yes_underpriced_buy_yes,0.97,151,warehouse-flb-v1",
        )
    )


def _context() -> StrategyContext:
    return StrategyContext(
        strategy_id="h1_flb",
        strategy_version_id="h1-flb-v1",
        as_of=NOW,
    )


def _portfolio(free_usdc: float = 100.0) -> Portfolio:
    return Portfolio(
        total_usdc=free_usdc,
        free_usdc=free_usdc,
        locked_usdc=0.0,
        open_positions=[],
    )


class _FixedSizer:
    def __init__(self, notional_usdc: float = 5.0) -> None:
        self.notional_usdc = notional_usdc
        self.calls: list[tuple[float, float]] = []

    def size(
        self,
        *,
        prob: float,
        market_price: float,
        portfolio: Portfolio,
    ) -> float:
        del portfolio
        self.calls.append((prob, market_price))
        return self.notional_usdc


class _ZeroSizer:
    def size(
        self,
        *,
        prob: float,
        market_price: float,
        portfolio: Portfolio,
    ) -> float:
        del prob, market_price, portfolio
        return 0.0


class _StaticMarketReader:
    def __init__(self, market: FlbMarketSnapshot | None) -> None:
        self.market = market
        self.calls: list[tuple[str, datetime]] = []

    async def latest(
        self,
        market_id: str,
        *,
        as_of: datetime,
    ) -> FlbMarketSnapshot | None:
        self.calls.append((market_id, as_of))
        return self.market


def _market(**overrides: object) -> FlbMarketSnapshot:
    data: dict[str, object] = {
        "market_id": "market-flb-1",
        "title": "Will the H1 FLB strategy choose the contrarian side?",
        "yes_token_id": "token-yes",
        "no_token_id": "token-no",
        "yes_price": 0.05,
        "observed_at": NOW,
        "yes_best_ask": 0.05,
        "no_best_ask": 0.96,
        "resolves_at": NOW + timedelta(days=7),
    }
    data.update(overrides)
    return FlbMarketSnapshot(**cast(Any, data))


def _module(
    market: FlbMarketSnapshot | None,
    *,
    sizer: FlbPositionSizer | None = None,
    calibration_model: FlbCalibrationModel | None = None,
    entry_execution_cost_bps: float = 0.0,
    fee_rate: float = 0.0,
) -> FlbStrategyModule:
    return FlbStrategyModule(
        source=LiveFlbSource(
            market_ids=("market-flb-1",),
            market_reader=_StaticMarketReader(market),
            position_sizer=sizer or _FixedSizer(),
            portfolio=_portfolio(),
            calibration_model=calibration_model,
            entry_execution_cost_bps=entry_execution_cost_bps,
            fee_rate=fee_rate,
        ),
        controller=FlbController(),
        agent=FlbAgent(),
        strategy_id="h1_flb",
        strategy_version_id="h1-flb-v1",
    )


def _calibration_model(
    *,
    longshot_probability: float = 0.99,
    favorite_probability: float = 0.97,
) -> FlbCalibrationModel:
    return FlbCalibrationModel(
        calibrations=(
            FlbSignalCalibration(
                signal_name="longshot_yes_overpriced_buy_no",
                probability_estimate=longshot_probability,
                sample_count=150,
                source_label="warehouse-flb-v1",
            ),
            FlbSignalCalibration(
                signal_name="favorite_yes_underpriced_buy_yes",
                probability_estimate=favorite_probability,
                sample_count=150,
                source_label="warehouse-flb-v1",
            ),
        ),
    )


def test_flb_components_satisfy_strategy_protocols() -> None:
    module = _module(_market())

    source: StrategyObservationSource = module.source
    controller: StrategyController = module.controller
    agent: StrategyAgent = module.agent
    strategy_module: StrategyModule = module

    assert source is module.source
    assert controller is module.controller
    assert agent is module.agent
    assert strategy_module.strategy_id == "h1_flb"


@pytest.mark.asyncio
async def test_flb_longshot_signal_buys_no_contract() -> None:
    sizer = _FixedSizer(5.0)
    module = _module(_market(yes_price=0.05, no_best_ask=0.96), sizer=sizer)

    intents = await module.run(_context())

    assert len(intents) == 1
    intent = intents[0]
    assert isinstance(intent, TradeIntent)
    assert intent.outcome == "NO"
    assert intent.side == "BUY"
    assert intent.token_id == "token-no"
    assert intent.limit_price == pytest.approx(0.96)
    assert intent.expected_price == pytest.approx(0.98)
    assert intent.expected_edge == pytest.approx(0.02)
    assert intent.notional_usdc == pytest.approx(5.0)
    assert len(sizer.calls) == 1
    assert sizer.calls[0][0] == pytest.approx(0.98)
    assert sizer.calls[0][1] == pytest.approx(0.96)
    assert FLB_RESEARCH_REF in intent.evidence_refs


@pytest.mark.asyncio
async def test_flb_favorite_signal_buys_yes_contract() -> None:
    module = _module(_market(yes_price=0.95, yes_best_ask=0.94))

    intents = await module.run(_context())

    assert len(intents) == 1
    intent = intents[0]
    assert isinstance(intent, TradeIntent)
    assert intent.outcome == "YES"
    assert intent.side == "BUY"
    assert intent.token_id == "token-yes"
    assert intent.limit_price == pytest.approx(0.94)
    assert intent.expected_price == pytest.approx(0.96)


@pytest.mark.asyncio
async def test_flb_source_uses_configured_calibration_model_probability() -> None:
    sizer = _FixedSizer(5.0)
    module = _module(
        _market(yes_price=0.05, no_best_ask=0.96),
        sizer=sizer,
        calibration_model=_calibration_model(),
    )

    intents = await module.run(_context())

    assert len(intents) == 1
    intent = intents[0]
    assert isinstance(intent, TradeIntent)
    assert intent.expected_price == pytest.approx(0.99)
    assert intent.expected_edge == pytest.approx(0.03)
    assert len(sizer.calls) == 1
    assert sizer.calls[0][0] == pytest.approx(0.99)
    assert sizer.calls[0][1] == pytest.approx(0.96)
    assert (
        "flb_calibration_model:warehouse-flb-v1:longshot_yes_overpriced_buy_no"
        in intent.evidence_refs
    )


@pytest.mark.asyncio
async def test_flb_source_suppresses_calibrated_signal_below_edge_gate() -> None:
    sizer = _FixedSizer(5.0)
    module = _module(
        _market(yes_price=0.05, no_best_ask=0.96),
        sizer=sizer,
        calibration_model=_calibration_model(longshot_probability=0.965),
    )

    assert await module.run(_context()) == ()
    assert sizer.calls == []


@pytest.mark.asyncio
async def test_flb_source_reports_net_edge_after_entry_costs() -> None:
    module = _module(
        _market(yes_price=0.05, no_best_ask=0.96),
        calibration_model=_calibration_model(longshot_probability=0.99),
        entry_execution_cost_bps=50.0,
        fee_rate=0.04,
    )

    observations = await module.source.observe(_context())
    intents = await module.run(_context())

    assert len(observations) == 1
    payload = observations[0].payload
    metadata = payload["metadata"]
    assert payload["expected_edge"] == pytest.approx(0.0234)
    assert metadata["gross_expected_edge"] == pytest.approx(0.03)
    assert metadata["entry_execution_cost_bps"] == pytest.approx(50.0)
    assert metadata["entry_execution_cost_edge"] == pytest.approx(0.005)
    assert metadata["fee_rate"] == pytest.approx(0.04)
    assert metadata["fee_edge"] == pytest.approx(0.0016)
    assert metadata["net_expected_edge"] == pytest.approx(0.0234)
    assert len(intents) == 1
    intent = intents[0]
    assert isinstance(intent, TradeIntent)
    assert intent.expected_edge == pytest.approx(0.0234)


@pytest.mark.asyncio
async def test_flb_source_suppresses_signal_when_entry_costs_erase_edge() -> None:
    sizer = _FixedSizer(5.0)
    module = _module(
        _market(yes_price=0.05, no_best_ask=0.96),
        sizer=sizer,
        calibration_model=_calibration_model(longshot_probability=0.985),
        entry_execution_cost_bps=50.0,
        fee_rate=0.04,
    )

    assert await module.run(_context()) == ()
    assert sizer.calls == []


@pytest.mark.asyncio
async def test_flb_source_ignores_middle_decile_markets() -> None:
    module = _module(_market(yes_price=0.50, yes_best_ask=0.50, no_best_ask=0.50))

    assert await module.run(_context()) == ()


@pytest.mark.asyncio
async def test_flb_source_suppresses_zero_sized_trades() -> None:
    module = _module(_market(), sizer=_ZeroSizer())

    assert await module.run(_context()) == ()


@pytest.mark.asyncio
async def test_flb_source_skips_resolved_markets() -> None:
    module = _module(_market(resolves_at=NOW - timedelta(minutes=1)))

    assert await module.run(_context()) == ()


@pytest.mark.asyncio
async def test_flb_module_rejects_context_strategy_mismatch() -> None:
    module = _module(_market())
    context = StrategyContext(
        strategy_id="other",
        strategy_version_id="h1-flb-v1",
        as_of=NOW,
    )

    with pytest.raises(ValueError, match="context strategy identity must match"):
        await module.run(context)


def test_strategy_import_linter_contract_includes_flb_plugin() -> None:
    pyproject = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    contracts: Sequence[dict[str, object]] = pyproject["tool"]["importlinter"][
        "contracts"
    ]
    contract = next(
        candidate
        for candidate in contracts
        if candidate["name"] == "Strategy plugins: no actuator, controller, or venue adapter imports"
    )

    assert contract["source_modules"] == [
        "pms.strategies.ripple",
        "pms.strategies.flb",
        "pms.strategies.anchoring",
    ]


def test_load_flb_calibration_csv_parses_model(tmp_path: Path) -> None:
    model_path = tmp_path / "flb-calibration.csv"
    model_path.write_text(_valid_flb_calibration_csv(), encoding="utf-8")

    model = load_flb_calibration_csv(model_path)

    longshot = model.calibration_for("longshot_yes_overpriced_buy_no")
    assert longshot.probability_estimate == pytest.approx(0.99)
    assert longshot.sample_count == 150


def test_load_flb_calibration_csv_rejects_symlink_path(tmp_path: Path) -> None:
    target_path = tmp_path / "target-flb-calibration.csv"
    target_path.write_text(_valid_flb_calibration_csv(), encoding="utf-8")
    model_path = tmp_path / "flb-calibration.csv"
    model_path.symlink_to(target_path)

    with pytest.raises(ValueError, match="cannot be read safely"):
        load_flb_calibration_csv(model_path)


def test_load_flb_calibration_csv_opens_model_with_no_follow_when_available(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    no_follow_flag = getattr(os, "O_NOFOLLOW", 0)
    if no_follow_flag == 0:
        pytest.skip("os.O_NOFOLLOW is unavailable on this platform")

    model_path = tmp_path / "flb-calibration.csv"
    model_path.write_text(_valid_flb_calibration_csv(), encoding="utf-8")
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

    model = load_flb_calibration_csv(model_path)

    observed_by_path = {path: flags for path, flags in observed}
    assert model.calibration_for("longshot_yes_overpriced_buy_no").sample_count == 150
    assert observed_by_path[model_path] & no_follow_flag


def test_load_flb_calibration_csv_rejects_hardlink_swap_during_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    model_path = tmp_path / "flb-calibration.csv"
    model_path.write_text(_valid_flb_calibration_csv(), encoding="utf-8")
    replacement_source = tmp_path / "replacement-flb-calibration.csv"
    replacement_source.write_text(_valid_flb_calibration_csv(), encoding="utf-8")
    real_open = os.open
    swapped = False

    def swapping_open(
        path_arg: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        flags: int,
        mode: int = 0o777,
    ) -> int:
        nonlocal swapped
        observed_path = Path(os.fsdecode(os.fspath(path_arg)))
        if observed_path == model_path and not swapped:
            swapped = True
            model_path.unlink()
            os.link(replacement_source, model_path)
        return real_open(path_arg, flags, mode)

    monkeypatch.setattr(os, "open", swapping_open)

    with pytest.raises(ValueError, match="cannot be read safely"):
        load_flb_calibration_csv(model_path)

    assert swapped is True


def test_load_flb_calibration_csv_rejects_missing_target_signal(tmp_path: Path) -> None:
    model_path = tmp_path / "flb-calibration.csv"
    model_path.write_text(
        "\n".join(
            (
                "signal_name,probability_estimate,sample_count,source_label",
                "longshot_yes_overpriced_buy_no,0.99,150,warehouse-flb-v1",
            )
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="missing calibrated FLB signals"):
        load_flb_calibration_csv(model_path)


def test_load_flb_calibration_csv_rejects_duplicate_header(tmp_path: Path) -> None:
    model_path = tmp_path / "flb-calibration.csv"
    model_path.write_text(
        "\n".join(
            (
                "signal_name,probability_estimate,sample_count,source_label,source_label",
                "longshot_yes_overpriced_buy_no,0.99,150,warehouse-flb-v1,shadowed",
                "favorite_yes_underpriced_buy_yes,0.97,151,warehouse-flb-v1,shadowed",
            )
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="duplicate CSV column: source_label"):
        load_flb_calibration_csv(model_path)


def test_load_flb_calibration_csv_rejects_low_sample_signal(tmp_path: Path) -> None:
    model_path = tmp_path / "flb-calibration.csv"
    model_path.write_text(
        "\n".join(
            (
                "signal_name,probability_estimate,sample_count,source_label",
                "longshot_yes_overpriced_buy_no,0.99,99,warehouse-flb-v1",
                "favorite_yes_underpriced_buy_yes,0.97,150,warehouse-flb-v1",
            )
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="sample_count"):
        load_flb_calibration_csv(model_path)
