from __future__ import annotations

from datetime import UTC, datetime
from hashlib import sha256
import json
from pathlib import Path
from typing import Any, cast

import pytest

from pms.config import PMSSettings, StrategyRuntimeSettings
from pms.core.enums import RunMode
from pms.runner import Runner
from pms.strategies.aggregate import Strategy
from pms.strategies.flb.projection import H1_FLB_STRATEGY_ID
from pms.strategies.paper_multifactor import PAPER_MULTI_FACTOR_STRATEGY_ID
from pms.strategies.projections import FactorCompositionStep, StrategyVersion


class _FakeStrategyRegistry:
    def __init__(self) -> None:
        self.created: list[tuple[Strategy, bool]] = []
        self.populated: list[tuple[str, str, tuple[FactorCompositionStep, ...]]] = []
        self.archived: list[str] = []
        self.active: list[tuple[str, str]] = []

    async def create_version(
        self,
        strategy: Strategy,
        *,
        activate: bool = True,
    ) -> StrategyVersion:
        self.created.append((strategy, activate))
        return StrategyVersion(
            strategy_id=strategy.config.strategy_id,
            strategy_version_id="paper-multi-factor-test-version",
            created_at=datetime(2026, 6, 1, tzinfo=UTC),
        )

    async def populate_strategy_factors(
        self,
        strategy_id: str,
        strategy_version_id: str,
        steps: tuple[FactorCompositionStep, ...],
    ) -> None:
        self.populated.append((strategy_id, strategy_version_id, steps))

    async def archive_strategy(self, strategy_id: str) -> None:
        self.archived.append(strategy_id)

    async def set_active(self, strategy_id: str, strategy_version_id: str) -> None:
        self.active.append((strategy_id, strategy_version_id))


@pytest.mark.asyncio
async def test_runner_installs_configured_paper_multi_factor_strategy_before_soak() -> None:
    runner = Runner(
        config=PMSSettings(
            mode=RunMode.PAPER,
            paper_soak_strategy_id="paper_multi_factor_v1",
            paper_soak_archive_default=True,
        )
    )
    registry = _FakeStrategyRegistry()
    runner._strategy_registry = cast(Any, registry)  # noqa: SLF001

    await runner._ensure_paper_soak_strategy()  # noqa: SLF001

    assert len(registry.created) == 1
    strategy, activate = registry.created[0]
    assert strategy.config.strategy_id == PAPER_MULTI_FACTOR_STRATEGY_ID
    assert activate is False
    assert registry.archived == ["default"]
    assert registry.active == [
        (PAPER_MULTI_FACTOR_STRATEGY_ID, "paper-multi-factor-test-version")
    ]
    assert registry.populated
    populated_strategy_id, populated_version_id, steps = registry.populated[0]
    assert populated_strategy_id == PAPER_MULTI_FACTOR_STRATEGY_ID
    assert populated_version_id == "paper-multi-factor-test-version"
    assert {step.factor_id for step in steps} == {
        "favorite_longshot_bias",
        "metaculus_prior",
        "orderbook_imbalance",
    }


def _write_flb_calibration(path: Path) -> Path:
    path.write_text(
        "\n".join(
            (
                "signal_name,probability_estimate,sample_count,source_label",
                "longshot_yes_overpriced_buy_no,0.99,150,warehouse-flb-v1",
                "favorite_yes_underpriced_buy_yes,0.97,151,warehouse-flb-v1",
            )
        )
        + "\n",
        encoding="utf-8",
    )
    Path(f"{path}.provenance.json").write_text(
        json.dumps(
            {
                "artifact_type": "flb_calibration_provenance",
                "generated_by": "scripts/flb_data_feasibility.py",
                "source": "warehouse-csv",
                "generated_at": "2026-06-01T00:00:00+00:00",
                "warehouse_csv_sha256": sha256(
                    b"unit warehouse provenance fixture"
                ).hexdigest(),
                "warehouse_market_count": 301,
                "warehouse_longshot_count": 150,
                "warehouse_favorite_count": 151,
                "calibration_csv_sha256": sha256(path.read_bytes()).hexdigest(),
                "calibration_source_label": "warehouse-flb-v1",
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    return path


@pytest.mark.asyncio
async def test_runner_rejects_h1_flb_paper_soak_without_calibration_path() -> None:
    runner = Runner(
        config=PMSSettings(
            mode=RunMode.PAPER,
            paper_soak_strategy_id="h1_flb",
            paper_soak_archive_default=True,
        )
    )
    runner._strategy_registry = cast(Any, _FakeStrategyRegistry())  # noqa: SLF001

    with pytest.raises(ValueError, match="h1_flb paper soak requires"):
        await runner._ensure_paper_soak_strategy()  # noqa: SLF001


@pytest.mark.asyncio
async def test_runner_installs_configured_h1_flb_strategy_before_soak(
    tmp_path: Path,
) -> None:
    calibration_path = _write_flb_calibration(tmp_path / "flb-calibration.csv")
    runner = Runner(
        config=PMSSettings(
            mode=RunMode.PAPER,
            paper_soak_strategy_id="h1_flb",
            paper_soak_archive_default=True,
            strategies=StrategyRuntimeSettings(
                flb_calibration_path=str(calibration_path),
            ),
        )
    )
    registry = _FakeStrategyRegistry()
    runner._strategy_registry = cast(Any, registry)  # noqa: SLF001

    await runner._ensure_paper_soak_strategy()  # noqa: SLF001

    assert len(registry.created) == 1
    strategy, activate = registry.created[0]
    metadata = dict(strategy.config.metadata)
    assert strategy.config.strategy_id == H1_FLB_STRATEGY_ID
    assert metadata["live_allowed"] == "true"
    assert metadata["alpha_source"] == "warehouse_flb_decile_model_v1"
    assert metadata["edge_model_source"] == "flb_calibration_model_v1"
    assert metadata["calibration_source"] == "warehouse_flb_v1"
    assert strategy.calibration.enabled is True
    assert strategy.forecaster.forecasters == (("flb", ()),)
    assert activate is False
    assert registry.archived == ["default"]
    assert registry.active == [(H1_FLB_STRATEGY_ID, "paper-multi-factor-test-version")]
    assert registry.populated == [(H1_FLB_STRATEGY_ID, "paper-multi-factor-test-version", ())]
