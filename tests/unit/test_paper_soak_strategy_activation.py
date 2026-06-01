from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, cast

import pytest

from pms.config import PMSSettings
from pms.core.enums import RunMode
from pms.runner import Runner
from pms.strategies.aggregate import Strategy
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
