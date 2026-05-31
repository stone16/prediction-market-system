from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest

from pms.strategies.paper_multifactor import build_paper_multi_factor_strategy
from pms.strategies.projections import StrategyVersion
from scripts import install_paper_multi_factor_strategy


def test_paper_multi_factor_strategy_factor_rows_exclude_runtime_rules() -> None:
    strategy = build_paper_multi_factor_strategy()

    factor_rows = install_paper_multi_factor_strategy._strategy_factor_steps(strategy)

    assert tuple(step.factor_id for step in factor_rows) == (
        "orderbook_imbalance",
        "metaculus_prior",
        "favorite_longshot_bias",
    )


def test_install_paper_multi_factor_requires_database_url(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.delenv("DATABASE_URL", raising=False)

    result = install_paper_multi_factor_strategy.main([])

    captured = capsys.readouterr()
    assert result == 2
    assert "DATABASE_URL is not set" in captured.err


def test_install_paper_multi_factor_prints_registered_version(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    observed_archive_default: list[bool] = []

    def _fake_install(database_url: str, *, archive_default: bool = False) -> StrategyVersion:
        assert database_url == "postgresql://example/pms"
        observed_archive_default.append(archive_default)
        return StrategyVersion(
            strategy_id="paper_multi_factor_v1",
            strategy_version_id="version-456",
            created_at=datetime(2026, 5, 5, 13, 0, tzinfo=UTC),
        )

    monkeypatch.setattr(
        install_paper_multi_factor_strategy,
        "_run_install",
        _fake_install,
    )

    result = install_paper_multi_factor_strategy.main(
        ["--database-url", "postgresql://example/pms"]
    )

    captured = capsys.readouterr()
    assert result == 0
    assert observed_archive_default == [False]
    assert "strategy_id: paper_multi_factor_v1" in captured.out
    assert "strategy_version_id: version-456" in captured.out


def test_install_paper_multi_factor_can_archive_default_for_exclusive_paper_soak(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    observed_archive_default: list[bool] = []

    def _fake_install(database_url: str, *, archive_default: bool = False) -> StrategyVersion:
        assert database_url == "postgresql://example/pms"
        observed_archive_default.append(archive_default)
        return StrategyVersion(
            strategy_id="paper_multi_factor_v1",
            strategy_version_id="version-456",
            created_at=datetime(2026, 5, 5, 13, 0, tzinfo=UTC),
        )

    monkeypatch.setattr(
        install_paper_multi_factor_strategy,
        "_run_install",
        _fake_install,
    )

    result = install_paper_multi_factor_strategy.main(
        [
            "--database-url",
            "postgresql://example/pms",
            "--archive-default",
        ]
    )

    captured = capsys.readouterr()
    assert result == 0
    assert observed_archive_default == [True]
    assert "archived_default: true" in captured.out


@pytest.mark.asyncio
async def test_install_paper_multi_factor_ensures_factor_catalog_before_populating(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []

    class FakePool:
        async def close(self) -> None:
            events.append("close_pool")

    class FakeRegistry:
        def __init__(self, pool: FakePool) -> None:
            events.append("registry")
            self.pool = pool

        async def create_version(self, strategy: Any) -> StrategyVersion:
            del strategy
            events.append("create_version")
            return StrategyVersion(
                strategy_id="paper_multi_factor_v1",
                strategy_version_id="version-456",
                created_at=datetime(2026, 5, 5, 13, 0, tzinfo=UTC),
            )

        async def populate_strategy_factors(
            self,
            strategy_id: str,
            strategy_version_id: str,
            steps: Any,
        ) -> None:
            del strategy_id, strategy_version_id, steps
            events.append("populate_strategy_factors")

    async def fake_create_pool(**kwargs: Any) -> FakePool:
        assert kwargs["dsn"] == "postgresql://example/pms"
        events.append("create_pool")
        return FakePool()

    async def fake_ensure_factor_catalog(
        pool: FakePool,
        *,
        factor_ids: tuple[str, ...],
    ) -> None:
        del pool
        assert "favorite_longshot_bias" in factor_ids
        events.append("ensure_factor_catalog")

    monkeypatch.setattr(
        "scripts.install_paper_multi_factor_strategy.asyncpg.create_pool",
        fake_create_pool,
    )
    monkeypatch.setattr(
        install_paper_multi_factor_strategy,
        "PostgresStrategyRegistry",
        FakeRegistry,
    )
    monkeypatch.setattr(
        install_paper_multi_factor_strategy,
        "ensure_factor_catalog",
        fake_ensure_factor_catalog,
    )

    await install_paper_multi_factor_strategy.install_paper_multi_factor_strategy(
        "postgresql://example/pms"
    )

    assert events == [
        "create_pool",
        "registry",
        "create_version",
        "ensure_factor_catalog",
        "populate_strategy_factors",
        "close_pool",
    ]
