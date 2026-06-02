from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest

from pms.strategies.projections import StrategyVersion
from scripts import install_paper_canary_strategy


def test_install_paper_canary_requires_database_url(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.delenv("DATABASE_URL", raising=False)

    result = install_paper_canary_strategy.main([])

    captured = capsys.readouterr()
    assert result == 2
    assert "DATABASE_URL is not set" in captured.err


def test_install_paper_canary_prints_registered_version(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    observed_archive_default: list[bool] = []
    observed_sample_modulus: list[int] = []

    def _fake_install(
        database_url: str,
        *,
        archive_default: bool = False,
        sample_modulus: int = 25,
    ) -> StrategyVersion:
        assert database_url == "postgresql://example/pms"
        observed_archive_default.append(archive_default)
        observed_sample_modulus.append(sample_modulus)
        return StrategyVersion(
            strategy_id="paper_canary_v1",
            strategy_version_id="version-123",
            created_at=datetime(2026, 5, 5, 12, 0, tzinfo=UTC),
        )

    monkeypatch.setattr(
        install_paper_canary_strategy,
        "_run_install",
        _fake_install,
    )

    result = install_paper_canary_strategy.main(
        ["--database-url", "postgresql://example/pms"]
    )

    captured = capsys.readouterr()
    assert result == 0
    assert observed_archive_default == [False]
    assert observed_sample_modulus == [25]
    assert "strategy_id: paper_canary_v1" in captured.out
    assert "strategy_version_id: version-123" in captured.out
    assert "archived_default: false" in captured.out
    assert "sample_modulus: 25" in captured.out


def test_install_paper_canary_can_archive_default_for_exclusive_plumbing_smoke(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    observed_archive_default: list[bool] = []
    observed_sample_modulus: list[int] = []

    def _fake_install(
        database_url: str,
        *,
        archive_default: bool = False,
        sample_modulus: int = 25,
    ) -> StrategyVersion:
        assert database_url == "postgresql://example/pms"
        observed_archive_default.append(archive_default)
        observed_sample_modulus.append(sample_modulus)
        return StrategyVersion(
            strategy_id="paper_canary_v1",
            strategy_version_id="version-123",
            created_at=datetime(2026, 5, 5, 12, 0, tzinfo=UTC),
        )

    monkeypatch.setattr(
        install_paper_canary_strategy,
        "_run_install",
        _fake_install,
    )

    result = install_paper_canary_strategy.main(
        [
            "--database-url",
            "postgresql://example/pms",
            "--archive-default",
            "--sample-modulus",
            "1",
        ]
    )

    captured = capsys.readouterr()
    assert result == 0
    assert observed_archive_default == [True]
    assert observed_sample_modulus == [1]
    assert "archived_default: true" in captured.out
    assert "sample_modulus: 1" in captured.out


@pytest.mark.asyncio
async def test_install_paper_canary_continues_when_default_strategy_is_absent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []

    class FakePool:
        async def close(self) -> None:
            events.append("close_pool")

    class FakeRegistry:
        def __init__(self, pool: FakePool) -> None:
            del pool
            events.append("registry")

        async def create_version(
            self,
            strategy: Any,
            *,
            activate: bool = True,
        ) -> StrategyVersion:
            del strategy
            events.append(f"create_version:{'active' if activate else 'inactive'}")
            return StrategyVersion(
                strategy_id="paper_canary_v1",
                strategy_version_id="version-123",
                created_at=datetime(2026, 5, 5, 12, 0, tzinfo=UTC),
            )

        async def archive_strategy(self, strategy_id: str) -> None:
            assert strategy_id == "default"
            events.append("archive_default_missing")
            raise LookupError("default missing")

        async def set_active(
            self,
            strategy_id: str,
            strategy_version_id: str,
        ) -> None:
            assert strategy_id == "paper_canary_v1"
            assert strategy_version_id == "version-123"
            events.append("set_active")

    async def fake_create_pool(**kwargs: Any) -> FakePool:
        assert kwargs["dsn"] == "postgresql://example/pms"
        events.append("create_pool")
        return FakePool()

    monkeypatch.setattr(
        "scripts.install_paper_canary_strategy.asyncpg.create_pool",
        fake_create_pool,
    )
    monkeypatch.setattr(
        install_paper_canary_strategy,
        "PostgresStrategyRegistry",
        FakeRegistry,
    )

    await install_paper_canary_strategy.install_paper_canary_strategy(
        "postgresql://example/pms",
        archive_default=True,
    )

    assert events == [
        "create_pool",
        "registry",
        "create_version:inactive",
        "archive_default_missing",
        "set_active",
        "close_pool",
    ]


@pytest.mark.asyncio
async def test_install_paper_canary_can_create_unsampled_strategy_for_short_smoke(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed_sample_params: list[dict[str, str]] = []

    class FakePool:
        async def close(self) -> None:
            return None

    class FakeRegistry:
        def __init__(self, pool: FakePool) -> None:
            del pool

        async def create_version(
            self,
            strategy: Any,
            *,
            activate: bool = True,
        ) -> StrategyVersion:
            del activate
            observed_sample_params.append(dict(strategy.forecaster.forecasters[0][1]))
            observed_sample_params.append(dict(strategy.config.metadata))
            return StrategyVersion(
                strategy_id="paper_canary_v1",
                strategy_version_id="version-123",
                created_at=datetime(2026, 5, 5, 12, 0, tzinfo=UTC),
            )

        async def set_active(
            self,
            strategy_id: str,
            strategy_version_id: str,
        ) -> None:
            del strategy_id, strategy_version_id

    async def fake_create_pool(**kwargs: Any) -> FakePool:
        assert kwargs["dsn"] == "postgresql://example/pms"
        return FakePool()

    monkeypatch.setattr(
        "scripts.install_paper_canary_strategy.asyncpg.create_pool",
        fake_create_pool,
    )
    monkeypatch.setattr(
        install_paper_canary_strategy,
        "PostgresStrategyRegistry",
        FakeRegistry,
    )

    await install_paper_canary_strategy.install_paper_canary_strategy(
        "postgresql://example/pms",
        sample_modulus=1,
    )

    assert observed_sample_params == [
        {
            "edge_bps": "1000",
            "max_probability": "0.97",
            "min_price": "0.05",
            "max_price": "0.90",
            "sample_modulus": "1",
            "sample_remainder": "0",
        },
        {
            "owner": "system",
            "purpose": "paper_e2e_canary",
            "price_reference": "best_ask",
            "event_filter": "book",
            "sample": "0/1",
            "live_allowed": "false",
        },
    ]
