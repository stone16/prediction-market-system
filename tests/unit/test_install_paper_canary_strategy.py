from __future__ import annotations

from datetime import UTC, datetime

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
    def _fake_install(database_url: str) -> StrategyVersion:
        assert database_url == "postgresql://example/pms"
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
    assert "strategy_id: paper_canary_v1" in captured.out
    assert "strategy_version_id: version-123" in captured.out
