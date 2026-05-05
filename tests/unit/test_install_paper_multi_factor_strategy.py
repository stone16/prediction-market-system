from __future__ import annotations

from datetime import UTC, datetime

import pytest

from pms.strategies.paper_multifactor import build_paper_multi_factor_strategy
from pms.strategies.projections import StrategyVersion
from scripts import install_paper_multi_factor_strategy


def test_paper_multi_factor_strategy_factor_rows_exclude_runtime_rules() -> None:
    strategy = build_paper_multi_factor_strategy()

    factor_rows = install_paper_multi_factor_strategy._strategy_factor_steps(strategy)

    assert tuple(step.factor_id for step in factor_rows) == (
        "orderbook_imbalance",
        "orderbook_imbalance",
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
    def _fake_install(database_url: str) -> StrategyVersion:
        assert database_url == "postgresql://example/pms"
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
    assert "strategy_id: paper_multi_factor_v1" in captured.out
    assert "strategy_version_id: version-456" in captured.out
