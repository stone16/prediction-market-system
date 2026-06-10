from __future__ import annotations

from datetime import UTC, datetime

import pytest

from pms.strategies.projections import StrategyVersion
from pms.strategies.wc_flb_tail_fade import build_wc_flb_tail_fade_strategy
from scripts import install_wc_flb_tail_fade_strategy


def test_wc_flb_tail_fade_factor_rows_exclude_runtime_rules() -> None:
    strategy = build_wc_flb_tail_fade_strategy()

    factor_rows = install_wc_flb_tail_fade_strategy._strategy_factor_steps(strategy)

    assert tuple(step.factor_id for step in factor_rows) == (
        "favorite_longshot_bias",
        "orderbook_imbalance",
    )


def test_install_wc_flb_tail_fade_requires_database_url(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.delenv("DATABASE_URL", raising=False)

    result = install_wc_flb_tail_fade_strategy.main([])

    captured = capsys.readouterr()
    assert result == 2
    assert "DATABASE_URL is not set" in captured.err


def test_install_wc_flb_tail_fade_prints_registered_version(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    observed_archive_default: list[bool] = []

    def _fake_install(
        database_url: str,
        *,
        archive_default: bool = False,
    ) -> StrategyVersion:
        assert database_url == "postgresql://example/pms"
        observed_archive_default.append(archive_default)
        return StrategyVersion(
            strategy_id="wc_flb_tail_fade_v1",
            strategy_version_id="version-789",
            created_at=datetime(2026, 6, 10, 13, 0, tzinfo=UTC),
        )

    monkeypatch.setattr(
        install_wc_flb_tail_fade_strategy,
        "_run_install",
        _fake_install,
    )

    result = install_wc_flb_tail_fade_strategy.main(
        [
            "--database-url",
            "postgresql://example/pms",
            "--archive-default",
        ]
    )

    captured = capsys.readouterr()
    assert result == 0
    assert observed_archive_default == [True]
    assert "strategy_id: wc_flb_tail_fade_v1" in captured.out
    assert "strategy_version_id: version-789" in captured.out
    assert "archived_default: true" in captured.out
