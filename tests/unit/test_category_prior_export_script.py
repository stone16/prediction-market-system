from __future__ import annotations

import stat
from pathlib import Path

import pytest

from scripts import export_category_prior_observations as exporter
from scripts.export_category_prior_observations import (
    CategoryPriorCsvRow,
    export_category_prior_observations,
    main,
    observation_row_from_gamma_market,
)


def test_observation_row_from_gamma_market_exports_binary_yes_resolution() -> None:
    row = observation_row_from_gamma_market(
        {
            "conditionId": "0xmarket",
            "category": "Politics",
            "closedTime": "2026-06-02 05:14:32+00",
            "outcomes": '["Yes", "No"]',
            "outcomePrices": '["1", "0"]',
        }
    )

    assert row == CategoryPriorCsvRow(
        market_id="0xmarket",
        category="Politics",
        yes_payout="1",
        no_payout="0",
        resolved_at="2026-06-02T05:14:32Z",
    )


def test_observation_row_from_gamma_market_falls_back_to_series_slug() -> None:
    row = observation_row_from_gamma_market(
        {
            "conditionId": "0xmarket",
            "closedTime": "2026-06-02T05:14:32Z",
            "outcomes": '["Yes", "No"]',
            "outcomePrices": '["0", "1"]',
            "events": [
                {
                    "slug": "event-slug",
                    "series": [{"slug": "legislation-signed"}],
                }
            ],
        }
    )

    assert row is not None
    assert row.category == "legislation-signed"
    assert row.yes_payout == "0"
    assert row.no_payout == "1"


def test_observation_row_from_gamma_market_skips_unresolved_prices() -> None:
    row = observation_row_from_gamma_market(
        {
            "conditionId": "0xmarket",
            "category": "Politics",
            "closedTime": "2026-06-02T05:14:32Z",
            "outcomes": '["Yes", "No"]',
            "outcomePrices": '["0.5", "0.5"]',
        }
    )

    assert row is None


def test_observation_row_from_gamma_market_skips_non_binary_markets() -> None:
    row = observation_row_from_gamma_market(
        {
            "conditionId": "0xmarket",
            "category": "Politics",
            "closedTime": "2026-06-02T05:14:32Z",
            "outcomes": '["A", "B", "C"]',
            "outcomePrices": '["1", "0", "0"]',
        }
    )

    assert row is None


@pytest.mark.parametrize(
    "malformed_field",
    [
        {"outcomes": '["Yes",'},
        {"outcomePrices": '["1",'},
    ],
)
def test_observation_row_from_gamma_market_skips_malformed_json_sequences(
    malformed_field: dict[str, str],
) -> None:
    row = observation_row_from_gamma_market(
        {
            "conditionId": "0xmarket",
            "category": "Politics",
            "closedTime": "2026-06-02T05:14:32Z",
            "outcomes": '["Yes", "No"]',
            "outcomePrices": '["1", "0"]',
            **malformed_field,
        }
    )

    assert row is None


def test_export_category_prior_observations_publishes_private_artifact(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output_path = tmp_path / "secure" / "category-prior-observations.csv"

    monkeypatch.setattr(
        exporter,
        "_fetch_closed_market_page",
        lambda _client, *, limit, offset: [
            {
                "conditionId": "market-1",
                "category": "Politics",
                "closedTime": "2026-06-02T05:14:32Z",
                "outcomes": '["Yes", "No"]',
                "outcomePrices": '["1", "0"]',
            },
            {
                "conditionId": "market-2",
                "category": "Politics",
                "closedTime": "2026-06-02T05:15:32Z",
                "outcomes": '["Yes", "No"]',
                "outcomePrices": '["0", "1"]',
            },
        ],
    )

    stats = export_category_prior_observations(
        output_path=output_path,
        gamma_base_url="https://gamma.example.test",
        page_limit=2,
        max_pages=1,
        min_observations=2,
    )

    assert stats.written == 2
    assert stat.S_IMODE(output_path.parent.stat().st_mode) == 0o700
    assert stat.S_IMODE(output_path.stat().st_mode) == 0o600


def test_export_category_prior_observations_reports_expanded_output_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    home_path = tmp_path / "home"
    output_path = Path("~/secure/category-prior-observations.csv")
    expanded_output_path = home_path / "secure" / "category-prior-observations.csv"
    monkeypatch.setenv("HOME", str(home_path))
    monkeypatch.setattr(
        exporter,
        "_fetch_closed_market_page",
        lambda _client, *, limit, offset: [
            {
                "conditionId": "market-1",
                "category": "Politics",
                "closedTime": "2026-06-02T05:14:32Z",
                "outcomes": '["Yes", "No"]',
                "outcomePrices": '["1", "0"]',
            },
        ],
    )

    stats = export_category_prior_observations(
        output_path=output_path,
        gamma_base_url="https://gamma.example.test",
        page_limit=1,
        max_pages=1,
        min_observations=1,
    )

    assert expanded_output_path.exists()
    assert stats.output_path == expanded_output_path


def test_export_category_prior_observations_rejects_permissive_parent_without_publishing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output_dir = tmp_path / "permissive"
    output_dir.mkdir()
    output_dir.chmod(0o755)
    output_path = output_dir / "category-prior-observations.csv"
    output_path.write_text("existing artifact\n", encoding="utf-8")

    monkeypatch.setattr(
        exporter,
        "_fetch_closed_market_page",
        lambda _client, *, limit, offset: [
            {
                "conditionId": "market-1",
                "category": "Politics",
                "closedTime": "2026-06-02T05:14:32Z",
                "outcomes": '["Yes", "No"]',
                "outcomePrices": '["1", "0"]',
            },
        ],
    )

    with pytest.raises(ValueError, match="too permissive"):
        export_category_prior_observations(
            output_path=output_path,
            gamma_base_url="https://gamma.example.test",
            page_limit=1,
            max_pages=1,
            min_observations=1,
        )

    assert output_path.read_text(encoding="utf-8") == "existing artifact\n"


def test_export_category_prior_observations_rejects_output_inside_working_tree(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo_dir = tmp_path / "repo"
    repo_dir.mkdir()
    (repo_dir / ".git").mkdir()
    output_dir = repo_dir / "secure"
    output_dir.mkdir(mode=0o700)
    output_path = output_dir / "category-prior-observations.csv"
    monkeypatch.chdir(repo_dir)
    monkeypatch.setattr(
        exporter,
        "_fetch_closed_market_page",
        lambda _client, *, limit, offset: [
            {
                "conditionId": "market-1",
                "category": "Politics",
                "closedTime": "2026-06-02T05:14:32Z",
                "outcomes": '["Yes", "No"]',
                "outcomePrices": '["1", "0"]',
            },
        ],
    )

    with pytest.raises(ValueError, match="outside the working tree"):
        export_category_prior_observations(
            output_path=output_path,
            gamma_base_url="https://gamma.example.test",
            page_limit=1,
            max_pages=1,
            min_observations=1,
        )

    assert not output_path.exists()


def test_export_category_prior_observations_validates_runtime_artifact_before_publish(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output_dir = tmp_path / "secure"
    output_dir.mkdir(mode=0o700)
    output_path = output_dir / "category-prior-observations.csv"
    output_path.write_text("existing artifact\n", encoding="utf-8")
    output_path.chmod(0o600)

    monkeypatch.setattr(
        exporter,
        "_fetch_closed_market_page",
        lambda _client, *, limit, offset: [{"conditionId": "market-1"}],
    )
    monkeypatch.setattr(
        exporter,
        "observation_row_from_gamma_market",
        lambda _market: CategoryPriorCsvRow(
            market_id="market-1",
            category="",
            yes_payout="1",
            no_payout="0",
            resolved_at="2026-06-02T05:14:32Z",
        ),
    )

    with pytest.raises(ValueError, match="empty required column value 'category'"):
        export_category_prior_observations(
            output_path=output_path,
            gamma_base_url="https://gamma.example.test",
            page_limit=1,
            max_pages=1,
            min_observations=1,
        )

    assert output_path.read_text(encoding="utf-8") == "existing artifact\n"


def test_category_prior_export_cli_reports_operator_errors_without_traceback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    output_path = tmp_path / "secure" / "category-prior-observations.csv"
    monkeypatch.setattr(
        exporter,
        "_fetch_closed_market_page",
        lambda _client, *, limit, offset: [],
    )

    exit_code = main([
        "--output",
        str(output_path),
        "--gamma-base-url",
        "https://gamma.example.test",
        "--max-pages",
        "1",
        "--min-observations",
        "1",
    ])

    captured = capsys.readouterr()
    assert exit_code == 2
    assert "ERROR: insufficient category-prior observations exported: 0 < 1" in (
        captured.err
    )
    assert "Traceback" not in captured.err
    assert not output_path.exists()
