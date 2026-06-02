from __future__ import annotations

import os
from datetime import UTC, datetime
from pathlib import Path

import pytest

from pms.config import ControllerSettings, PMSSettings
from pms.controller.baselines import (
    CategoryPriorBaselineEstimator,
    CategoryPriorObservation,
    enrich_signal_with_category_prior,
    load_category_prior_observations_csv,
)
from pms.core.enums import MarketStatus
from pms.core.models import MarketSignal
from pms.runner import Runner


def _observation(
    category: str,
    outcome: float,
    *,
    resolved_at: datetime,
) -> CategoryPriorObservation:
    return CategoryPriorObservation(
        category=category,
        resolved_outcome=outcome,
        resolved_at=resolved_at,
    )


def _valid_category_prior_csv() -> str:
    return "\n".join(
        (
            "market_id,category,yes_payout,no_payout,resolved_at",
            "m-1,politics,1,0,2026-05-01T12:00:00Z",
            "m-2,sports,0,1,2026-05-02T12:00:00Z",
        )
    )


def _signal(
    *,
    category: str | None = "politics",
    fetched_at: datetime = datetime(2026, 5, 10, tzinfo=UTC),
    external_signal: dict[str, object] | None = None,
) -> MarketSignal:
    payload: dict[str, object] = {}
    if category is not None:
        payload["category"] = category
    if external_signal is not None:
        payload.update(external_signal)
    return MarketSignal(
        market_id="market-category-prior",
        token_id="token-yes",
        venue="polymarket",
        title="Will category prior be calibrated?",
        yes_price=0.41,
        volume_24h=1000.0,
        resolves_at=datetime(2026, 6, 1, tzinfo=UTC),
        orderbook={"bids": [], "asks": []},
        external_signal=payload,
        fetched_at=fetched_at,
        market_status=MarketStatus.OPEN.value,
    )


def test_category_prior_estimator_uses_only_past_resolved_category_outcomes() -> None:
    estimator = CategoryPriorBaselineEstimator(
        observations=(
            _observation(
                "politics",
                1.0,
                resolved_at=datetime(2026, 5, 1, tzinfo=UTC),
            ),
            _observation(
                "politics",
                0.0,
                resolved_at=datetime(2026, 5, 2, tzinfo=UTC),
            ),
            _observation(
                "politics",
                1.0,
                resolved_at=datetime(2026, 5, 10, tzinfo=UTC),
            ),
            _observation(
                "sports",
                0.0,
                resolved_at=datetime(2026, 5, 1, tzinfo=UTC),
            ),
        ),
        min_category_samples=2,
        min_global_samples=1,
        smoothing_alpha=1.0,
        smoothing_beta=1.0,
    )

    estimate = estimator.estimate(_signal(fetched_at=datetime(2026, 5, 10, tzinfo=UTC)))

    assert estimate is not None
    assert estimate.source == "category"
    assert estimate.category == "politics"
    assert estimate.sample_count == 2
    assert estimate.probability == pytest.approx(0.5)


def test_category_prior_estimator_falls_back_to_global_when_category_is_thin() -> None:
    estimator = CategoryPriorBaselineEstimator(
        observations=(
            _observation(
                "politics",
                1.0,
                resolved_at=datetime(2026, 5, 1, tzinfo=UTC),
            ),
            _observation(
                "sports",
                0.0,
                resolved_at=datetime(2026, 5, 1, tzinfo=UTC),
            ),
            _observation(
                "crypto",
                0.0,
                resolved_at=datetime(2026, 5, 2, tzinfo=UTC),
            ),
        ),
        min_category_samples=2,
        min_global_samples=3,
        smoothing_alpha=1.0,
        smoothing_beta=1.0,
    )

    estimate = estimator.estimate(_signal(category="politics"))

    assert estimate is not None
    assert estimate.source == "global"
    assert estimate.category == "politics"
    assert estimate.sample_count == 3
    assert estimate.probability == pytest.approx(0.4)


def test_category_prior_estimator_uses_risk_group_key_for_global_fallback() -> None:
    estimator = CategoryPriorBaselineEstimator(
        observations=(
            _observation(
                "politics",
                1.0,
                resolved_at=datetime(2026, 5, 1, tzinfo=UTC),
            ),
            _observation(
                "sports",
                0.0,
                resolved_at=datetime(2026, 5, 1, tzinfo=UTC),
            ),
            _observation(
                "crypto",
                0.0,
                resolved_at=datetime(2026, 5, 2, tzinfo=UTC),
            ),
        ),
        min_category_samples=2,
        min_global_samples=3,
        smoothing_alpha=1.0,
        smoothing_beta=1.0,
    )

    estimate = estimator.estimate(
        _signal(category=None, external_signal={"risk_group_id": "event:106520"})
    )

    assert estimate is not None
    assert estimate.source == "global"
    assert estimate.category == "event:106520"
    assert estimate.sample_count == 3
    assert estimate.probability == pytest.approx(0.4)


def test_category_prior_estimator_returns_none_when_no_safe_history_exists() -> None:
    estimator = CategoryPriorBaselineEstimator(
        observations=(
            _observation(
                "politics",
                1.0,
                resolved_at=datetime(2026, 5, 10, tzinfo=UTC),
            ),
        ),
        min_category_samples=1,
        min_global_samples=1,
    )

    estimate = estimator.estimate(_signal(fetched_at=datetime(2026, 5, 10, tzinfo=UTC)))

    assert estimate is None


def test_enrich_signal_with_category_prior_adds_decision_time_evidence() -> None:
    estimator = CategoryPriorBaselineEstimator(
        observations=(
            _observation(
                "politics",
                1.0,
                resolved_at=datetime(2026, 5, 1, tzinfo=UTC),
            ),
            _observation(
                "politics",
                0.0,
                resolved_at=datetime(2026, 5, 2, tzinfo=UTC),
            ),
        ),
        min_category_samples=2,
    )

    enriched = enrich_signal_with_category_prior(_signal(), estimator)

    assert enriched.external_signal["category_prior_baseline_prob_estimate"] == (
        pytest.approx(0.5)
    )
    assert enriched.external_signal["category_prior_baseline_source"] == "category"
    assert enriched.external_signal["category_prior_baseline_sample_count"] == 2


def test_enrich_signal_with_category_prior_preserves_existing_prior() -> None:
    estimator = CategoryPriorBaselineEstimator(
        observations=(
            _observation(
                "politics",
                0.0,
                resolved_at=datetime(2026, 5, 1, tzinfo=UTC),
            ),
            _observation(
                "politics",
                0.0,
                resolved_at=datetime(2026, 5, 2, tzinfo=UTC),
            ),
        ),
        min_category_samples=2,
    )
    signal = _signal(
        external_signal={"category_prior_baseline_prob_estimate": 0.72},
    )

    enriched = enrich_signal_with_category_prior(signal, estimator)

    assert enriched.external_signal["category_prior_baseline_prob_estimate"] == 0.72


def test_load_category_prior_observations_csv_parses_strict_resolution_export(
    tmp_path: Path,
) -> None:
    export_path = tmp_path / "category-prior.csv"
    export_path.write_text(
        "\n".join(
            (
                "market_id,category,yes_payout,no_payout,resolved_at",
                "m-1, Politics ,1,0,2026-05-01T12:00:00Z",
                "m-2,sports,0,1,2026-05-02T12:00:00+00:00",
                "m-3,politics,0.5,0.5,2026-05-03T12:00:00Z",
            )
        ),
        encoding="utf-8",
    )

    loaded = load_category_prior_observations_csv(export_path)

    assert loaded.skipped_ambiguous_count == 1
    assert [observation.category for observation in loaded.observations] == [
        "politics",
        "sports",
    ]
    assert [observation.resolved_outcome for observation in loaded.observations] == [
        1.0,
        0.0,
    ]
    assert loaded.observations[0].resolved_at == datetime(
        2026,
        5,
        1,
        12,
        tzinfo=UTC,
    )


def test_load_category_prior_observations_csv_rejects_symlink_path(
    tmp_path: Path,
) -> None:
    target_path = tmp_path / "target-category-prior.csv"
    target_path.write_text(_valid_category_prior_csv(), encoding="utf-8")
    export_path = tmp_path / "category-prior.csv"
    export_path.symlink_to(target_path)

    with pytest.raises(ValueError, match="cannot be read safely"):
        load_category_prior_observations_csv(export_path)


def test_load_category_prior_observations_csv_opens_model_with_no_follow_when_available(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    no_follow_flag = getattr(os, "O_NOFOLLOW", 0)
    if no_follow_flag == 0:
        pytest.skip("os.O_NOFOLLOW is unavailable on this platform")

    export_path = tmp_path / "category-prior.csv"
    export_path.write_text(_valid_category_prior_csv(), encoding="utf-8")
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

    loaded = load_category_prior_observations_csv(export_path)

    observed_by_path = {path: flags for path, flags in observed}
    assert len(loaded.observations) == 2
    assert observed_by_path[export_path] & no_follow_flag


def test_load_category_prior_observations_csv_rejects_hardlink_swap_during_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    export_path = tmp_path / "category-prior.csv"
    export_path.write_text(_valid_category_prior_csv(), encoding="utf-8")
    replacement_source = tmp_path / "replacement-category-prior.csv"
    replacement_source.write_text(_valid_category_prior_csv(), encoding="utf-8")
    real_open = os.open
    swapped = False

    def swapping_open(
        path_arg: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        flags: int,
        mode: int = 0o777,
    ) -> int:
        nonlocal swapped
        observed_path = Path(os.fsdecode(os.fspath(path_arg)))
        if observed_path == export_path and not swapped:
            swapped = True
            export_path.unlink()
            os.link(replacement_source, export_path)
        return real_open(path_arg, flags, mode)

    monkeypatch.setattr(os, "open", swapping_open)

    with pytest.raises(ValueError, match="cannot be read safely"):
        load_category_prior_observations_csv(export_path)

    assert swapped is True


def test_load_category_prior_observations_csv_rejects_missing_columns(
    tmp_path: Path,
) -> None:
    export_path = tmp_path / "category-prior.csv"
    export_path.write_text(
        "\n".join(
            (
                "market_id,category,yes_payout,resolved_at",
                "m-1,politics,1,2026-05-01T12:00:00Z",
            )
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="missing required columns"):
        load_category_prior_observations_csv(export_path)


def test_load_category_prior_observations_csv_rejects_price_like_payouts(
    tmp_path: Path,
) -> None:
    export_path = tmp_path / "category-prior.csv"
    export_path.write_text(
        "\n".join(
            (
                "market_id,category,yes_payout,no_payout,resolved_at",
                "m-1,politics,0.99,0.01,2026-05-01T12:00:00Z",
            )
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="settled payout vector"):
        load_category_prior_observations_csv(export_path)


def test_load_category_prior_observations_csv_rejects_duplicate_markets(
    tmp_path: Path,
) -> None:
    export_path = tmp_path / "category-prior.csv"
    export_path.write_text(
        "\n".join(
            (
                "market_id,category,yes_payout,no_payout,resolved_at",
                "m-1,politics,1,0,2026-05-01T12:00:00Z",
                "m-1,politics,0,1,2026-05-02T12:00:00Z",
            )
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="duplicate market_id"):
        load_category_prior_observations_csv(export_path)


def test_load_category_prior_observations_csv_rejects_duplicate_header(
    tmp_path: Path,
) -> None:
    export_path = tmp_path / "category-prior.csv"
    export_path.write_text(
        "\n".join(
            (
                "market_id,category,category,yes_payout,no_payout,resolved_at",
                "m-1,politics,shadowed,1,0,2026-05-01T12:00:00Z",
            )
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="duplicate CSV column: category"):
        load_category_prior_observations_csv(export_path)


def test_runner_loads_configured_category_prior_csv_for_decision_evidence(
    tmp_path: Path,
) -> None:
    export_path = tmp_path / "category-prior.csv"
    export_path.write_text(
        "\n".join(
            (
                "market_id,category,yes_payout,no_payout,resolved_at",
                "m-1,politics,1,0,2026-05-01T12:00:00Z",
                "m-2,politics,0,1,2026-05-02T12:00:00Z",
            )
        ),
        encoding="utf-8",
    )
    runner = Runner(
        config=PMSSettings(
            controller=ControllerSettings(
                category_prior_observations_path=str(export_path),
                category_prior_min_category_samples=2,
            ),
        ),
    )

    enriched = runner._enrich_signal_with_controller_baselines(_signal())

    assert enriched.external_signal["category_prior_baseline_prob_estimate"] == (
        pytest.approx(0.5)
    )
    assert enriched.external_signal["category_prior_baseline_source"] == "category"
