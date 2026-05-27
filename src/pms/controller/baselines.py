from __future__ import annotations

import csv
import io
import os
import stat
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from math import isfinite
from pathlib import Path
from typing import Literal

from pms.core.models import MarketSignal


CategoryPriorSource = Literal["category", "global"]
_CATEGORY_PRIOR_REQUIRED_COLUMNS = frozenset(
    {"market_id", "category", "yes_payout", "no_payout", "resolved_at"}
)


@dataclass(frozen=True, slots=True)
class CategoryPriorObservation:
    category: str
    resolved_outcome: float
    resolved_at: datetime

    def __post_init__(self) -> None:
        normalized_category = _normalize_category(self.category)
        if normalized_category is None:
            msg = "CategoryPriorObservation.category must be non-empty"
            raise ValueError(msg)
        if (
            not isfinite(self.resolved_outcome)
            or self.resolved_outcome < 0.0
            or self.resolved_outcome > 1.0
        ):
            msg = "CategoryPriorObservation.resolved_outcome must be in [0, 1]"
            raise ValueError(msg)
        object.__setattr__(self, "category", normalized_category)
        object.__setattr__(self, "resolved_at", _aware_utc(self.resolved_at))


@dataclass(frozen=True, slots=True)
class CategoryPriorObservationLoad:
    observations: tuple[CategoryPriorObservation, ...]
    skipped_ambiguous_count: int


@dataclass(frozen=True, slots=True)
class CategoryPriorEstimate:
    probability: float
    source: CategoryPriorSource
    category: str
    sample_count: int


@dataclass(frozen=True, slots=True)
class CategoryPriorBaselineEstimator:
    observations: Iterable[CategoryPriorObservation]
    min_category_samples: int = 20
    min_global_samples: int = 100
    smoothing_alpha: float = 1.0
    smoothing_beta: float = 1.0

    def __post_init__(self) -> None:
        if self.min_category_samples <= 0:
            msg = "min_category_samples must be positive"
            raise ValueError(msg)
        if self.min_global_samples <= 0:
            msg = "min_global_samples must be positive"
            raise ValueError(msg)
        if self.smoothing_alpha <= 0.0 or self.smoothing_beta <= 0.0:
            msg = "smoothing_alpha and smoothing_beta must be positive"
            raise ValueError(msg)
        object.__setattr__(self, "observations", tuple(self.observations))

    def estimate(self, signal: MarketSignal) -> CategoryPriorEstimate | None:
        category = _signal_category(signal)
        if category is None:
            return None

        as_of = _aware_utc(signal.fetched_at)
        eligible = [
            observation
            for observation in self.observations
            if observation.resolved_at < as_of
        ]
        category_observations = [
            observation
            for observation in eligible
            if observation.category == category
        ]
        if len(category_observations) >= self.min_category_samples:
            return CategoryPriorEstimate(
                probability=self._smoothed_rate(category_observations),
                source="category",
                category=category,
                sample_count=len(category_observations),
            )
        if len(eligible) >= self.min_global_samples:
            return CategoryPriorEstimate(
                probability=self._smoothed_rate(eligible),
                source="global",
                category=category,
                sample_count=len(eligible),
            )
        return None

    def _smoothed_rate(
        self,
        observations: list[CategoryPriorObservation],
    ) -> float:
        positive_outcomes = sum(observation.resolved_outcome for observation in observations)
        denominator = len(observations) + self.smoothing_alpha + self.smoothing_beta
        return (positive_outcomes + self.smoothing_alpha) / denominator


def enrich_signal_with_category_prior(
    signal: MarketSignal,
    estimator: CategoryPriorBaselineEstimator,
) -> MarketSignal:
    if "category_prior_baseline_prob_estimate" in signal.external_signal:
        return signal

    estimate = estimator.estimate(signal)
    if estimate is None:
        return signal

    external_signal = dict(signal.external_signal)
    external_signal.update(
        {
            "category_prior_baseline_prob_estimate": estimate.probability,
            "category_prior_baseline_source": estimate.source,
            "category_prior_baseline_category": estimate.category,
            "category_prior_baseline_sample_count": estimate.sample_count,
        }
    )
    return replace(signal, external_signal=external_signal)


def load_category_prior_observations_csv(
    path: str | Path,
) -> CategoryPriorObservationLoad:
    csv_path = Path(path).expanduser()
    if not csv_path.exists():
        msg = f"Category prior observations CSV does not exist: {csv_path}"
        raise ValueError(msg)

    observations: list[CategoryPriorObservation] = []
    seen_market_ids: set[str] = set()
    skipped_ambiguous_count = 0
    with io.StringIO(_read_text_no_follow(csv_path), newline="") as csv_file:
        reader = csv.DictReader(csv_file)
        if reader.fieldnames is None:
            msg = f"Category prior observations CSV is empty: {csv_path}"
            raise ValueError(msg)
        _require_unique_csv_fieldnames(reader.fieldnames)
        missing_columns = sorted(
            _CATEGORY_PRIOR_REQUIRED_COLUMNS.difference(reader.fieldnames)
        )
        if missing_columns:
            msg = (
                "Category prior observations CSV missing required columns: "
                f"{', '.join(missing_columns)}"
            )
            raise ValueError(msg)

        for row_number, row in enumerate(reader, start=2):
            market_id = _required_csv_text(
                row,
                "market_id",
                path=csv_path,
                row_number=row_number,
            )
            if market_id in seen_market_ids:
                msg = f"{csv_path}:{row_number}: duplicate market_id {market_id!r}"
                raise ValueError(msg)
            seen_market_ids.add(market_id)

            category = _required_csv_text(
                row,
                "category",
                path=csv_path,
                row_number=row_number,
            )
            yes_payout = _required_csv_decimal(
                row,
                "yes_payout",
                path=csv_path,
                row_number=row_number,
            )
            no_payout = _required_csv_decimal(
                row,
                "no_payout",
                path=csv_path,
                row_number=row_number,
            )
            resolved_outcome = _strict_resolved_outcome_from_payouts(
                yes_payout,
                no_payout,
                path=csv_path,
                row_number=row_number,
            )
            if resolved_outcome is None:
                skipped_ambiguous_count += 1
                continue
            observations.append(
                CategoryPriorObservation(
                    category=category,
                    resolved_outcome=resolved_outcome,
                    resolved_at=_parse_csv_datetime(
                        _required_csv_text(
                            row,
                            "resolved_at",
                            path=csv_path,
                            row_number=row_number,
                        ),
                        path=csv_path,
                        row_number=row_number,
                    ),
                )
            )

    return CategoryPriorObservationLoad(
        observations=tuple(observations),
        skipped_ambiguous_count=skipped_ambiguous_count,
    )


def _read_text_no_follow(path: Path) -> str:
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    fd = -1
    try:
        fd = os.open(path, flags, 0o777)
        path_stat = os.fstat(fd)
        if not stat.S_ISREG(path_stat.st_mode):
            raise OSError(
                f"Category prior observations CSV cannot be read safely: {path}"
            )
        if path_stat.st_nlink != 1:
            raise OSError(
                f"Category prior observations CSV cannot be read safely: {path}"
            )
        with os.fdopen(fd, "r", encoding="utf-8") as file:
            fd = -1
            return file.read()
    except OSError as exc:
        msg = f"Category prior observations CSV cannot be read safely: {path}"
        raise ValueError(msg) from exc
    finally:
        if fd >= 0:
            os.close(fd)


def _require_unique_csv_fieldnames(fieldnames: Sequence[str]) -> None:
    seen: set[str] = set()
    for fieldname in fieldnames:
        if fieldname in seen:
            msg = f"duplicate CSV column: {fieldname}"
            raise ValueError(msg)
        seen.add(fieldname)


def _signal_category(signal: MarketSignal) -> str | None:
    for key in ("category", "market_category"):
        value = signal.external_signal.get(key)
        if isinstance(value, str):
            category = _normalize_category(value)
            if category is not None:
                return category
    return None


def _normalize_category(value: str) -> str | None:
    normalized = value.strip().lower()
    return None if normalized == "" else normalized


def _aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _required_csv_text(
    row: dict[str, str | None],
    column: str,
    *,
    path: Path,
    row_number: int,
) -> str:
    raw_value = row.get(column)
    if raw_value is None:
        msg = f"{path}:{row_number}: missing required column value {column!r}"
        raise ValueError(msg)
    value = raw_value.strip()
    if value == "":
        msg = f"{path}:{row_number}: empty required column value {column!r}"
        raise ValueError(msg)
    return value


def _required_csv_decimal(
    row: dict[str, str | None],
    column: str,
    *,
    path: Path,
    row_number: int,
) -> Decimal:
    value = _required_csv_text(row, column, path=path, row_number=row_number)
    try:
        return Decimal(value)
    except InvalidOperation:
        msg = f"{path}:{row_number}: invalid decimal in {column!r}: {value!r}"
        raise ValueError(msg) from None


def _strict_resolved_outcome_from_payouts(
    yes_payout: Decimal,
    no_payout: Decimal,
    *,
    path: Path,
    row_number: int,
) -> float | None:
    if yes_payout == Decimal("1") and no_payout == Decimal("0"):
        return 1.0
    if yes_payout == Decimal("0") and no_payout == Decimal("1"):
        return 0.0
    if yes_payout == Decimal("0.5") and no_payout == Decimal("0.5"):
        return None
    msg = (
        f"{path}:{row_number}: expected settled payout vector "
        f"(1, 0), (0, 1), or ambiguous refund (0.5, 0.5); "
        f"got ({yes_payout}, {no_payout})"
    )
    raise ValueError(msg)


def _parse_csv_datetime(
    value: str,
    *,
    path: Path,
    row_number: int,
) -> datetime:
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        return _aware_utc(datetime.fromisoformat(normalized))
    except ValueError:
        msg = f"{path}:{row_number}: invalid ISO datetime: {value!r}"
        raise ValueError(msg) from None
