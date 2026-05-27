"""Observation source for the H1 favorite-longshot bias strategy."""

from __future__ import annotations

import csv
import io
import os
import stat
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime
from math import isfinite
from pathlib import Path
from typing import Literal, Protocol, cast

from pms.core.enums import TimeInForce
from pms.core.models import BookSide, Outcome, Portfolio, Venue
from pms.strategies.flb.evaluator import DEFAULT_MIN_EXPECTED_EDGE
from pms.strategies.intents import StrategyContext, StrategyObservation


FLB_RESEARCH_REF = "research:h1-flb-strategy#h1"
LIVE_FLB_SOURCE = "live_flb_market_source"
LONGSHOT_YES_THRESHOLD = 0.10
FAVORITE_YES_THRESHOLD = 0.90
DEFAULT_FLB_CONFIDENCE = 0.65
DEFAULT_FLB_CALIBRATION_MIN_SAMPLES = 100
DEFAULT_FLB_ENTRY_EXECUTION_COST_BPS = 15.0
DEFAULT_FLB_FEE_RATE = 0.04
FlbSignalName = Literal[
    "longshot_yes_overpriced_buy_no",
    "favorite_yes_underpriced_buy_yes",
]
_FLB_SIGNAL_NAMES = frozenset(
    {
        "longshot_yes_overpriced_buy_no",
        "favorite_yes_underpriced_buy_yes",
    }
)
_FLB_CALIBRATION_REQUIRED_COLUMNS = frozenset(
    {"signal_name", "probability_estimate", "sample_count", "source_label"}
)


class FlbMarketSnapshotReader(Protocol):
    async def latest(
        self,
        market_id: str,
        *,
        as_of: datetime,
    ) -> FlbMarketSnapshot | None: ...


class FlbPositionSizer(Protocol):
    def size(
        self,
        *,
        prob: float,
        market_price: float,
        portfolio: Portfolio,
    ) -> float: ...


@dataclass(frozen=True, slots=True)
class FlbSignalCalibration:
    signal_name: FlbSignalName
    probability_estimate: float
    sample_count: int
    source_label: str

    def __post_init__(self) -> None:
        if self.signal_name not in _FLB_SIGNAL_NAMES:
            msg = f"unsupported FLB signal_name: {self.signal_name!r}"
            raise ValueError(msg)
        _require_open_probability(self.probability_estimate, "probability_estimate")
        if self.sample_count <= 0:
            msg = "sample_count must be positive"
            raise ValueError(msg)
        _require_non_empty(self.source_label, "source_label")


@dataclass(frozen=True, slots=True)
class FlbCalibrationModel:
    calibrations: Sequence[FlbSignalCalibration]
    min_sample_count: int = DEFAULT_FLB_CALIBRATION_MIN_SAMPLES
    _by_signal: dict[FlbSignalName, FlbSignalCalibration] = field(
        init=False,
        repr=False,
        default_factory=dict,
    )

    def __post_init__(self) -> None:
        if self.min_sample_count <= 0:
            msg = "min_sample_count must be positive"
            raise ValueError(msg)
        by_signal: dict[FlbSignalName, FlbSignalCalibration] = {}
        for calibration in self.calibrations:
            if calibration.signal_name in by_signal:
                msg = f"duplicate FLB calibration for {calibration.signal_name!r}"
                raise ValueError(msg)
            if calibration.sample_count < self.min_sample_count:
                msg = (
                    f"FLB calibration sample_count for {calibration.signal_name!r} "
                    f"must be >= {self.min_sample_count}"
                )
                raise ValueError(msg)
            by_signal[calibration.signal_name] = calibration
        missing = sorted(_FLB_SIGNAL_NAMES.difference(by_signal))
        if missing:
            msg = f"missing calibrated FLB signals: {', '.join(missing)}"
            raise ValueError(msg)
        object.__setattr__(self, "_by_signal", by_signal)
        object.__setattr__(self, "calibrations", tuple(self.calibrations))

    def calibration_for(self, signal_name: FlbSignalName) -> FlbSignalCalibration:
        return self._by_signal[signal_name]


@dataclass(frozen=True, slots=True)
class FlbMarketSnapshot:
    market_id: str
    title: str
    yes_token_id: str
    no_token_id: str
    yes_price: float
    observed_at: datetime
    yes_best_ask: float | None = None
    no_best_ask: float | None = None
    resolves_at: datetime | None = None
    venue: Venue = "polymarket"

    def __post_init__(self) -> None:
        _require_non_empty(self.market_id, "market_id")
        _require_non_empty(self.title, "title")
        _require_non_empty(self.yes_token_id, "yes_token_id")
        _require_non_empty(self.no_token_id, "no_token_id")
        _require_open_probability(self.yes_price, "yes_price")
        if self.yes_best_ask is not None:
            _require_open_probability(self.yes_best_ask, "yes_best_ask")
        if self.no_best_ask is not None:
            _require_open_probability(self.no_best_ask, "no_best_ask")


@dataclass(frozen=True, slots=True)
class LiveFlbSource:
    """Market-price source for H1 FLB signals.

    This source implements only H1 bucket semantics from the research brief.
    When a warehouse calibration model is supplied, it uses model probabilities
    and suppresses signals below the edge gate. Without a model, the legacy
    limit-price-plus-edge path remains paper-plumbing only. H2 anchoring-lag /
    news replay remains out of scope until the H1 data spine is proven viable.
    """

    market_ids: Sequence[str]
    market_reader: FlbMarketSnapshotReader
    position_sizer: FlbPositionSizer
    portfolio: Portfolio
    min_expected_edge: float = DEFAULT_MIN_EXPECTED_EDGE
    longshot_yes_threshold: float = LONGSHOT_YES_THRESHOLD
    favorite_yes_threshold: float = FAVORITE_YES_THRESHOLD
    confidence: float = DEFAULT_FLB_CONFIDENCE
    max_slippage_bps: int = 50
    entry_execution_cost_bps: float = DEFAULT_FLB_ENTRY_EXECUTION_COST_BPS
    fee_rate: float = DEFAULT_FLB_FEE_RATE
    time_in_force: TimeInForce = TimeInForce.GTC
    calibration_model: FlbCalibrationModel | None = None

    def __post_init__(self) -> None:
        if not self.market_ids:
            msg = "market_ids must not be empty"
            raise ValueError(msg)
        for market_id in self.market_ids:
            _require_non_empty(market_id, "market_id")
        if self.min_expected_edge <= 0.0:
            msg = "min_expected_edge must be > 0.0"
            raise ValueError(msg)
        _require_open_probability(self.longshot_yes_threshold, "longshot_yes_threshold")
        _require_open_probability(self.favorite_yes_threshold, "favorite_yes_threshold")
        if self.longshot_yes_threshold >= self.favorite_yes_threshold:
            msg = "longshot_yes_threshold must be below favorite_yes_threshold"
            raise ValueError(msg)
        if not 0.0 <= self.confidence <= 1.0:
            msg = "confidence must satisfy 0.0 <= confidence <= 1.0"
            raise ValueError(msg)
        if self.max_slippage_bps < 0:
            msg = "max_slippage_bps must be >= 0"
            raise ValueError(msg)
        _require_nonnegative_finite(
            self.entry_execution_cost_bps,
            "entry_execution_cost_bps",
        )
        _require_unit_interval_closed(self.fee_rate, "fee_rate")

    async def observe(self, context: StrategyContext) -> Sequence[StrategyObservation]:
        observations: list[StrategyObservation] = []
        for market_id in self.market_ids:
            market = await self.market_reader.latest(market_id, as_of=context.as_of)
            if market is None or _is_resolved(context.as_of, market.resolves_at):
                continue
            observation = _observation_from_market(
                context=context,
                market=market,
                position_sizer=self.position_sizer,
                portfolio=self.portfolio,
                min_expected_edge=self.min_expected_edge,
                longshot_yes_threshold=self.longshot_yes_threshold,
                favorite_yes_threshold=self.favorite_yes_threshold,
                confidence=self.confidence,
                max_slippage_bps=self.max_slippage_bps,
                entry_execution_cost_bps=self.entry_execution_cost_bps,
                fee_rate=self.fee_rate,
                time_in_force=self.time_in_force,
                calibration_model=self.calibration_model,
            )
            if observation is not None:
                observations.append(observation)
        return tuple(observations)


@dataclass(frozen=True, slots=True)
class _FlbSignal:
    signal_name: FlbSignalName
    thesis: str
    token_id: str
    outcome: Outcome
    side: BookSide
    limit_price: float
    probability_estimate: float
    probability_source: str
    calibration_source: str | None = None
    calibration_sample_count: int | None = None


def _observation_from_market(
    *,
    context: StrategyContext,
    market: FlbMarketSnapshot,
    position_sizer: FlbPositionSizer,
    portfolio: Portfolio,
    min_expected_edge: float,
    longshot_yes_threshold: float,
    favorite_yes_threshold: float,
    confidence: float,
    max_slippage_bps: int,
    entry_execution_cost_bps: float,
    fee_rate: float,
    time_in_force: TimeInForce,
    calibration_model: FlbCalibrationModel | None,
) -> StrategyObservation | None:
    signal = _classify_market(
        market=market,
        min_expected_edge=min_expected_edge,
        longshot_yes_threshold=longshot_yes_threshold,
        favorite_yes_threshold=favorite_yes_threshold,
        calibration_model=calibration_model,
    )
    if signal is None:
        return None

    gross_expected_edge = signal.probability_estimate - signal.limit_price
    entry_execution_cost_edge = entry_execution_cost_bps / 10_000.0
    fee_edge = _fee_edge(fee_rate=fee_rate, limit_price=signal.limit_price)
    net_expected_edge = (
        gross_expected_edge - entry_execution_cost_edge - fee_edge
    )
    if net_expected_edge < min_expected_edge:
        return None

    notional_usdc = position_sizer.size(
        prob=signal.probability_estimate,
        market_price=signal.limit_price,
        portfolio=portfolio,
    )
    if notional_usdc <= 0.0:
        return None

    evidence_refs = _evidence_refs(market, signal)
    metadata = {
        "source": LIVE_FLB_SOURCE,
        "h1_signal": signal.signal_name,
        "yes_price": market.yes_price,
        "yes_best_ask": market.yes_best_ask,
        "no_best_ask": market.no_best_ask,
        "observed_at": market.observed_at.isoformat(),
        "resolves_at": market.resolves_at.isoformat() if market.resolves_at else None,
        "longshot_yes_threshold": longshot_yes_threshold,
        "favorite_yes_threshold": favorite_yes_threshold,
        "min_expected_edge": min_expected_edge,
        "probability_source": signal.probability_source,
        "calibration_source": signal.calibration_source,
        "calibration_sample_count": signal.calibration_sample_count,
        "gross_expected_edge": gross_expected_edge,
        "entry_execution_cost_bps": entry_execution_cost_bps,
        "entry_execution_cost_edge": entry_execution_cost_edge,
        "fee_rate": fee_rate,
        "fee_edge": fee_edge,
        "net_expected_edge": net_expected_edge,
    }
    payload = {
        "market_id": market.market_id,
        "title": market.title,
        "thesis": signal.thesis,
        "probability_estimate": signal.probability_estimate,
        "expected_edge": net_expected_edge,
        "confidence": confidence,
        "token_id": signal.token_id,
        "venue": market.venue,
        "side": signal.side,
        "outcome": signal.outcome,
        "limit_price": signal.limit_price,
        "notional_usdc": notional_usdc,
        "expected_price": signal.probability_estimate,
        "max_slippage_bps": max_slippage_bps,
        "time_in_force": time_in_force,
        "contradiction_refs": (),
        "metadata": metadata,
    }
    return StrategyObservation(
        observation_id=f"live-flb-{market.market_id}-{context.as_of.isoformat()}",
        strategy_id=context.strategy_id,
        strategy_version_id=context.strategy_version_id,
        source=LIVE_FLB_SOURCE,
        observed_at=context.as_of,
        summary=signal.thesis,
        payload=payload,
        evidence_refs=evidence_refs,
    )


def _classify_market(
    *,
    market: FlbMarketSnapshot,
    min_expected_edge: float,
    longshot_yes_threshold: float,
    favorite_yes_threshold: float,
    calibration_model: FlbCalibrationModel | None,
) -> _FlbSignal | None:
    if market.yes_price < longshot_yes_threshold:
        limit_price = _bounded_probability(
            market.no_best_ask if market.no_best_ask is not None else 1.0 - market.yes_price,
            "no_limit_price",
        )
        probability_estimate, calibration = _flb_signal_probability(
            signal_name="longshot_yes_overpriced_buy_no",
            limit_price=limit_price,
            min_expected_edge=min_expected_edge,
            field_name="no_probability_estimate",
            calibration_model=calibration_model,
        )
        if probability_estimate is None:
            return None
        return _FlbSignal(
            signal_name="longshot_yes_overpriced_buy_no",
            thesis=(
                "H1 FLB: low-YES longshot bucket is treated as overpriced; "
                "buy NO exposure."
            ),
            token_id=market.no_token_id,
            outcome="NO",
            side="BUY",
            limit_price=limit_price,
            probability_estimate=probability_estimate,
            probability_source=_probability_source(calibration),
            calibration_source=_calibration_source(calibration),
            calibration_sample_count=_calibration_sample_count(calibration),
        )
    if market.yes_price > favorite_yes_threshold:
        limit_price = _bounded_probability(
            market.yes_best_ask if market.yes_best_ask is not None else market.yes_price,
            "yes_limit_price",
        )
        probability_estimate, calibration = _flb_signal_probability(
            signal_name="favorite_yes_underpriced_buy_yes",
            limit_price=limit_price,
            min_expected_edge=min_expected_edge,
            field_name="yes_probability_estimate",
            calibration_model=calibration_model,
        )
        if probability_estimate is None:
            return None
        return _FlbSignal(
            signal_name="favorite_yes_underpriced_buy_yes",
            thesis=(
                "H1 FLB: high-YES favorite bucket is treated as underpriced; "
                "buy YES exposure."
            ),
            token_id=market.yes_token_id,
            outcome="YES",
            side="BUY",
            limit_price=limit_price,
            probability_estimate=probability_estimate,
            probability_source=_probability_source(calibration),
            calibration_source=_calibration_source(calibration),
            calibration_sample_count=_calibration_sample_count(calibration),
        )
    return None


def _is_resolved(as_of: datetime, resolves_at: datetime | None) -> bool:
    return resolves_at is not None and resolves_at <= as_of


def _evidence_refs(
    market: FlbMarketSnapshot,
    signal: _FlbSignal,
) -> tuple[str, ...]:
    market_ref = f"market_snapshot:{market.market_id}:{market.observed_at.isoformat()}"
    if signal.calibration_source is None:
        return (FLB_RESEARCH_REF, market_ref)
    calibration_ref = (
        "flb_calibration_model:"
        f"{signal.calibration_source}:{signal.signal_name}"
    )
    return (FLB_RESEARCH_REF, calibration_ref, market_ref)


def _require_non_empty(value: str, field_name: str) -> None:
    if not value:
        msg = f"{field_name} must be non-empty"
        raise ValueError(msg)


def _require_open_probability(value: float, field_name: str) -> None:
    if value <= 0.0 or value >= 1.0 or value != value:
        msg = f"{field_name} must satisfy 0.0 < value < 1.0"
        raise ValueError(msg)


def _bounded_probability(value: float, field_name: str) -> float:
    if value != value:
        msg = f"{field_name} must not be NaN"
        raise ValueError(msg)
    return min(max(float(value), 0.0001), 0.9999)


def _require_nonnegative_finite(value: float, field_name: str) -> None:
    if value < 0.0 or not isfinite(value):
        msg = f"{field_name} must be non-negative and finite"
        raise ValueError(msg)


def _require_unit_interval_closed(value: float, field_name: str) -> None:
    if value < 0.0 or value > 1.0 or not isfinite(value):
        msg = f"{field_name} must satisfy 0.0 <= value <= 1.0"
        raise ValueError(msg)


def _fee_edge(*, fee_rate: float, limit_price: float) -> float:
    return fee_rate * (1.0 - limit_price)


def _flb_signal_probability(
    *,
    signal_name: FlbSignalName,
    limit_price: float,
    min_expected_edge: float,
    field_name: str,
    calibration_model: FlbCalibrationModel | None,
) -> tuple[float | None, FlbSignalCalibration | None]:
    if calibration_model is None:
        return (
            _bounded_probability(limit_price + min_expected_edge, field_name),
            None,
        )
    calibration = calibration_model.calibration_for(signal_name)
    return calibration.probability_estimate, calibration


def _probability_source(calibration: FlbSignalCalibration | None) -> str:
    if calibration is None:
        return "placeholder_min_expected_edge"
    return "flb_calibration_model"


def _calibration_source(calibration: FlbSignalCalibration | None) -> str | None:
    if calibration is None:
        return None
    return calibration.source_label


def _calibration_sample_count(calibration: FlbSignalCalibration | None) -> int | None:
    if calibration is None:
        return None
    return calibration.sample_count


def load_flb_calibration_csv(
    path: str | Path,
    *,
    min_sample_count: int = DEFAULT_FLB_CALIBRATION_MIN_SAMPLES,
) -> FlbCalibrationModel:
    csv_path = Path(path).expanduser()
    if not csv_path.exists():
        msg = f"FLB calibration CSV does not exist: {csv_path}"
        raise ValueError(msg)

    calibrations: list[FlbSignalCalibration] = []
    with io.StringIO(_read_text_no_follow(csv_path), newline="") as csv_file:
        reader = csv.DictReader(csv_file)
        if reader.fieldnames is None:
            msg = f"FLB calibration CSV is empty: {csv_path}"
            raise ValueError(msg)
        _require_unique_csv_fieldnames(reader.fieldnames)
        missing_columns = sorted(
            _FLB_CALIBRATION_REQUIRED_COLUMNS.difference(reader.fieldnames)
        )
        if missing_columns:
            msg = (
                "FLB calibration CSV missing required columns: "
                f"{', '.join(missing_columns)}"
            )
            raise ValueError(msg)
        for row_number, row in enumerate(reader, start=2):
            calibrations.append(
                FlbSignalCalibration(
                    signal_name=_required_flb_signal_name(
                        row,
                        path=csv_path,
                        row_number=row_number,
                    ),
                    probability_estimate=_required_probability(
                        row,
                        "probability_estimate",
                        path=csv_path,
                        row_number=row_number,
                    ),
                    sample_count=_required_positive_int(
                        row,
                        "sample_count",
                        path=csv_path,
                        row_number=row_number,
                    ),
                    source_label=_required_csv_text(
                        row,
                        "source_label",
                        path=csv_path,
                        row_number=row_number,
                    ),
                )
            )

    return FlbCalibrationModel(
        calibrations=tuple(calibrations),
        min_sample_count=min_sample_count,
    )


def _read_text_no_follow(path: Path) -> str:
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    fd = -1
    try:
        fd = os.open(path, flags, 0o777)
        path_stat = os.fstat(fd)
        if not stat.S_ISREG(path_stat.st_mode):
            raise OSError(f"FLB calibration CSV cannot be read safely: {path}")
        if path_stat.st_nlink != 1:
            raise OSError(f"FLB calibration CSV cannot be read safely: {path}")
        with os.fdopen(fd, "r", encoding="utf-8") as file:
            fd = -1
            return file.read()
    except OSError as exc:
        msg = f"FLB calibration CSV cannot be read safely: {path}"
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


def _required_flb_signal_name(
    row: dict[str, str | None],
    *,
    path: Path,
    row_number: int,
) -> FlbSignalName:
    value = _required_csv_text(
        row,
        "signal_name",
        path=path,
        row_number=row_number,
    )
    if value not in _FLB_SIGNAL_NAMES:
        msg = f"{path}:{row_number}: unsupported FLB signal_name {value!r}"
        raise ValueError(msg)
    return cast(FlbSignalName, value)


def _required_probability(
    row: dict[str, str | None],
    column: str,
    *,
    path: Path,
    row_number: int,
) -> float:
    value = _required_float(row, column, path=path, row_number=row_number)
    _require_open_probability(value, column)
    return value


def _required_float(
    row: dict[str, str | None],
    column: str,
    *,
    path: Path,
    row_number: int,
) -> float:
    raw_value = _required_csv_text(row, column, path=path, row_number=row_number)
    try:
        value = float(raw_value)
    except ValueError:
        msg = f"{path}:{row_number}: invalid float in {column!r}: {raw_value!r}"
        raise ValueError(msg) from None
    if not isfinite(value):
        msg = f"{path}:{row_number}: non-finite float in {column!r}: {raw_value!r}"
        raise ValueError(msg)
    return value


def _required_positive_int(
    row: dict[str, str | None],
    column: str,
    *,
    path: Path,
    row_number: int,
) -> int:
    raw_value = _required_csv_text(row, column, path=path, row_number=row_number)
    try:
        value = int(raw_value)
    except ValueError:
        msg = f"{path}:{row_number}: invalid integer in {column!r}: {raw_value!r}"
        raise ValueError(msg) from None
    if value <= 0:
        msg = f"{path}:{row_number}: {column!r} must be positive"
        raise ValueError(msg)
    return value


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
