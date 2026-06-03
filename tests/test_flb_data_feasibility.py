"""Tests for H1 FLB data feasibility analysis.

Tests the pure-analysis functions (decile assignment, Wilson interval,
sample gate, report generation) without hitting the Gamma API.
"""

from __future__ import annotations

import csv
import json
import os
import stat
import sys
from collections.abc import Callable
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from types import TracebackType
from typing import IO, ClassVar, cast

import pytest

import scripts.flb_data_feasibility as flb_data_feasibility
from scripts.flb_data_feasibility import (
    DecileStats,
    FlbCalibrationArtifactRow,
    ResolvedMarket,
    _assign_decile,
    _parse_market,
    _wilson_interval,
    build_flb_calibration_rows,
    check_sample_gate,
    compute_decile_stats,
    fetch_resolved_markets,
    generate_report,
    load_warehouse_markets,
    main,
    markets_to_contracts,
    save_decile_csv,
    save_flb_calibration_csv,
    save_flb_calibration_provenance_json,
)
from pms.strategies.flb.source import load_flb_calibration_csv


def test_flb_data_feasibility_docstring_documents_operator_error_exit_code() -> None:
    docstring = flb_data_feasibility.__doc__

    assert docstring is not None
    assert "2 — operator/input error" in docstring
    assert "malformed warehouse CSV" in docstring


# ── Helpers ──────────────────────────────────────────────────────────────────


class _FailingTextWriter:
    def __init__(self, wrapped: IO[str]) -> None:
        self._wrapped = wrapped

    def __enter__(self) -> "_FailingTextWriter":
        self._wrapped.__enter__()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool | None:
        return self._wrapped.__exit__(exc_type, exc, traceback)

    def write(self, content: str) -> int:
        self._wrapped.write(content)
        raise OSError("simulated write failure")


def _patch_text_artifact_writes_to_fail(monkeypatch: pytest.MonkeyPatch) -> None:
    real_fdopen = os.fdopen

    def failing_fdopen(
        fd: int,
        mode: str = "r",
        buffering: int = -1,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
        closefd: bool = True,
        opener: Callable[[str, int], int] | None = None,
    ) -> object:
        file = real_fdopen(
            fd,
            mode,
            buffering,
            encoding,
            errors,
            newline,
            closefd,
            opener,
        )
        if "w" in mode:
            return _FailingTextWriter(cast(IO[str], file))
        return file

    monkeypatch.setattr(os, "fdopen", failing_fdopen)


def _market(
    yes_price: float,
    resolved_yes: bool,
    *,
    volume: float = 10_000.0,
    liquidity: float = 500.0,
    category: str = "other",
) -> ResolvedMarket:
    """Build a minimal ResolvedMarket for testing."""
    return ResolvedMarket(
        market_id=f"m-{yes_price:.2f}-{resolved_yes}",
        question=f"Test market at {yes_price:.2f}",
        yes_price=yes_price,
        resolved_yes=resolved_yes,
        volume=volume,
        liquidity=liquidity,
        end_date="2026-05-01",
        category=category,
    )


class _FakeGammaResponse:
    def __init__(self, payload: list[dict[str, object]]) -> None:
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> list[dict[str, object]]:
        return self._payload


WAREHOUSE_COLUMNS = [
    "market_id",
    "question",
    "entry_yes_price",
    "yes_payout",
    "no_payout",
    "volume",
    "liquidity",
    "entry_timestamp",
    "resolved_at",
    "category",
]


def test_fetch_resolved_markets_orders_gamma_by_recent_close(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Gamma's default closed-market sort is oldest-first and mostly cleared rows."""

    class RecordingGammaClient:
        calls: ClassVar[list[tuple[str, dict[str, str]]]] = []

        def __init__(self, *args: object, **kwargs: object) -> None:
            del args, kwargs

        def __enter__(self) -> "RecordingGammaClient":
            return self

        def __exit__(
            self,
            exc_type: type[BaseException] | None,
            exc: BaseException | None,
            traceback: TracebackType | None,
        ) -> None:
            del exc_type, exc, traceback

        def get(self, path: str, *, params: dict[str, str]) -> _FakeGammaResponse:
            self.calls.append((path, dict(params)))
            return _FakeGammaResponse(
                [
                    {
                        "id": "recent-closed-market",
                        "question": "Recent closed market?",
                        "outcomes": '["Yes", "No"]',
                        "outcomePrices": '["1", "0"]',
                        "lastTradePrice": 0.49,
                        "volumeNum": 10_000.0,
                        "liquidityNum": 500.0,
                        "endDate": "2026-06-01T23:20:00Z",
                        "slug": "recent-closed-market",
                    }
                ]
            )

    monkeypatch.setattr(
        "scripts.flb_data_feasibility.httpx.Client",
        RecordingGammaClient,
    )

    markets = fetch_resolved_markets(limit=10, max_pages=1)

    assert RecordingGammaClient.calls == [
        (
            "/markets",
            {
                "closed": "true",
                "order": "closedTime",
                "ascending": "false",
                "limit": "10",
                "offset": "0",
            },
        )
    ]
    assert markets == [
        ResolvedMarket(
            market_id="recent-closed-market",
            question="Recent closed market?",
            yes_price=0.49,
            resolved_yes=True,
            volume=10_000.0,
            liquidity=500.0,
            end_date="2026-06-01T23:20:00Z",
            category="other",
        )
    ]


def _write_warehouse_csv(path: Path, rows: list[dict[str, str]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=WAREHOUSE_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def _warehouse_row(
    *,
    market_id: str = "m-1",
    entry_yes_price: str = "0.05",
    yes_payout: str = "0",
    no_payout: str = "1",
    entry_timestamp: str = "2025-12-01T00:00:00Z",
    resolved_at: str = "2026-01-01T00:00:00Z",
) -> dict[str, str]:
    return {
        "market_id": market_id,
        "question": f"Warehouse market {market_id}?",
        "entry_yes_price": entry_yes_price,
        "yes_payout": yes_payout,
        "no_payout": no_payout,
        "volume": "10000",
        "liquidity": "500",
        "entry_timestamp": entry_timestamp,
        "resolved_at": resolved_at,
        "category": "politics",
    }


# ── Warehouse CSV Loading ───────────────────────────────────────────────────


class TestWarehouseCsvLoading:
    def test_loads_explicit_settlement_vectors(self, tmp_path: Path) -> None:
        """Warehouse source uses explicit final payout vectors, not price heuristics."""
        path = tmp_path / "resolved_binary.csv"
        _write_warehouse_csv(path, [
            _warehouse_row(
                market_id="yes-wins",
                entry_yes_price="0.92",
                yes_payout="1",
                no_payout="0",
            ),
            _warehouse_row(
                market_id="no-wins",
                entry_yes_price="0.08",
                yes_payout="0",
                no_payout="1",
            ),
        ])

        markets, skipped = load_warehouse_markets(path)

        assert len(markets) == 2
        assert skipped == 0
        assert markets[0].resolved_yes is True
        assert markets[0].yes_price == pytest.approx(0.92)
        assert markets[1].resolved_yes is False
        assert len(markets_to_contracts(markets)) == 4

    def test_rejects_near_settled_payout_vector(self, tmp_path: Path) -> None:
        """0.995/0.005 is a price-like vector, not explicit settlement truth."""
        path = tmp_path / "near_settled.csv"
        _write_warehouse_csv(path, [
            _warehouse_row(yes_payout="0.995", no_payout="0.005")
        ])

        with pytest.raises(ValueError, match="exact final payout vector"):
            load_warehouse_markets(path)

    def test_skips_ambiguous_fifty_fifty_payout_vector(self, tmp_path: Path) -> None:
        """Ambiguous 50/50 resolutions are silently skipped — not safe labels for H1 FLB."""
        path = tmp_path / "fifty_fifty.csv"
        _write_warehouse_csv(path, [
            _warehouse_row(yes_payout="0.5", no_payout="0.5")
        ])

        markets, skipped = load_warehouse_markets(path)
        assert len(markets) == 0
        assert skipped == 1  # 50/50 market skipped without error

    def test_rejects_missing_required_column(self, tmp_path: Path) -> None:
        path = tmp_path / "missing_column.csv"
        with path.open("w", newline="", encoding="utf-8") as f:
            fieldnames = [c for c in WAREHOUSE_COLUMNS if c != "liquidity"]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            row = _warehouse_row()
            row.pop("liquidity")
            writer.writerow(row)

        with pytest.raises(ValueError, match="missing required columns: liquidity"):
            load_warehouse_markets(path)

    def test_rejects_duplicate_header(self, tmp_path: Path) -> None:
        path = tmp_path / "duplicate_header.csv"
        path.write_text(
            "\n".join(
                (
                    (
                        "market_id,question,entry_yes_price,yes_payout,no_payout,"
                        "volume,liquidity,liquidity,entry_timestamp,resolved_at,"
                        "category"
                    ),
                    (
                        "m-1,Will duplicate headers fail?,0.05,0,1,10000,500,"
                        "999,2025-12-01T00:00:00Z,2026-01-01T00:00:00Z,"
                        "politics"
                    ),
                )
            ),
            encoding="utf-8",
        )

        with pytest.raises(ValueError, match="duplicate CSV column: liquidity"):
            load_warehouse_markets(path)

    def test_rejects_duplicate_market_ids(self, tmp_path: Path) -> None:
        """Duplicate markets would falsely inflate the contract sample gate."""
        path = tmp_path / "duplicate_markets.csv"
        _write_warehouse_csv(path, [
            _warehouse_row(market_id="duplicated-market"),
            _warehouse_row(market_id="duplicated-market"),
        ])

        with pytest.raises(ValueError, match="duplicate market_id"):
            load_warehouse_markets(path)

    def test_rejects_symlink_input(self, tmp_path: Path) -> None:
        target_path = tmp_path / "target-warehouse.csv"
        _write_warehouse_csv(target_path, [_warehouse_row()])
        path = tmp_path / "resolved_binary.csv"
        path.symlink_to(target_path)

        with pytest.raises(ValueError, match="cannot be read safely"):
            load_warehouse_markets(path)

    def test_opens_input_with_no_follow_when_available(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        no_follow_flag = getattr(os, "O_NOFOLLOW", 0)
        if no_follow_flag == 0:
            pytest.skip("os.O_NOFOLLOW is unavailable on this platform")

        path = tmp_path / "resolved_binary.csv"
        _write_warehouse_csv(path, [_warehouse_row()])
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

        markets, skipped = load_warehouse_markets(path)

        observed_by_path = {observed_path: flags for observed_path, flags in observed}
        assert len(markets) == 1
        assert skipped == 0
        assert observed_by_path[path] & no_follow_flag

    def test_rejects_hardlink_swap_during_input_read(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        path = tmp_path / "resolved_binary.csv"
        _write_warehouse_csv(path, [_warehouse_row()])
        replacement_source = tmp_path / "replacement-warehouse.csv"
        _write_warehouse_csv(replacement_source, [_warehouse_row(market_id="m-2")])
        real_open = os.open
        swapped = False

        def swapping_open(
            path_arg: str | bytes | os.PathLike[str] | os.PathLike[bytes],
            flags: int,
            mode: int = 0o777,
        ) -> int:
            nonlocal swapped
            observed_path = Path(os.fsdecode(os.fspath(path_arg)))
            if observed_path == path and not swapped:
                swapped = True
                path.unlink()
                os.link(replacement_source, path)
            return real_open(path_arg, flags, mode)

        monkeypatch.setattr(os, "open", swapping_open)

        with pytest.raises(ValueError, match="cannot be read safely"):
            load_warehouse_markets(path)

        assert swapped is True

    def test_rejects_entry_timestamp_after_resolution(self, tmp_path: Path) -> None:
        """Post-resolution entry prices leak settlement truth into FLB samples."""
        path = tmp_path / "post_resolution_entry.csv"
        _write_warehouse_csv(path, [
            _warehouse_row(
                entry_timestamp="2026-01-02T00:00:00Z",
                resolved_at="2026-01-01T00:00:00Z",
            )
        ])

        with pytest.raises(ValueError, match="entry_timestamp must be before resolved_at"):
            load_warehouse_markets(path)

    def test_rejects_entry_timestamp_equal_to_resolution(self, tmp_path: Path) -> None:
        """Same-time entry snapshots are also unsafe for historical replay."""
        path = tmp_path / "same_time_entry.csv"
        _write_warehouse_csv(path, [
            _warehouse_row(
                entry_timestamp="2026-01-01T00:00:00Z",
                resolved_at="2026-01-01T00:00:00Z",
            )
        ])

        with pytest.raises(ValueError, match="entry_timestamp must be before resolved_at"):
            load_warehouse_markets(path)

    def test_mixed_timezone_timestamps_are_normalized(self, tmp_path: Path) -> None:
        """Naive and Z-suffixed ISO fields should compare as UTC instants."""
        path = tmp_path / "mixed_timezone.csv"
        _write_warehouse_csv(path, [
            _warehouse_row(
                entry_timestamp="2025-12-01T00:00:00",
                resolved_at="2026-01-01T00:00:00Z",
            )
        ])

        markets, _ = load_warehouse_markets(path)

        assert len(markets) == 1

    def test_mixed_dataset_normal_and_fifty_fifty_resolutions(
        self, tmp_path: Path
    ) -> None:
        """50/50 markets are skipped, normal markets are still loaded."""
        path = tmp_path / "mixed_fifty_fifty.csv"
        _write_warehouse_csv(path, [
            _warehouse_row(
                market_id="normal-yes",
                yes_payout="1",
                no_payout="0",
            ),
            _warehouse_row(
                market_id="fifty-fifty",
                yes_payout="0.5",
                no_payout="0.5",
            ),
            _warehouse_row(
                market_id="normal-no",
                yes_payout="0",
                no_payout="1",
            ),
        ])

        markets, skipped = load_warehouse_markets(path)
        assert skipped == 1  # the 50/50 market
        assert len(markets) == 2  # normal markets still loaded
        assert [m.market_id for m in markets] == ["normal-yes", "normal-no"]
        assert markets[0].resolved_yes is True
        assert markets[1].resolved_yes is False

    def test_mixed_timezone_post_resolution_entry_is_validation_error(
        self, tmp_path: Path
    ) -> None:
        """Mixed timezone formats should not raise TypeError on unsafe rows."""
        path = tmp_path / "mixed_timezone_bad_order.csv"
        _write_warehouse_csv(path, [
            _warehouse_row(
                entry_timestamp="2026-01-02T00:00:00",
                resolved_at="2026-01-01T00:00:00Z",
            )
        ])

        with pytest.raises(ValueError, match="entry_timestamp must be before resolved_at"):
            load_warehouse_markets(path)

    def test_warehouse_longshot_contracts_do_not_imply_signal_viability(
        self, tmp_path: Path
    ) -> None:
        path = tmp_path / "sample_gate.csv"
        _write_warehouse_csv(path, [
            _warehouse_row(market_id=f"m-{i}", entry_yes_price="0.05")
            for i in range(120)
        ])

        markets, skipped = load_warehouse_markets(path)
        assert skipped == 0  # no 50/50 resolutions in this dataset
        contracts = markets_to_contracts(markets)
        stats = compute_decile_stats(contracts)
        gate = check_sample_gate(markets)
        report = generate_report(
            markets=markets,
            contracts=contracts,
            decile_stats=stats,
            gate=gate,
            fetched_at=datetime(2026, 5, 3, tzinfo=UTC),
            source_label=f"warehouse CSV: {path}",
        )

        assert len(markets) == 120
        assert stats[0].n == 120
        assert stats[9].n == 120
        assert gate.longshot_count == 120
        assert gate.favorite_count == 0
        assert gate.passed is False
        assert "H1 NOT VIABLE YET" in report
        assert "warehouse CSV:" in report

    def test_warehouse_signal_gate_passes_when_both_runtime_buckets_exist(
        self, tmp_path: Path
    ) -> None:
        path = tmp_path / "sample_gate.csv"
        _write_warehouse_csv(path, [
            _warehouse_row(market_id=f"longshot-{i}", entry_yes_price="0.05")
            for i in range(120)
        ] + [
            _warehouse_row(
                market_id=f"favorite-{i}",
                entry_yes_price="0.95",
                yes_payout="1",
                no_payout="0",
            )
            for i in range(120)
        ])

        markets, skipped = load_warehouse_markets(path)
        assert skipped == 0
        contracts = markets_to_contracts(markets)
        stats = compute_decile_stats(contracts)
        gate = check_sample_gate(markets)
        report = generate_report(
            markets=markets,
            contracts=contracts,
            decile_stats=stats,
            gate=gate,
            fetched_at=datetime(2026, 5, 3, tzinfo=UTC),
            source_label=f"warehouse CSV: {path}",
        )

        assert gate.longshot_count == 120
        assert gate.favorite_count == 120
        assert gate.passed is True
        assert "H1 DATA VIABLE" in report


class TestFlbCalibrationArtifact:
    def test_build_flb_calibration_rows_uses_signal_specific_outcomes(self) -> None:
        markets = [
            _market(0.05, False, category="politics")
            for _ in range(119)
        ]
        markets.append(_market(0.05, True, category="politics"))
        markets.extend(
            _market(0.95, True, category="sports")
            for _ in range(118)
        )
        markets.extend(
            _market(0.95, False, category="sports")
            for _ in range(2)
        )

        rows = build_flb_calibration_rows(
            markets,
            source_label="warehouse-flb-v1",
        )

        assert rows == [
            FlbCalibrationArtifactRow(
                signal_name="longshot_yes_overpriced_buy_no",
                probability_estimate=120 / 122,
                sample_count=120,
                source_label="warehouse-flb-v1",
            ),
            FlbCalibrationArtifactRow(
                signal_name="favorite_yes_underpriced_buy_yes",
                probability_estimate=119 / 122,
                sample_count=120,
                source_label="warehouse-flb-v1",
            ),
        ]

    def test_build_flb_calibration_rows_requires_each_target_signal_sample_gate(
        self,
    ) -> None:
        markets = [_market(0.05, False) for _ in range(120)]

        with pytest.raises(ValueError, match="favorite_yes_underpriced_buy_yes"):
            build_flb_calibration_rows(
                markets,
                source_label="warehouse-flb-v1",
            )

    def test_save_flb_calibration_csv_round_trips_into_runtime_loader(
        self,
        tmp_path: Path,
    ) -> None:
        rows = [
            FlbCalibrationArtifactRow(
                signal_name="longshot_yes_overpriced_buy_no",
                probability_estimate=0.99,
                sample_count=150,
                source_label="warehouse-flb-v1",
            ),
            FlbCalibrationArtifactRow(
                signal_name="favorite_yes_underpriced_buy_yes",
                probability_estimate=0.97,
                sample_count=151,
                source_label="warehouse-flb-v1",
            ),
        ]
        output_path = tmp_path / "flb-calibration.csv"

        save_flb_calibration_csv(rows, output_path)
        model = load_flb_calibration_csv(output_path)

        assert model.calibration_for(
            "longshot_yes_overpriced_buy_no"
        ).probability_estimate == pytest.approx(0.99)

    def test_save_flb_calibration_provenance_json_binds_calibration_and_warehouse(
        self,
        tmp_path: Path,
    ) -> None:
        rows = [
            FlbCalibrationArtifactRow(
                signal_name="longshot_yes_overpriced_buy_no",
                probability_estimate=0.99,
                sample_count=150,
                source_label="warehouse-flb-v1",
            ),
            FlbCalibrationArtifactRow(
                signal_name="favorite_yes_underpriced_buy_yes",
                probability_estimate=0.97,
                sample_count=151,
                source_label="warehouse-flb-v1",
            ),
        ]
        warehouse_path = tmp_path / "warehouse.csv"
        _write_warehouse_csv(
            warehouse_path,
            [
                _warehouse_row(
                    market_id=f"longshot-{index}",
                    entry_yes_price="0.05",
                    yes_payout="0",
                    no_payout="1",
                )
                for index in range(150)
            ]
            + [
                _warehouse_row(
                    market_id=f"favorite-{index}",
                    entry_yes_price="0.95",
                    yes_payout="1",
                    no_payout="0",
                )
                for index in range(151)
            ],
        )
        calibration_path = tmp_path / "flb-calibration.csv"
        provenance_path = tmp_path / "flb-calibration.csv.provenance.json"

        save_flb_calibration_csv(rows, calibration_path)
        save_flb_calibration_provenance_json(
            rows,
            warehouse_csv_path=warehouse_path,
            warehouse_market_count=301,
            calibration_csv_path=calibration_path,
            output_path=provenance_path,
            generated_at=datetime(2026, 6, 1, tzinfo=UTC),
        )

        payload = json.loads(provenance_path.read_text(encoding="utf-8"))
        assert payload["artifact_type"] == "flb_calibration_provenance"
        assert payload["source"] == "warehouse-csv"
        assert payload["warehouse_csv_sha256"] == sha256(
            warehouse_path.read_bytes()
        ).hexdigest()
        assert payload["calibration_csv_sha256"] == sha256(
            calibration_path.read_bytes()
        ).hexdigest()
        assert payload["warehouse_longshot_count"] == 150
        assert payload["warehouse_favorite_count"] == 151
        assert payload["calibration_source_label"] == "warehouse-flb-v1"

    def test_save_flb_calibration_csv_creates_output_parent_private(
        self,
        tmp_path: Path,
    ) -> None:
        rows = [
            FlbCalibrationArtifactRow(
                signal_name="longshot_yes_overpriced_buy_no",
                probability_estimate=0.99,
                sample_count=150,
                source_label="warehouse-flb-v1",
            ),
            FlbCalibrationArtifactRow(
                signal_name="favorite_yes_underpriced_buy_yes",
                probability_estimate=0.97,
                sample_count=151,
                source_label="warehouse-flb-v1",
            ),
        ]
        output_dir = tmp_path / "artifacts"

        save_flb_calibration_csv(rows, output_dir / "flb-calibration.csv")

        assert stat.S_IMODE(output_dir.stat().st_mode) == 0o700

    def test_save_flb_calibration_csv_refuses_permissive_output_parent(
        self,
        tmp_path: Path,
    ) -> None:
        rows = [
            FlbCalibrationArtifactRow(
                signal_name="longshot_yes_overpriced_buy_no",
                probability_estimate=0.99,
                sample_count=150,
                source_label="warehouse-flb-v1",
            ),
            FlbCalibrationArtifactRow(
                signal_name="favorite_yes_underpriced_buy_yes",
                probability_estimate=0.97,
                sample_count=151,
                source_label="warehouse-flb-v1",
            ),
        ]
        output_dir = tmp_path / "shared-artifacts"
        output_dir.mkdir(mode=0o700)
        output_dir.chmod(0o755)

        try:
            with pytest.raises(OSError, match="FLB calibration CSV output parent"):
                save_flb_calibration_csv(rows, output_dir / "flb-calibration.csv")
        finally:
            output_dir.chmod(0o700)

        assert not (output_dir / "flb-calibration.csv").exists()

    def test_save_flb_calibration_csv_refuses_output_inside_working_tree(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        rows = [
            FlbCalibrationArtifactRow(
                signal_name="longshot_yes_overpriced_buy_no",
                probability_estimate=0.99,
                sample_count=150,
                source_label="warehouse-flb-v1",
            ),
            FlbCalibrationArtifactRow(
                signal_name="favorite_yes_underpriced_buy_yes",
                probability_estimate=0.97,
                sample_count=151,
                source_label="warehouse-flb-v1",
            ),
        ]
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        (repo_dir / ".git").mkdir()
        output_dir = repo_dir / "artifacts"
        output_dir.mkdir(mode=0o700)
        output_path = output_dir / "flb-calibration.csv"
        monkeypatch.chdir(repo_dir)

        with pytest.raises(OSError, match="working tree"):
            save_flb_calibration_csv(rows, output_path)

        assert not output_path.exists()

    def test_save_flb_calibration_csv_preserves_existing_output_when_write_fails(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        rows = [
            FlbCalibrationArtifactRow(
                signal_name="longshot_yes_overpriced_buy_no",
                probability_estimate=0.99,
                sample_count=150,
                source_label="warehouse-flb-v1",
            ),
            FlbCalibrationArtifactRow(
                signal_name="favorite_yes_underpriced_buy_yes",
                probability_estimate=0.97,
                sample_count=151,
                source_label="warehouse-flb-v1",
            ),
        ]
        output_path = tmp_path / "flb-calibration.csv"
        output_path.write_text("old calibration\n", encoding="utf-8")
        _patch_text_artifact_writes_to_fail(monkeypatch)

        with pytest.raises(OSError, match="simulated write failure"):
            save_flb_calibration_csv(rows, output_path)

        assert output_path.read_text(encoding="utf-8") == "old calibration\n"

    def test_save_flb_calibration_csv_does_not_publish_new_output_when_write_fails(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        rows = [
            FlbCalibrationArtifactRow(
                signal_name="longshot_yes_overpriced_buy_no",
                probability_estimate=0.99,
                sample_count=150,
                source_label="warehouse-flb-v1",
            ),
            FlbCalibrationArtifactRow(
                signal_name="favorite_yes_underpriced_buy_yes",
                probability_estimate=0.97,
                sample_count=151,
                source_label="warehouse-flb-v1",
            ),
        ]
        output_path = tmp_path / "flb-calibration.csv"
        _patch_text_artifact_writes_to_fail(monkeypatch)

        with pytest.raises(OSError, match="simulated write failure"):
            save_flb_calibration_csv(rows, output_path)

        assert not output_path.exists()

    def test_save_decile_csv_creates_output_parent_private(
        self,
        tmp_path: Path,
    ) -> None:
        stats = compute_decile_stats(
            markets_to_contracts(
                [
                    _market(0.05, False),
                    _market(0.95, True),
                ]
            )
        )
        output_dir = tmp_path / "artifacts"

        save_decile_csv(stats, output_dir / "flb-deciles.csv")

        assert stat.S_IMODE(output_dir.stat().st_mode) == 0o700

    def test_save_decile_csv_refuses_permissive_output_parent(
        self,
        tmp_path: Path,
    ) -> None:
        stats = compute_decile_stats(
            markets_to_contracts(
                [
                    _market(0.05, False),
                    _market(0.95, True),
                ]
            )
        )
        output_dir = tmp_path / "shared-artifacts"
        output_dir.mkdir(mode=0o700)
        output_dir.chmod(0o755)

        try:
            with pytest.raises(OSError, match="FLB decile CSV output parent"):
                save_decile_csv(stats, output_dir / "flb-deciles.csv")
        finally:
            output_dir.chmod(0o700)

        assert not (output_dir / "flb-deciles.csv").exists()

    def test_save_decile_csv_preserves_existing_output_when_write_fails(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        stats = compute_decile_stats(
            markets_to_contracts(
                [
                    _market(0.05, False),
                    _market(0.95, True),
                ]
            )
        )
        output_path = tmp_path / "flb-deciles.csv"
        output_path.write_text("old deciles\n", encoding="utf-8")
        _patch_text_artifact_writes_to_fail(monkeypatch)

        with pytest.raises(OSError, match="simulated write failure"):
            save_decile_csv(stats, output_path)

        assert output_path.read_text(encoding="utf-8") == "old deciles\n"

    def test_save_decile_csv_does_not_publish_new_output_when_write_fails(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        stats = compute_decile_stats(
            markets_to_contracts(
                [
                    _market(0.05, False),
                    _market(0.95, True),
                ]
            )
        )
        output_path = tmp_path / "flb-deciles.csv"
        _patch_text_artifact_writes_to_fail(monkeypatch)

        with pytest.raises(OSError, match="simulated write failure"):
            save_decile_csv(stats, output_path)

        assert not output_path.exists()

    def test_cli_returns_operator_error_for_permissive_report_parent(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        input_path = tmp_path / "warehouse.csv"
        _write_warehouse_csv(
            input_path,
            [
                _warehouse_row(market_id="longshot-1"),
                _warehouse_row(
                    market_id="favorite-1",
                    entry_yes_price="0.95",
                    yes_payout="1",
                    no_payout="0",
                ),
            ],
        )
        output_dir = tmp_path / "shared-artifacts"
        output_dir.mkdir(mode=0o700)
        output_dir.chmod(0o755)
        output_path = output_dir / "flb-report.md"
        monkeypatch.setattr(
            sys,
            "argv",
            [
                "flb_data_feasibility.py",
                "--source",
                "warehouse-csv",
                "--input",
                str(input_path),
                "--output",
                str(output_path),
            ],
        )

        try:
            exit_code = main()
            captured = capsys.readouterr()
        finally:
            output_dir.chmod(0o700)

        assert exit_code == 2
        assert "FLB report output parent" in captured.err
        assert "too permissive" in captured.err
        assert not output_path.exists()

    def test_cli_returns_operator_error_for_permissive_calibration_parent(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        input_path = tmp_path / "warehouse.csv"
        rows = [
            _warehouse_row(
                market_id=f"longshot-{index}",
                entry_yes_price="0.05",
                yes_payout="0",
                no_payout="1",
            )
            for index in range(120)
        ] + [
            _warehouse_row(
                market_id=f"favorite-{index}",
                entry_yes_price="0.95",
                yes_payout="1",
                no_payout="0",
            )
            for index in range(120)
        ]
        _write_warehouse_csv(input_path, rows)
        output_dir = tmp_path / "shared-artifacts"
        output_dir.mkdir(mode=0o700)
        output_dir.chmod(0o755)
        output_path = output_dir / "flb-calibration.csv"
        monkeypatch.setattr(
            sys,
            "argv",
            [
                "flb_data_feasibility.py",
                "--source",
                "warehouse-csv",
                "--input",
                str(input_path),
                "--calibration-csv",
                str(output_path),
                "--calibration-source-label",
                "warehouse-flb-v1",
            ],
        )

        try:
            exit_code = main()
            captured = capsys.readouterr()
        finally:
            output_dir.chmod(0o700)

        assert exit_code == 2
        assert "FLB calibration CSV output parent" in captured.err
        assert "too permissive" in captured.err
        assert not output_path.exists()

    def test_cli_writes_flb_calibration_provenance_json(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        input_path = tmp_path / "warehouse.csv"
        rows = [
            _warehouse_row(
                market_id=f"longshot-{index}",
                entry_yes_price="0.05",
                yes_payout="0",
                no_payout="1",
            )
            for index in range(120)
        ] + [
            _warehouse_row(
                market_id=f"favorite-{index}",
                entry_yes_price="0.95",
                yes_payout="1",
                no_payout="0",
            )
            for index in range(120)
        ]
        _write_warehouse_csv(input_path, rows)
        calibration_path = tmp_path / "flb-calibration.csv"
        provenance_path = Path(f"{calibration_path}.provenance.json")
        monkeypatch.setattr(
            sys,
            "argv",
            [
                "flb_data_feasibility.py",
                "--source",
                "warehouse-csv",
                "--input",
                str(input_path),
                "--calibration-csv",
                str(calibration_path),
                "--calibration-source-label",
                "warehouse-flb-v1",
                "--calibration-provenance-json",
                str(provenance_path),
            ],
        )

        exit_code = main()
        captured = capsys.readouterr()

        assert exit_code == 0
        assert calibration_path.exists()
        assert provenance_path.exists()
        assert "FLB calibration CSV written" in captured.err
        assert "FLB calibration provenance JSON written" in captured.err
        payload = json.loads(provenance_path.read_text(encoding="utf-8"))
        assert payload["warehouse_market_count"] == 240
        assert payload["warehouse_longshot_count"] == 120
        assert payload["warehouse_favorite_count"] == 120
        assert payload["calibration_csv_sha256"] == sha256(
            calibration_path.read_bytes()
        ).hexdigest()

    def test_cli_returns_sample_gate_exit_for_thin_calibration_source(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        input_path = tmp_path / "warehouse.csv"
        output_path = tmp_path / "flb-calibration.csv"
        _write_warehouse_csv(
            input_path,
            [
                _warehouse_row(
                    market_id="longshot-1",
                    entry_yes_price="0.05",
                    yes_payout="0",
                    no_payout="1",
                ),
                _warehouse_row(
                    market_id="favorite-1",
                    entry_yes_price="0.95",
                    yes_payout="1",
                    no_payout="0",
                ),
            ],
        )
        monkeypatch.setattr(
            sys,
            "argv",
            [
                "flb_data_feasibility.py",
                "--source",
                "warehouse-csv",
                "--input",
                str(input_path),
                "--calibration-csv",
                str(output_path),
                "--calibration-source-label",
                "warehouse-flb-v1",
            ],
        )

        exit_code = main()
        captured = capsys.readouterr()

        assert exit_code == 1
        assert "insufficient FLB calibration samples" in captured.err
        assert "ERROR:" not in captured.err
        assert "# H1 FLB Data Feasibility Report" in captured.out
        assert not output_path.exists()

    def test_cli_requires_explicit_calibration_source_label(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        input_path = tmp_path / "warehouse.csv"
        output_path = tmp_path / "flb-calibration.csv"
        _write_warehouse_csv(
            input_path,
            [
                _warehouse_row(
                    market_id=f"longshot-{index}",
                    entry_yes_price="0.05",
                    yes_payout="0",
                    no_payout="1",
                )
                for index in range(100)
            ]
            + [
                _warehouse_row(
                    market_id=f"favorite-{index}",
                    entry_yes_price="0.95",
                    yes_payout="1",
                    no_payout="0",
                )
                for index in range(100)
            ],
        )
        monkeypatch.setattr(
            sys,
            "argv",
            [
                "flb_data_feasibility.py",
                "--source",
                "warehouse-csv",
                "--input",
                str(input_path),
                "--calibration-csv",
                str(output_path),
            ],
        )

        with pytest.raises(SystemExit) as exc_info:
            main()

        assert exc_info.value.code == 2
        assert not output_path.exists()

    def test_save_flb_calibration_csv_atomic_publish_does_not_mutate_hardlink_swap_target(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        rows = [
            FlbCalibrationArtifactRow(
                signal_name="longshot_yes_overpriced_buy_no",
                probability_estimate=0.99,
                sample_count=150,
                source_label="warehouse-flb-v1",
            ),
            FlbCalibrationArtifactRow(
                signal_name="favorite_yes_underpriced_buy_yes",
                probability_estimate=0.97,
                sample_count=151,
                source_label="warehouse-flb-v1",
            ),
        ]
        target_path = tmp_path / "target-flb-calibration.csv"
        target_path.write_text("target must not be overwritten\n", encoding="utf-8")
        output_path = tmp_path / "flb-calibration.csv"
        output_path.write_text("old calibration\n", encoding="utf-8")
        real_replace = os.replace
        swapped = False

        def swapping_replace(
            src: str | bytes | os.PathLike[str] | os.PathLike[bytes],
            dst: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        ) -> None:
            nonlocal swapped
            observed_dst = Path(os.fsdecode(os.fspath(dst)))
            if observed_dst == output_path and not swapped:
                swapped = True
                output_path.unlink()
                os.link(target_path, output_path)
            real_replace(src, dst)

        monkeypatch.setattr(os, "replace", swapping_replace)

        save_flb_calibration_csv(rows, output_path)

        assert swapped is True
        assert target_path.read_text(encoding="utf-8") == (
            "target must not be overwritten\n"
        )
        assert output_path.stat().st_nlink == 1
        assert load_flb_calibration_csv(output_path).calibration_for(
            "longshot_yes_overpriced_buy_no"
        ).probability_estimate == pytest.approx(0.99)

    def test_cli_rejects_calibration_output_reusing_warehouse_input(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        input_path = tmp_path / "warehouse.csv"
        rows = [
            _warehouse_row(
                market_id=f"longshot-{index}",
                entry_yes_price="0.05",
                yes_payout="0",
                no_payout="1",
            )
            for index in range(100)
        ] + [
            _warehouse_row(
                market_id=f"favorite-{index}",
                entry_yes_price="0.95",
                yes_payout="1",
                no_payout="0",
            )
            for index in range(100)
        ]
        _write_warehouse_csv(input_path, rows)
        original_input = input_path.read_text(encoding="utf-8")
        monkeypatch.setattr(
            sys,
            "argv",
            [
                "flb_data_feasibility.py",
                "--source",
                "warehouse-csv",
                "--input",
                str(input_path),
                "--calibration-csv",
                str(input_path),
                "--calibration-source-label",
                "warehouse-flb-v1",
            ],
        )

        with pytest.raises(SystemExit) as exc_info:
            main()

        assert exc_info.value.code == 2
        assert input_path.read_text(encoding="utf-8") == original_input

    def test_cli_rejects_report_output_reusing_decile_csv_output(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        input_path = tmp_path / "warehouse.csv"
        output_path = tmp_path / "flb-artifact.csv"
        _write_warehouse_csv(
            input_path,
            [
                _warehouse_row(market_id="longshot-1"),
                _warehouse_row(
                    market_id="favorite-1",
                    entry_yes_price="0.95",
                    yes_payout="1",
                    no_payout="0",
                ),
            ],
        )
        monkeypatch.setattr(
            sys,
            "argv",
            [
                "flb_data_feasibility.py",
                "--source",
                "warehouse-csv",
                "--input",
                str(input_path),
                "--output",
                str(output_path),
                "--csv",
                str(output_path),
            ],
        )

        with pytest.raises(SystemExit) as exc_info:
            main()

        assert exc_info.value.code == 2
        assert not output_path.exists()

    def test_cli_returns_operator_error_for_malformed_warehouse_csv(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        input_path = tmp_path / "warehouse.csv"
        with input_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=[column for column in WAREHOUSE_COLUMNS if column != "liquidity"],
            )
            writer.writeheader()
            row = _warehouse_row(market_id="longshot-1")
            del row["liquidity"]
            writer.writerow(row)

        monkeypatch.setattr(
            sys,
            "argv",
            [
                "flb_data_feasibility.py",
                "--source",
                "warehouse-csv",
                "--input",
                str(input_path),
            ],
        )

        exit_code = main()
        captured = capsys.readouterr()

        assert exit_code == 2
        assert "missing required columns: liquidity" in captured.err
        assert "# H1 FLB Data Feasibility Report" not in captured.out

    def test_cli_returns_operator_error_for_symlink_report_output(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        input_path = tmp_path / "warehouse.csv"
        _write_warehouse_csv(
            input_path,
            [
                _warehouse_row(market_id="longshot-1"),
                _warehouse_row(
                    market_id="favorite-1",
                    entry_yes_price="0.95",
                    yes_payout="1",
                    no_payout="0",
                ),
            ],
        )
        target_path = tmp_path / "target-report.md"
        target_path.write_text("target must not be overwritten\n", encoding="utf-8")
        output_path = tmp_path / "flb-report.md"
        output_path.symlink_to(target_path)
        monkeypatch.setattr(
            sys,
            "argv",
            [
                "flb_data_feasibility.py",
                "--source",
                "warehouse-csv",
                "--input",
                str(input_path),
                "--output",
                str(output_path),
            ],
        )

        exit_code = main()
        captured = capsys.readouterr()

        assert exit_code == 2
        assert "FLB report output path" in captured.err
        assert "regular file" in captured.err
        assert target_path.read_text(encoding="utf-8") == (
            "target must not be overwritten\n"
        )


# ── Decile Assignment ───────────────────────────────────────────────────────


class TestAssignDecile:
    def test_zero_price(self) -> None:
        assert _assign_decile(0.0) == 0

    def test_one_price(self) -> None:
        assert _assign_decile(1.0) == 9

    def test_boundary_10_percent(self) -> None:
        assert _assign_decile(0.10) == 1  # 0.10 * 10 = 1 → decile 1

    def test_just_below_10_percent(self) -> None:
        assert _assign_decile(0.09) == 0

    def test_midrange(self) -> None:
        assert _assign_decile(0.55) == 5

    def test_just_below_90_percent(self) -> None:
        assert _assign_decile(0.89) == 8

    def test_at_90_percent(self) -> None:
        assert _assign_decile(0.90) == 9

    def test_very_small(self) -> None:
        assert _assign_decile(0.001) == 0

    def test_very_large(self) -> None:
        assert _assign_decile(0.999) == 9


# ── Wilson Interval ─────────────────────────────────────────────────────────


class TestWilsonInterval:
    def test_zero_trials(self) -> None:
        lower, upper = _wilson_interval(0, 0)
        assert lower == 0.0
        assert upper == 1.0

    def test_all_success(self) -> None:
        lower, upper = _wilson_interval(100, 100)
        assert lower > 0.95  # tight interval near 1.0
        assert upper == pytest.approx(1.0, abs=1e-9)

    def test_no_success(self) -> None:
        lower, upper = _wilson_interval(0, 100)
        assert lower == 0.0
        assert upper < 0.05  # tight interval near 0.0

    def test_half_success(self) -> None:
        lower, upper = _wilson_interval(50, 100)
        assert 0.35 < lower < 0.50
        assert 0.50 < upper < 0.65

    def test_small_sample_wider_interval(self) -> None:
        """Smaller samples should produce wider confidence intervals."""
        wide_lower, wide_upper = _wilson_interval(5, 10)
        narrow_lower, narrow_upper = _wilson_interval(50, 100)
        wide_width = wide_upper - wide_lower
        narrow_width = narrow_upper - narrow_lower
        assert wide_width > narrow_width


# ── FLB Decile Computation ──────────────────────────────────────────────────


class TestComputeDecileStats:
    def test_empty_markets(self) -> None:
        stats = compute_decile_stats([])
        assert len(stats) == 10
        assert all(s.n == 0 for s in stats)

    def test_single_market_creates_two_contracts(self) -> None:
        """One market creates two contract observations in different deciles."""
        markets = [_market(0.05, False)]
        stats = compute_decile_stats(markets_to_contracts(markets))
        # One market creates YES@0.05 (decile 0) and NO@0.95 (decile 9)
        assert stats[0].n == 1  # YES contract in longshot bucket
        assert stats[9].n == 1  # NO contract in favorite bucket
        assert all(stats[i].n == 0 for i in [1, 2, 3, 4, 5, 6, 7, 8])  # Others empty

    def test_multiple_markets_fill_both_extreme_buckets(self) -> None:
        """Multiple markets fill both longshot and favorite buckets through contract-level analysis."""
        markets = [_market(0.05, False) for _ in range(20)]
        stats = compute_decile_stats(markets_to_contracts(markets))
        # Each market creates YES@0.05 (decile 0) and NO@0.95 (decile 9)
        assert stats[0].n == 20  # 20 YES contracts in longshot bucket
        assert stats[9].n == 20  # 20 NO contracts in favorite bucket
        assert all(stats[i].n == 0 for i in [1, 2, 3, 4, 5, 6, 7, 8])  # Others empty

    def test_flb_pattern_longshots_overpriced(self) -> None:
        """If longshots (YES <10%) mostly resolve NO, FLB gap is positive."""
        # 20 longshot markets at 5% implied, only 1 resolves YES (5% actual)
        # FLB says they should resolve even less → gap = implied - actual
        markets = [_market(0.05, i == 0) for i in range(20)]
        stats = compute_decile_stats(markets_to_contracts(markets))
        longshot = stats[0]
        assert longshot.n == 20
        assert longshot.n_yes == 1
        assert longshot.actual_rate == pytest.approx(0.05)
        # implied ≈ 0.05, actual = 0.05 → gap ≈ 0 (no FLB detected here)

    def test_flb_pattern_longshots_strongly_overpriced(self) -> None:
        """Markets at 5% implied but 0% actual → strong FLB signal."""
        markets = [_market(0.05, False) for _ in range(100)]
        stats = compute_decile_stats(markets_to_contracts(markets))
        longshot = stats[0]
        assert longshot.actual_rate == 0.0
        assert longshot.flb_gap > 0.04  # implied ~5% minus actual 0%
        assert longshot.recommended_side == "buy_no"

    def test_flb_pattern_favorites_underpriced(self) -> None:
        """Markets at 95% implied but 100% actual → underpriced favorites."""
        markets = [_market(0.95, True) for _ in range(100)]
        stats = compute_decile_stats(markets_to_contracts(markets))
        favorite = stats[9]
        assert favorite.actual_rate == 1.0
        assert favorite.flb_gap < 0  # implied < actual → negative gap
        assert favorite.recommended_side == "buy_yes"

    def test_no_edge_when_implied_matches_actual(self) -> None:
        """When implied ≈ actual, no statistically significant edge."""
        # 100 markets at 50% implied, 50 resolve YES
        markets = [_market(0.50, i < 50) for i in range(100)]
        stats = compute_decile_stats(markets_to_contracts(markets))
        mid = stats[5]
        assert mid.recommended_side == "no_edge"

    def test_contract_level_distribution_symmetry(self) -> None:
        """Contract-level analysis creates symmetric distribution."""
        markets = []
        for d in range(10):
            price = 0.05 + d * 0.10  # 0.05, 0.15, ..., 0.95
            for _ in range(10):
                markets.append(_market(price, True))
        stats = compute_decile_stats(markets_to_contracts(markets))

        # Each decile d gets original contracts + complementary decile (9-d) contracts
        # decile 0: 10 from price=0.05 + 10 from price=0.95 -> total 20
        # decile 1: 10 from price=0.15 + 10 from price=0.85 -> total 20
        # ...
        # decile 4: 10 from price=0.45 + 10 from price=0.55 -> total 20
        # decile 5: 10 from price=0.55 + 10 from price=0.45 -> total 20
        # decile 9: 10 from price=0.95 + 10 from price=0.05 -> total 20
        expected_counts = [20, 20, 20, 20, 20, 20, 20, 20, 20, 20]
        actual_counts = [s.n for s in stats]
        assert actual_counts == expected_counts


# ── Sample Gate ──────────────────────────────────────────────────────────────


class TestSampleGate:
    def _markets_with_signal_counts(
        self, longshot_n: int, favorite_n: int
    ) -> list[ResolvedMarket]:
        """Build markets with specified counts in runtime FLB signal buckets."""
        return (
            [_market(0.05, False) for _ in range(longshot_n)]
            + [_market(0.95, True) for _ in range(favorite_n)]
        )

    def test_gate_passes_when_both_buckets_sufficient(self) -> None:
        markets = self._markets_with_signal_counts(150, 120)
        gate = check_sample_gate(markets)
        assert gate.passed is True

    def test_gate_fails_when_longshot_insufficient(self) -> None:
        markets = self._markets_with_signal_counts(50, 120)
        gate = check_sample_gate(markets)
        assert gate.passed is False
        assert gate.longshot_passed is False
        assert gate.favorite_passed is True

    def test_gate_fails_when_favorite_insufficient(self) -> None:
        markets = self._markets_with_signal_counts(150, 30)
        gate = check_sample_gate(markets)
        assert gate.passed is False
        assert gate.longshot_passed is True
        assert gate.favorite_passed is False

    def test_gate_fails_when_both_insufficient(self) -> None:
        markets = self._markets_with_signal_counts(10, 10)
        gate = check_sample_gate(markets)
        assert gate.passed is False


# ── Report Generation ───────────────────────────────────────────────────────


class TestReportGeneration:
    def test_report_contains_gate_section(self) -> None:
        markets = [_market(0.50, True)]
        stats = compute_decile_stats(markets_to_contracts(markets))
        gate = check_sample_gate(markets)
        report = generate_report(
            markets=markets,
            contracts=markets_to_contracts(markets),
            decile_stats=stats,
            gate=gate,
            fetched_at=datetime(2026, 5, 3, tzinfo=UTC),
        )
        assert "Sample Gate" in report
        assert "Longshot" in report
        assert "Favorite" in report

    def test_report_contains_decile_table(self) -> None:
        markets = [_market(0.50, True)]
        stats = compute_decile_stats(markets_to_contracts(markets))
        gate = check_sample_gate(markets)
        report = generate_report(
            markets=markets,
            contracts=markets_to_contracts(markets),
            decile_stats=stats,
            gate=gate,
            fetched_at=datetime(2026, 5, 3, tzinfo=UTC),
        )
        assert "FLB by Probability Decile" in report
        assert "Implied P" in report
        assert "Actual Rate" in report

    def test_report_contains_side_semantics(self) -> None:
        markets = [_market(0.50, True)]
        stats = compute_decile_stats(markets_to_contracts(markets))
        gate = check_sample_gate(markets)
        report = generate_report(
            markets=markets,
            contracts=markets_to_contracts(markets),
            decile_stats=stats,
            gate=gate,
            fetched_at=datetime(2026, 5, 3, tzinfo=UTC),
        )
        assert "Side Semantics" in report
        assert "BUY NO" in report
        assert "BUY YES" in report

    def test_report_shows_not_viable_when_gate_fails(self) -> None:
        markets = [_market(0.50, True)]
        stats = compute_decile_stats(markets_to_contracts(markets))
        gate = check_sample_gate(markets)
        report = generate_report(
            markets=markets,
            contracts=markets_to_contracts(markets),
            decile_stats=stats,
            gate=gate,
            fetched_at=datetime(2026, 5, 3, tzinfo=UTC),
        )
        assert "NOT VIABLE" in report

    def test_report_shows_viable_when_gate_passes(self) -> None:
        # Build enough markets to pass the gate.
        markets = []
        for _ in range(120):
            markets.append(_market(0.05, False))  # longshot bucket
            markets.append(_market(0.95, True))   # favorite bucket
        stats = compute_decile_stats(markets_to_contracts(markets))
        gate = check_sample_gate(markets)
        report = generate_report(
            markets=markets,
            contracts=markets_to_contracts(markets),
            decile_stats=stats,
            gate=gate,
            fetched_at=datetime(2026, 5, 3, tzinfo=UTC),
        )
        assert "NOT VIABLE" not in report

    def test_report_contains_category_breakdown(self) -> None:
        markets = [
            _market(0.50, True, category="politics"),
            _market(0.30, False, category="sports"),
        ]
        stats = compute_decile_stats(markets_to_contracts(markets))
        gate = check_sample_gate(markets)
        report = generate_report(
            markets=markets,
            contracts=markets_to_contracts(markets),
            decile_stats=stats,
            gate=gate,
            fetched_at=datetime(2026, 5, 3, tzinfo=UTC),
        )
        assert "Market Categories" in report
        assert "politics" in report
        assert "sports" in report

    def test_report_uses_runtime_signal_boundary_language(self) -> None:
        """Report gate rows should match runtime calibration signal buckets."""
        markets = [_market(0.50, True)]
        stats = compute_decile_stats(markets_to_contracts(markets))
        gate = check_sample_gate(markets)
        report = generate_report(
            markets=markets,
            contracts=markets_to_contracts(markets),
            decile_stats=stats,
            gate=gate,
            fetched_at=datetime(2026, 5, 3, tzinfo=UTC),
        )
        assert "YES < 10%" in report
        assert "YES > 90%" in report


# ── P1 Regression: outcomePrices ordering ───────────────────────────────────


class TestOutcomePricesOrdering:
    """Regression: Gamma API can return outcomes in ['No', 'Yes'] order.

    P1 bug fix: the parser must use the outcomes array to determine which
    index in outcomePrices is YES, not assume index 0.
    """

    def _make_row(
        self,
        outcomes: list[str],
        prices: list[str],
        *,
        resolved_idx: int = 0,
    ) -> dict[str, object]:
        """Build a Gamma API row with specified outcome ordering."""
        # Set one price to 1.0 (resolved) and the other to 0.0.
        resolved_prices = list(prices)
        resolved_prices[resolved_idx] = "1.0"
        resolved_prices[1 - resolved_idx] = "0.0"
        return {
            "id": "test-001",
            "question": "Test market",
            "outcomes": outcomes,
            "outcomePrices": resolved_prices,
            "lastTradePrice": 0.75,
            "volumeNum": 10000.0,
            "liquidityNum": 500.0,
            "endDate": "2026-05-01",
            "slug": "test-market",
        }

    def test_yes_no_order_resolves_yes(self) -> None:
        """['Yes', 'No'] with YES=1.0 → resolved_yes=True."""
        row = self._make_row(["Yes", "No"], ["0.0", "0.0"], resolved_idx=0)
        market = _parse_market(row)
        assert market is not None
        assert market.resolved_yes is True

    def test_no_yes_order_resolves_yes(self) -> None:
        """['No', 'Yes'] with YES=1.0 (index 1) → resolved_yes=True.

        This is the P1 regression: without the fix, the parser would see
        prices[0]=0.0 and prices[1]=1.0 and incorrectly conclude resolved_no.
        """
        row = self._make_row(["No", "Yes"], ["0.0", "0.0"], resolved_idx=1)
        market = _parse_market(row)
        assert market is not None
        assert market.resolved_yes is True

    def test_yes_no_order_resolves_no(self) -> None:
        """['Yes', 'No'] with NO=1.0 (index 1) → resolved_yes=False."""
        row = self._make_row(["Yes", "No"], ["0.0", "0.0"], resolved_idx=1)
        market = _parse_market(row)
        assert market is not None
        assert market.resolved_yes is False

    def test_no_yes_order_resolves_no(self) -> None:
        """['No', 'Yes'] with NO=1.0 (index 0) → resolved_yes=False."""
        row = self._make_row(["No", "Yes"], ["0.0", "0.0"], resolved_idx=0)
        market = _parse_market(row)
        assert market is not None
        assert market.resolved_yes is False

    def test_flipped_order_does_not_flip_resolution(self) -> None:
        """Both orderings with same resolution should agree on resolved_yes.

        Builds two rows: one with ['Yes', 'No'] and one with ['No', 'Yes'],
        both resolving YES. Both must produce resolved_yes=True.
        """
        row_std = self._make_row(["Yes", "No"], ["0.0", "0.0"], resolved_idx=0)
        row_flip = self._make_row(["No", "Yes"], ["0.0", "0.0"], resolved_idx=1)
        mkt_std = _parse_market(row_std)
        mkt_flip = _parse_market(row_flip)
        assert mkt_std is not None
        assert mkt_flip is not None
        assert mkt_std.resolved_yes == mkt_flip.resolved_yes is True


# ── P2 Regression: 90% boundary in favorite bucket ─────────────────────────


class TestNinetyPercentBoundary:
    """P2: keep contract deciles and runtime signal gates distinct."""

    def test_exactly_90_in_favorite_decile(self) -> None:
        """Markets at exactly 90% should be in decile 9."""
        assert _assign_decile(0.90) == 9

    def test_exactly_90_not_counted_in_signal_sample_gate(self) -> None:
        """Runtime favorite calibration uses the strict YES > 90% signal bucket."""
        markets = [_market(0.90, True) for _ in range(120)]
        gate = check_sample_gate(markets)
        assert gate.favorite_count == 0
        assert gate.favorite_passed is False

    def test_just_below_90_not_in_favorite(self) -> None:
        """Markets at 89.9% should be in decile 8, not the favorite bucket."""
        assert _assign_decile(0.899) == 8

    def test_boundary_does_not_overstate_favorite_sample(self) -> None:
        """Only markets >90% should count; 89% markets should not inflate."""
        # 50 markets at 89% (decile 8) + 50 at 91% (decile 9)
        markets = [_market(0.89, True) for _ in range(50)]
        markets += [_market(0.91, True) for _ in range(50)]
        stats = compute_decile_stats(markets_to_contracts(markets))
        # Only the 91% markets should be in the favorite bucket
        assert stats[9].n == 50
        assert stats[8].n == 50
        gate = check_sample_gate(markets)
        assert gate.favorite_count == 50


# ── Regression: binary-only parser ──────────────────────────────────────────


class TestBinaryOnlyParser:
    """Regression: parser must reject 3+ outcome markets.

    CodeRabbit catch: after fixing outcome ordering, the parser still
    accepted multi-outcome rows where `outcomes` contained "Yes" among
    other entries. `no_idx = 1 - yes_idx` is only valid for exactly
    binary Yes/No markets.
    """

    def _make_row(self, outcomes: list[str], prices: list[str]) -> dict[str, object]:
        return {
            "id": "test-001",
            "question": "Test market",
            "outcomes": outcomes,
            "outcomePrices": prices,
            "lastTradePrice": 0.75,
            "volumeNum": 10000.0,
            "liquidityNum": 500.0,
            "endDate": "2026-05-01",
            "slug": "test-market",
        }

    def test_binary_yes_no_accepted(self) -> None:
        """Exactly ['Yes', 'No'] should be accepted."""
        row = self._make_row(["Yes", "No"], ["1.0", "0.0"])
        assert _parse_market(row) is not None

    def test_binary_no_yes_accepted(self) -> None:
        """Exactly ['No', 'Yes'] should be accepted."""
        row = self._make_row(["No", "Yes"], ["0.0", "1.0"])
        assert _parse_market(row) is not None

    def test_three_outcomes_rejected(self) -> None:
        """3-outcome market (e.g. Yes/No/Draw) should return None."""
        row = self._make_row(["Yes", "No", "Draw"], ["0.5", "0.3", "0.2"])
        assert _parse_market(row) is None

    def test_four_outcomes_rejected(self) -> None:
        """4-outcome market should return None."""
        row = self._make_row(
            ["Yes", "No", "Candidate A", "Candidate B"],
            ["0.3", "0.3", "0.2", "0.2"],
        )
        assert _parse_market(row) is None

    def test_yes_with_extra_outcome_rejected(self) -> None:
        """['Yes', 'Other'] (not No) should return None."""
        row = self._make_row(["Yes", "Other"], ["1.0", "0.0"])
        assert _parse_market(row) is None

    def test_single_outcome_rejected(self) -> None:
        """Single-outcome row should return None."""
        row = self._make_row(["Yes"], ["1.0"])
        assert _parse_market(row) is None


# ── Regression: median volume ───────────────────────────────────────────────


class TestMedianVolume:
    """Regression: median volume must use statistics.median().

    Previous implementation used `sorted(volumes)[len(volumes) // 2]`
    which returns the upper-middle value for even-length lists instead
    of averaging the two middle values.
    """

    def test_even_count_averages_middle_two(self) -> None:
        """4 markets with volumes [100, 200, 300, 400] → median = 250."""
        markets = [
            _market(0.50, True, volume=100.0),
            _market(0.51, True, volume=200.0),
            _market(0.52, True, volume=300.0),
            _market(0.53, True, volume=400.0),
        ]
        stats = compute_decile_stats(markets_to_contracts(markets))
        gate = check_sample_gate(markets)
        report = generate_report(
            markets=markets,
            contracts=markets_to_contracts(markets),
            decile_stats=stats,
            gate=gate,
            fetched_at=datetime(2026, 5, 3, tzinfo=UTC),
        )
        # statistics.median([100, 200, 300, 400]) = 250.0
        assert "$250" in report

    def test_odd_count_takes_middle(self) -> None:
        """3 markets with volumes [100, 200, 300] → median = 200."""
        markets = [
            _market(0.50, True, volume=100.0),
            _market(0.51, True, volume=200.0),
            _market(0.52, True, volume=300.0),
        ]
        stats = compute_decile_stats(markets_to_contracts(markets))
        gate = check_sample_gate(markets)
        report = generate_report(
            markets=markets,
            contracts=markets_to_contracts(markets),
            decile_stats=stats,
            gate=gate,
            fetched_at=datetime(2026, 5, 3, tzinfo=UTC),
        )
        assert "$200" in report


class TestSettlementCriteria:
    """Regression: parser must reject closed-but-unsettled markets.

    Gamma API has no explicit settlement flag.  The parser uses price-based
    heuristics to distinguish oracle-settled markets (prices at 0.0/1.0)
    from closed-but-unsettled markets (prices at e.g. 0.995/0.005).

    Previous tolerance of 0.01 admitted near-resolution prices as ground
    truth, corrupting the FLB label set.
    """

    @staticmethod
    def _gamma_row(
        outcomes: str = '["Yes", "No"]',
        outcome_prices: str = '["1.0", "0.0"]',
        last_trade_price: float = 0.05,
        volume: float = 10_000.0,
    ) -> dict[str, object]:
        """Build a minimal Gamma API row dict for _parse_market()."""
        return {
            "id": "test-123",
            "question": "Test market?",
            "outcomes": outcomes,
            "outcomePrices": outcome_prices,
            "lastTradePrice": last_trade_price,
            "volumeNum": volume,
            "liquidityNum": 500.0,
            "endDate": "2026-05-01",
            "slug": "test-market-sports",
        }

    def test_exact_payout_accepted(self) -> None:
        """Exact 1.0/0.0 payout vector is accepted."""
        row = self._gamma_row(outcome_prices='["1.0", "0.0"]')
        result = _parse_market(row)
        assert result is not None
        assert result.resolved_yes is True

    def test_exact_reverse_payout_accepted(self) -> None:
        """Exact 0.0/1.0 payout vector (NO wins) is accepted."""
        row = self._gamma_row(outcome_prices='["0.0", "1.0"]')
        result = _parse_market(row)
        assert result is not None
        assert result.resolved_yes is False

    def test_amm_residual_accepted(self) -> None:
        """AMM residual prices (0.999999/0.000001) are accepted."""
        row = self._gamma_row(
            outcome_prices='["0.9999989889179475", "0.0000010110820525"]'
        )
        result = _parse_market(row)
        assert result is not None

    def test_near_resolution_rejected(self) -> None:
        """0.995/0.005 near-resolution prices are rejected.

        This is the key regression: closed-but-unsettled markets at
        0.995/0.005 must NOT be treated as oracle-settled ground truth.
        """
        row = self._gamma_row(
            outcome_prices='["0.995", "0.005"]'
        )
        assert _parse_market(row) is None

    def test_99_1_percent_rejected(self) -> None:
        """0.99/0.01 prices are rejected (above 0.001 tolerance)."""
        row = self._gamma_row(
            outcome_prices='["0.99", "0.01"]'
        )
        assert _parse_market(row) is None

    def test_midrange_rejected(self) -> None:
        """Mid-range prices (0.58/0.42) are rejected."""
        row = self._gamma_row(
            outcome_prices='["0.58", "0.42"]'
        )
        assert _parse_market(row) is None

    def test_degenerate_zero_zero_rejected(self) -> None:
        """Both prices at 0.0 (cleared data) are rejected."""
        row = self._gamma_row(
            outcome_prices='["0", "0"]',
            last_trade_price=0.0,
        )
        assert _parse_market(row) is None


class TestContractLevelAnalysis:
    """Regression: each binary market contributes two contract observations.

    A binary market at YES price p contributes:
    - YES contract at price p (pays out if resolved YES)
    - NO contract at price 1-p (pays out if resolved NO)

    This ensures the sample gate counts the full opportunity set.
    """

    def test_single_market_yields_two_contracts(self) -> None:
        """One market at 0.08 YES contributes YES@0.08 and NO@0.92 contracts."""
        markets = [
            ResolvedMarket(
                market_id="m1",
                question="Will X happen?",
                yes_price=0.08,
                resolved_yes=True,  # YES resolved to 1.0
                volume=1000.0,
                liquidity=100.0,
                end_date="2026-05-01",
                category="sports",
            )
        ]

        contracts = markets_to_contracts(markets)
        assert len(contracts) == 2

        # YES contract: price 0.08, pays out because resolved_yes=True
        yes_contract = next(c for c in contracts if c.contract_side == "YES")
        assert yes_contract.entry_price == 0.08
        assert yes_contract.pays_out is True  # YES resolved to 1.0
        assert yes_contract.market_id == "m1"

        # NO contract: price 1.0 - 0.08 = 0.92, doesn't pay out because resolved_yes=True
        no_contract = next(c for c in contracts if c.contract_side == "NO")
        assert no_contract.entry_price == 0.92
        assert no_contract.pays_out is False  # NO resolved to 0.0
        assert no_contract.market_id == "m1"

    def test_no_resolution_flips_payout(self) -> None:
        """Market at 0.92 YES that resolves NO creates YES@0.92(NO payout) and NO@0.08(NO payout)."""
        markets = [
            ResolvedMarket(
                market_id="m2",
                question="Will Y happen?",
                yes_price=0.92,
                resolved_yes=False,  # YES resolved to 0.0, NO to 1.0
                volume=2000.0,
                liquidity=200.0,
                end_date="2026-05-01",
                category="politics",
            )
        ]

        contracts = markets_to_contracts(markets)
        assert len(contracts) == 2

        # YES contract: price 0.92, doesn't pay out because resolved_yes=False
        yes_contract = next(c for c in contracts if c.contract_side == "YES")
        assert yes_contract.entry_price == 0.92
        assert yes_contract.pays_out is False  # YES resolved to 0.0

        # NO contract: price 1.0 - 0.92 = 0.08, pays out because resolved_yes=False means NO won
        no_contract = next(c for c in contracts if c.contract_side == "NO")
        assert pytest.approx(no_contract.entry_price, abs=1e-10) == 0.08
        assert no_contract.pays_out is True  # NO resolved to 1.0

    def test_contract_level_decile_assignment(self) -> None:
        """A market at 0.08 YES creates contracts in different deciles: YES@0.08→decile0, NO@0.92→decile9."""
        markets = [
            ResolvedMarket(
                market_id="m3",
                question="Low prob event?",
                yes_price=0.08,  # Longshot
                resolved_yes=False,  # Resolve to NO (meaning YES pays 0, NO pays 1)
                volume=1500.0,
                liquidity=150.0,
                end_date="2026-05-01",
                category="other",
            )
        ]

        contracts = markets_to_contracts(markets)
        stats = compute_decile_stats(contracts)

        # The YES contract at 0.08 should be in decile 0 [0%-10%)
        # The NO contract at 0.92 should be in decile 9 [90%-100%]
        assert stats[0].n == 1  # One contract in longshot bucket
        assert stats[9].n == 1  # One contract in favorite bucket

        # Both contracts should show the relationship where the same underlying
        # market creates opposite FLB signals from two perspectives
        # YES contract: price=0.08, pays_out=False → gap = 0.08 - 0 = +0.08 (overpriced)
        # NO contract: price=0.92, pays_out=True  → gap = 0.92 - 1 = -0.08 (underpriced)
        assert stats[0].implied_prob == 0.08
        assert stats[0].actual_rate == 0.0
        assert stats[0].flb_gap == 0.08  # YES overpriced

        assert stats[9].implied_prob == 0.92
        assert stats[9].actual_rate == 1.0
        assert stats[9].flb_gap == pytest.approx(-0.08, abs=1e-10)  # NO underpriced

    def test_sample_gate_counts_original_yes_price_signal_buckets(self) -> None:
        """Contract deciles are diagnostic; runtime sample gate counts signals."""
        markets = [
            ResolvedMarket(market_id=f"m{i}", question=f"Q{i}", yes_price=0.05,
                          resolved_yes=(i % 2 == 0), volume=1000.0, liquidity=100.0,
                          end_date="2026-05-01", category="other")
            for i in range(3)
        ]

        contracts = markets_to_contracts(markets)
        stats = compute_decile_stats(contracts)
        gate = check_sample_gate(markets)

        # Should have 6 contracts (3 markets × 2)
        assert len(contracts) == 6

        # Each market contributes one contract to longshot bucket [0.05] and one to favorite bucket [0.95]
        longshot_contracts = [c for c in contracts if 0.0 <= c.entry_price < 0.1]
        favorite_contracts = [c for c in contracts if 0.9 <= c.entry_price <= 1.0]

        assert len(longshot_contracts) == 3  # Three 0.05 contracts
        assert len(favorite_contracts) == 3  # Three 0.95 contracts
        assert stats[0].n == 3
        assert stats[9].n == 3

        # Runtime signal gate should not count synthetic NO contracts as
        # favorite_yes_underpriced_buy_yes calibration samples.
        assert gate.longshot_count == 3
        assert gate.favorite_count == 0

    def test_flb_gap_same_magnitude_opposite_sign(self) -> None:
        """FLB gap should have same magnitude for both sides of same market."""
        # For market with YES price p and resolution R (1=YES, 0=NO):
        # YES contract: gap = p - R
        # NO contract: entry_price = (1-p), pays_out = (1-R)
        # NO contract gap = (1-p) - (1-R) = R - p = -(p - R)
        # So the gaps are negatives of each other, same magnitude, opposite sign.

        markets = [
            ResolvedMarket(
                market_id="m4",
                question="Test market",
                yes_price=0.15,
                resolved_yes=True,  # Resolves to YES (1.0)
                volume=1000.0,
                liquidity=100.0,
                end_date="2026-05-01",
                category="test",
            )
        ]

        contracts = markets_to_contracts(markets)
        # Find the specific contracts to verify the relationship
        yes_contract = next(c for c in contracts if c.contract_side == "YES")
        no_contract = next(c for c in contracts if c.contract_side == "NO")

        assert yes_contract.entry_price == 0.15
        assert yes_contract.pays_out is True  # Because resolved_yes=True
        assert no_contract.entry_price == 0.85
        assert no_contract.pays_out is False  # Because resolved_yes=True, so NO resolved to 0

        # Both contracts from same market should contribute to their respective deciles
        # with opposite-signed but same-magnitude FLB gaps
