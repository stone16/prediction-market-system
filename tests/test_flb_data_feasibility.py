"""Tests for H1 FLB data feasibility analysis.

Tests the pure-analysis functions (decile assignment, Wilson interval,
sample gate, report generation) without hitting the Gamma API.
"""

from __future__ import annotations

import csv
from datetime import UTC, datetime
from pathlib import Path

import pytest

from scripts.flb_data_feasibility import (
    DecileStats,
    ResolvedMarket,
    _assign_decile,
    _parse_market,
    _wilson_interval,
    check_sample_gate,
    compute_decile_stats,
    generate_report,
    load_warehouse_markets,
    markets_to_contracts,
)


# ── Helpers ──────────────────────────────────────────────────────────────────


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

        markets = load_warehouse_markets(path)

        assert len(markets) == 2
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

    def test_rejects_ambiguous_fifty_fifty_payout_vector(self, tmp_path: Path) -> None:
        """Ambiguous 50/50 resolutions are not safe labels for H1 FLB."""
        path = tmp_path / "fifty_fifty.csv"
        _write_warehouse_csv(path, [
            _warehouse_row(yes_payout="0.5", no_payout="0.5")
        ])

        with pytest.raises(ValueError, match="exact final payout vector"):
            load_warehouse_markets(path)

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

    def test_rejects_duplicate_market_ids(self, tmp_path: Path) -> None:
        """Duplicate markets would falsely inflate the contract sample gate."""
        path = tmp_path / "duplicate_markets.csv"
        _write_warehouse_csv(path, [
            _warehouse_row(market_id="duplicated-market"),
            _warehouse_row(market_id="duplicated-market"),
        ])

        with pytest.raises(ValueError, match="duplicate market_id"):
            load_warehouse_markets(path)

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

    def test_warehouse_contracts_can_pass_extreme_sample_gate(
        self, tmp_path: Path
    ) -> None:
        path = tmp_path / "sample_gate.csv"
        _write_warehouse_csv(path, [
            _warehouse_row(market_id=f"m-{i}", entry_yes_price="0.05")
            for i in range(120)
        ])

        markets = load_warehouse_markets(path)
        contracts = markets_to_contracts(markets)
        stats = compute_decile_stats(contracts)
        gate = check_sample_gate(stats)
        report = generate_report(
            markets=markets,
            contracts=contracts,
            decile_stats=stats,
            gate=gate,
            fetched_at=datetime(2026, 5, 3, tzinfo=UTC),
            source_label=f"warehouse CSV: {path}",
        )

        assert len(markets) == 120
        assert gate.longshot_count == 120
        assert gate.favorite_count == 120
        assert gate.passed is True
        assert "H1 DATA VIABLE" in report
        assert "warehouse CSV:" in report


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
    def _stats_with_counts(
        self, longshot_n: int, favorite_n: int
    ) -> list[DecileStats]:
        """Build decile stats with specified counts in target buckets."""
        stats = []
        for d in range(10):
            n = longshot_n if d == 0 else (favorite_n if d == 9 else 0)
            stats.append(DecileStats(
                decile=d,
                lower=d / 10.0,
                upper=(d + 1) / 10.0 if d < 9 else 1.0,
                n=n,
                n_yes=0,
                implied_prob=0.0,
                actual_rate=0.0,
                flb_gap=0.0,
                wilson_lower=0.0,
                wilson_upper=0.0,
                recommended_side="no_edge",
            ))
        return stats

    def test_gate_passes_when_both_buckets_sufficient(self) -> None:
        stats = self._stats_with_counts(150, 120)
        gate = check_sample_gate(stats)
        assert gate.passed is True

    def test_gate_fails_when_longshot_insufficient(self) -> None:
        stats = self._stats_with_counts(50, 120)
        gate = check_sample_gate(stats)
        assert gate.passed is False
        assert gate.longshot_passed is False
        assert gate.favorite_passed is True

    def test_gate_fails_when_favorite_insufficient(self) -> None:
        stats = self._stats_with_counts(150, 30)
        gate = check_sample_gate(stats)
        assert gate.passed is False
        assert gate.longshot_passed is True
        assert gate.favorite_passed is False

    def test_gate_fails_when_both_insufficient(self) -> None:
        stats = self._stats_with_counts(10, 10)
        gate = check_sample_gate(stats)
        assert gate.passed is False


# ── Report Generation ───────────────────────────────────────────────────────


class TestReportGeneration:
    def test_report_contains_gate_section(self) -> None:
        markets = [_market(0.50, True)]
        stats = compute_decile_stats(markets_to_contracts(markets))
        gate = check_sample_gate(stats)
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
        gate = check_sample_gate(stats)
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
        gate = check_sample_gate(stats)
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
        gate = check_sample_gate(stats)
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
        gate = check_sample_gate(stats)
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
        gate = check_sample_gate(stats)
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

    def test_report_uses_consistent_boundary_language(self) -> None:
        """Report should use [0%-10%) and [90%-100%] not <10% and >90%."""
        markets = [_market(0.50, True)]
        stats = compute_decile_stats(markets_to_contracts(markets))
        gate = check_sample_gate(stats)
        report = generate_report(
            markets=markets,
            contracts=markets_to_contracts(markets),
            decile_stats=stats,
            gate=gate,
            fetched_at=datetime(2026, 5, 3, tzinfo=UTC),
        )
        assert "[0%-10%)" in report
        assert "[90%-100%]" in report


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
    """P2: verify that exactly 90% markets land in decile 9 (favorite bucket).

    The sample gate counts decile 9 as the favorite bucket. Markets at
    exactly 90% should be counted, not excluded.
    """

    def test_exactly_90_in_favorite_decile(self) -> None:
        """Markets at exactly 90% should be in decile 9."""
        assert _assign_decile(0.90) == 9

    def test_exactly_90_counted_in_sample_gate(self) -> None:
        """120 markets at exactly 90% should pass the favorite gate."""
        markets = [_market(0.90, True) for _ in range(120)]
        stats = compute_decile_stats(markets_to_contracts(markets))
        gate = check_sample_gate(stats)
        assert gate.favorite_count == 120
        assert gate.favorite_passed is True

    def test_just_below_90_not_in_favorite(self) -> None:
        """Markets at 89.9% should be in decile 8, not the favorite bucket."""
        assert _assign_decile(0.899) == 8

    def test_boundary_does_not_overstate_favorite_sample(self) -> None:
        """Only markets ≥90% should count; 89% markets should not inflate."""
        # 50 markets at 89% (decile 8) + 50 at 91% (decile 9)
        markets = [_market(0.89, True) for _ in range(50)]
        markets += [_market(0.91, True) for _ in range(50)]
        stats = compute_decile_stats(markets_to_contracts(markets))
        # Only the 91% markets should be in the favorite bucket
        assert stats[9].n == 50
        assert stats[8].n == 50


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
        gate = check_sample_gate(stats)
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
        gate = check_sample_gate(stats)
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

    def test_sample_gate_counts_contracts_not_markets(self) -> None:
        """With 3 markets, should get 6 contracts, affecting sample gate."""
        markets = [
            ResolvedMarket(market_id=f"m{i}", question=f"Q{i}", yes_price=0.05,
                          resolved_yes=(i % 2 == 0), volume=1000.0, liquidity=100.0,
                          end_date="2026-05-01", category="other")
            for i in range(3)
        ]

        contracts = markets_to_contracts(markets)
        stats = compute_decile_stats(contracts)
        gate = check_sample_gate(stats)

        # Should have 6 contracts (3 markets × 2)
        assert len(contracts) == 6

        # Each market contributes one contract to longshot bucket [0.05] and one to favorite bucket [0.95]
        longshot_contracts = [c for c in contracts if 0.0 <= c.entry_price < 0.1]
        favorite_contracts = [c for c in contracts if 0.9 <= c.entry_price <= 1.0]

        assert len(longshot_contracts) == 3  # Three 0.05 contracts
        assert len(favorite_contracts) == 3  # Three 0.95 contracts

        # Sample gate should reflect contract counts
        assert gate.longshot_count == 3
        assert gate.favorite_count == 3

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
