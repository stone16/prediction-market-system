"""Tests for H1 FLB data feasibility analysis.

Tests the pure-analysis functions (decile assignment, Wilson interval,
sample gate, report generation) without hitting the Gamma API.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from scripts.flb_data_feasibility import (
    DecileStats,
    ResolvedMarket,
    _assign_decile,
    _wilson_interval,
    check_sample_gate,
    compute_decile_stats,
    generate_report,
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

    def test_single_decile(self) -> None:
        """Markets all in the same decile should populate only that bucket."""
        markets = [_market(0.05, False) for _ in range(20)]
        stats = compute_decile_stats(markets)
        assert stats[0].n == 20
        assert all(stats[i].n == 0 for i in range(1, 10))

    def test_flb_pattern_longshots_overpriced(self) -> None:
        """If longshots (YES <10%) mostly resolve NO, FLB gap is positive."""
        # 20 longshot markets at 5% implied, only 1 resolves YES (5% actual)
        # FLB says they should resolve even less → gap = implied - actual
        markets = [_market(0.05, i == 0) for i in range(20)]
        stats = compute_decile_stats(markets)
        longshot = stats[0]
        assert longshot.n == 20
        assert longshot.n_yes == 1
        assert longshot.actual_rate == pytest.approx(0.05)
        # implied ≈ 0.05, actual = 0.05 → gap ≈ 0 (no FLB detected here)

    def test_flb_pattern_longshots_strongly_overpriced(self) -> None:
        """Markets at 5% implied but 0% actual → strong FLB signal."""
        markets = [_market(0.05, False) for _ in range(100)]
        stats = compute_decile_stats(markets)
        longshot = stats[0]
        assert longshot.actual_rate == 0.0
        assert longshot.flb_gap > 0.04  # implied ~5% minus actual 0%
        assert longshot.recommended_side == "buy_no"

    def test_flb_pattern_favorites_underpriced(self) -> None:
        """Markets at 95% implied but 100% actual → underpriced favorites."""
        markets = [_market(0.95, True) for _ in range(100)]
        stats = compute_decile_stats(markets)
        favorite = stats[9]
        assert favorite.actual_rate == 1.0
        assert favorite.flb_gap < 0  # implied < actual → negative gap
        assert favorite.recommended_side == "buy_yes"

    def test_no_edge_when_implied_matches_actual(self) -> None:
        """When implied ≈ actual, no statistically significant edge."""
        # 100 markets at 50% implied, 50 resolve YES
        markets = [_market(0.50, i < 50) for i in range(100)]
        stats = compute_decile_stats(markets)
        mid = stats[5]
        assert mid.recommended_side == "no_edge"

    def test_ten_deciles_populated(self) -> None:
        """Markets spread across all deciles should populate all buckets."""
        markets = []
        for d in range(10):
            price = 0.05 + d * 0.10  # 0.05, 0.15, ..., 0.95
            for _ in range(10):
                markets.append(_market(price, True))
        stats = compute_decile_stats(markets)
        assert all(s.n == 10 for s in stats)


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
        stats = compute_decile_stats(markets)
        gate = check_sample_gate(stats)
        report = generate_report(
            markets=markets,
            decile_stats=stats,
            gate=gate,
            fetched_at=datetime(2026, 5, 3, tzinfo=UTC),
        )
        assert "Sample Gate" in report
        assert "Longshot" in report
        assert "Favorite" in report

    def test_report_contains_decile_table(self) -> None:
        markets = [_market(0.50, True)]
        stats = compute_decile_stats(markets)
        gate = check_sample_gate(stats)
        report = generate_report(
            markets=markets,
            decile_stats=stats,
            gate=gate,
            fetched_at=datetime(2026, 5, 3, tzinfo=UTC),
        )
        assert "FLB by Probability Decile" in report
        assert "Implied P" in report
        assert "Actual Rate" in report

    def test_report_contains_side_semantics(self) -> None:
        markets = [_market(0.50, True)]
        stats = compute_decile_stats(markets)
        gate = check_sample_gate(stats)
        report = generate_report(
            markets=markets,
            decile_stats=stats,
            gate=gate,
            fetched_at=datetime(2026, 5, 3, tzinfo=UTC),
        )
        assert "Side Semantics" in report
        assert "BUY NO" in report
        assert "BUY YES" in report

    def test_report_shows_not_viable_when_gate_fails(self) -> None:
        markets = [_market(0.50, True)]
        stats = compute_decile_stats(markets)
        gate = check_sample_gate(stats)
        report = generate_report(
            markets=markets,
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
        stats = compute_decile_stats(markets)
        gate = check_sample_gate(stats)
        report = generate_report(
            markets=markets,
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
        stats = compute_decile_stats(markets)
        gate = check_sample_gate(stats)
        report = generate_report(
            markets=markets,
            decile_stats=stats,
            gate=gate,
            fetched_at=datetime(2026, 5, 3, tzinfo=UTC),
        )
        assert "Market Categories" in report
        assert "politics" in report
        assert "sports" in report
