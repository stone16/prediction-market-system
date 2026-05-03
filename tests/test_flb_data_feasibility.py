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
    _parse_market,
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

    def test_report_uses_consistent_boundary_language(self) -> None:
        """Report should use [0%-10%) and [90%-100%] not <10% and >90%."""
        markets = [_market(0.50, True)]
        stats = compute_decile_stats(markets)
        gate = check_sample_gate(stats)
        report = generate_report(
            markets=markets,
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
        stats = compute_decile_stats(markets)
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
        stats = compute_decile_stats(markets)
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
        stats = compute_decile_stats(markets)
        gate = check_sample_gate(stats)
        report = generate_report(
            markets=markets,
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
        stats = compute_decile_stats(markets)
        gate = check_sample_gate(stats)
        report = generate_report(
            markets=markets,
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
