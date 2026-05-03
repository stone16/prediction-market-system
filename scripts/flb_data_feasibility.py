"""H1 FLB Data Feasibility Analysis.

Fetches resolved Polymarket markets from the Gamma API, measures
Favorite-Longshot Bias (FLB) by probability decile with Wilson score
confidence intervals, and evaluates H1 strategy viability.

Usage:
    uv run python scripts/flb_data_feasibility.py [--limit N] [--output PATH]

Exit codes:
    0 — H1 viable (sample gate passed)
    1 — H1 not viable (insufficient data in target buckets)

Limitations:
    - **Entry price proxy:** Uses ``lastTradePrice`` from the Gamma API,
      which is the last trade before resolution — NOT a timestamped entry
      snapshot. This means the "implied probability" is measured at or very
      near resolution time, not at a hypothetical entry point. For a real
      strategy P&L backtest, we would need timestamped price snapshots at
      a defined entry horizon (e.g., 24h before resolution).
    - **Data source:** The Gamma API ``closed=true`` endpoint returns a
      small window of recently resolved markets (typically <50). For
      statistically robust FLB measurement (≥100 contracts per target
      bucket), we need Dune Analytics on-chain data or a historical
      warehouse with full Polymarket resolution history.
    - **This script is a feasibility check**, not a strategy backtest.
      It answers: "Does Polymarket data show measurable FLB?" — not
      "Would FLB trading have been profitable after fees and slippage?"
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx

GAMMA_API_BASE = "https://gamma-api.polymarket.com"
SAMPLE_GATE_MIN = 100  # minimum resolved contracts per target bucket
DECILE_BOUNDARIES = [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
TARGET_BUCKETS = [0, 9]  # decile 0 = [0%, 10%), decile 9 = [90%, 100%]
WILSON_Z = 1.96  # 95% confidence


# ── Data Structures ─────────────────────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class ResolvedMarket:
    """A single resolved Polymarket binary market."""

    market_id: str
    question: str
    yes_price: float  # last trade price before resolution (market-implied P(YES))
    resolved_yes: bool  # True if market resolved YES
    volume: float
    liquidity: float
    end_date: str
    category: str  # extracted from slug/group or "uncategorized"


@dataclass(frozen=True, slots=True)
class DecileStats:
    """FLB statistics for one probability decile."""

    decile: int  # 0-9
    lower: float  # decile lower bound (inclusive)
    upper: float  # decile upper bound (exclusive, 1.0 for decile 9)
    n: int  # total markets in this decile
    n_yes: int  # markets that resolved YES
    implied_prob: float  # mean market-implied P(YES)
    actual_rate: float  # actual YES resolution rate
    flb_gap: float  # implied_prob - actual_rate (positive = overpriced)
    wilson_lower: float  # Wilson lower bound on actual_rate
    wilson_upper: float  # Wilson upper bound on actual_rate
    recommended_side: str  # "buy_yes", "buy_no", or "no_edge"


# ── Data Fetching ────────────────────────────────────────────────────────────


def fetch_resolved_markets(
    *,
    limit: int = 500,
    max_pages: int = 20,
) -> list[ResolvedMarket]:
    """Fetch resolved (closed) markets from the Polymarket Gamma API.

    Paginates through the API collecting markets where ``closed=true``.
    Only binary (YES/NO) markets with valid resolution data are included.
    """
    markets: list[ResolvedMarket] = []
    offset = 0

    with httpx.Client(base_url=GAMMA_API_BASE, timeout=30.0) as client:
        for _ in range(max_pages):
            response = client.get(
                "/markets",
                params={
                    "closed": "true",
                    "limit": str(limit),
                    "offset": str(offset),
                },
            )
            response.raise_for_status()
            payload = response.json()

            if not isinstance(payload, list) or not payload:
                break

            for row in payload:
                market = _parse_market(row)
                if market is not None:
                    markets.append(market)

            offset += limit

            # Stop early if the API returned fewer than requested.
            if len(payload) < limit:
                break

    return markets


def _parse_market(row: dict[str, Any]) -> ResolvedMarket | None:
    """Parse a Gamma API market row into a ResolvedMarket, or None if invalid."""
    try:
        # Must be a binary market with YES/NO outcomes.
        outcomes_raw = row.get("outcomes")
        if isinstance(outcomes_raw, str):
            outcomes = json.loads(outcomes_raw)
        else:
            outcomes = outcomes_raw
        # Only accept exactly binary Yes/No markets.
        # 3+ outcome markets (e.g. multi-party elections) have different
        # resolution semantics and would corrupt the FLB dataset.
        if (
            not isinstance(outcomes, list)
            or len(outcomes) != 2
            or set(outcomes) != {"Yes", "No"}
        ):
            return None

        # Parse outcome prices to determine resolution and last price.
        # CRITICAL: outcomePrices ordering follows the outcomes array.
        # ["Yes", "No"] → prices[0]=YES, prices[1]=NO
        # ["No", "Yes"] → prices[0]=NO,  prices[1]=YES
        prices_raw = row.get("outcomePrices")
        if isinstance(prices_raw, str):
            prices = json.loads(prices_raw)
        else:
            prices = prices_raw
        if not isinstance(prices, list) or len(prices) != 2:
            return None

        yes_idx = outcomes.index("Yes")
        no_idx = 1 - yes_idx  # binary market: the other index
        yes_price = float(prices[yes_idx])
        no_price = float(prices[no_idx])

        # A resolved market has one side at 1.0 and the other at 0.0.
        # Use a tolerance for floating-point representation.
        if abs(yes_price - 1.0) < 0.01:
            resolved_yes = True
        elif abs(no_price - 1.0) < 0.01:
            resolved_yes = False
        else:
            # Not clearly resolved — skip.
            return None

        # The "entry" price is the last trade price before resolution.
        # For resolved markets this is the best proxy for the market-implied
        # probability at the time a trader would have entered.
        entry_price = row.get("lastTradePrice")
        if entry_price is None:
            # Fall back to the pre-resolution price from outcomePrices.
            # For resolved YES markets, the pre-resolution YES price is
            # approximately yes_price (which is now 1.0), so we use
            # lastTradePrice when available.
            return None
        entry_price = float(entry_price)

        # Skip degenerate prices (exactly 0 or 1 — no trading signal).
        if entry_price <= 0.0 or entry_price >= 1.0:
            return None

        volume = float(row.get("volumeNum", row.get("volume", 0)))
        liquidity = float(row.get("liquidityNum", row.get("liquidity", 0)))
        end_date = row.get("endDate", row.get("endDateIso", ""))
        question = row.get("question", "")
        market_id = str(row.get("id", row.get("conditionId", "")))

        # Extract category from slug or groupItemTitle.
        slug = row.get("slug", "")
        category = _extract_category(slug, question)

        return ResolvedMarket(
            market_id=market_id,
            question=question,
            yes_price=entry_price,
            resolved_yes=resolved_yes,
            volume=volume,
            liquidity=liquidity,
            end_date=end_date,
            category=category,
        )
    except (KeyError, TypeError, ValueError, json.JSONDecodeError):
        return None


def _extract_category(slug: str, question: str) -> str:
    """Best-effort category extraction from slug or question text."""
    # Common Polymarket slug patterns: "will-X-happen", "election-winner"
    political_keywords = {"election", "president", "senate", "governor", "poll", "vote"}
    crypto_keywords = {"bitcoin", "btc", "ethereum", "eth", "crypto", "solana"}
    sports_keywords = {"nba", "nfl", "mlb", "soccer", "football", "championship", "winner"}
    culture_keywords = {"oscar", "grammy", "emmy", "movie", "album"}

    text = (slug + " " + question).lower()
    for kw in political_keywords:
        if kw in text:
            return "politics"
    for kw in crypto_keywords:
        if kw in text:
            return "crypto"
    for kw in sports_keywords:
        if kw in text:
            return "sports"
    for kw in culture_keywords:
        if kw in text:
            return "culture"
    return "other"


# ── FLB Analysis ─────────────────────────────────────────────────────────────


def compute_decile_stats(markets: list[ResolvedMarket]) -> list[DecileStats]:
    """Compute FLB statistics for each probability decile.

    Markets are bucketed by their ``yes_price`` into deciles [0-10%),
    [10-20%), ..., [90-100%]. For each decile we compute the mean implied
    probability, the actual YES resolution rate, the FLB gap, and a Wilson
    score confidence interval.
    """
    # Sort markets by yes_price for decile assignment.
    sorted_markets = sorted(markets, key=lambda m: m.yes_price)

    # Assign each market to a decile.
    decile_buckets: dict[int, list[ResolvedMarket]] = {i: [] for i in range(10)}
    for market in sorted_markets:
        decile = _assign_decile(market.yes_price)
        decile_buckets[decile].append(market)

    stats: list[DecileStats] = []
    for d in range(10):
        bucket = decile_buckets[d]
        n = len(bucket)
        if n == 0:
            stats.append(DecileStats(
                decile=d,
                lower=DECILE_BOUNDARIES[d],
                upper=DECILE_BOUNDARIES[d + 1],
                n=0,
                n_yes=0,
                implied_prob=0.0,
                actual_rate=0.0,
                flb_gap=0.0,
                wilson_lower=0.0,
                wilson_upper=0.0,
                recommended_side="no_edge",
            ))
            continue

        n_yes = sum(1 for m in bucket if m.resolved_yes)
        implied_prob = sum(m.yes_price for m in bucket) / n
        actual_rate = n_yes / n
        flb_gap = implied_prob - actual_rate

        wilson_lower, wilson_upper = _wilson_interval(n_yes, n)

        # Determine recommended side based on FLB gap significance.
        # If the Wilson interval doesn't contain the implied probability,
        # the mispricing is statistically significant.
        if actual_rate > 0 and wilson_lower > implied_prob:
            # Favorites underpriced → buy YES
            recommended_side = "buy_yes"
        elif actual_rate < 1 and wilson_upper < implied_prob:
            # Longshots overpriced → buy NO
            recommended_side = "buy_no"
        else:
            recommended_side = "no_edge"

        stats.append(DecileStats(
            decile=d,
            lower=DECILE_BOUNDARIES[d],
            upper=DECILE_BOUNDARIES[d + 1],
            n=n,
            n_yes=n_yes,
            implied_prob=implied_prob,
            actual_rate=actual_rate,
            flb_gap=flb_gap,
            wilson_lower=wilson_lower,
            wilson_upper=wilson_upper,
            recommended_side=recommended_side,
        ))

    return stats


def _assign_decile(price: float) -> int:
    """Assign a price to a decile bucket [0, 9]."""
    if price <= 0.0:
        return 0
    if price >= 1.0:
        return 9
    decile = int(price * 10)
    return min(decile, 9)


def _wilson_interval(successes: int, trials: int) -> tuple[float, float]:
    """Wilson score interval for a binomial proportion (95% CI).

    Returns (lower, upper) bounds. Handles edge cases (0 trials, 0 or all
    successes) gracefully.
    """
    if trials == 0:
        return (0.0, 1.0)

    p_hat = successes / trials
    z2 = WILSON_Z ** 2
    denominator = 1.0 + z2 / trials
    center = (p_hat + z2 / (2.0 * trials)) / denominator
    spread = (WILSON_Z / denominator) * math.sqrt(
        p_hat * (1.0 - p_hat) / trials + z2 / (4.0 * trials * trials)
    )

    lower = max(0.0, center - spread)
    upper = min(1.0, center + spread)
    return (lower, upper)


# ── Sample Gate ──────────────────────────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class SampleGateResult:
    """Result of the H1 sample gate check."""

    longshot_count: int  # markets in decile 0 [0%, 10%)
    favorite_count: int  # markets in decile 9 [90%, 100%]
    longshot_passed: bool
    favorite_passed: bool
    passed: bool  # both buckets meet minimum


def check_sample_gate(decile_stats: list[DecileStats]) -> SampleGateResult:
    """Check whether enough data exists in the target FLB buckets.

    The sample gate requires ≥100 resolved contracts in both the [0%, 10%)
    (longshot) and [90%, 100%] (favorite) buckets.
    """
    longshot_count = decile_stats[0].n
    favorite_count = decile_stats[9].n

    longshot_passed = longshot_count >= SAMPLE_GATE_MIN
    favorite_passed = favorite_count >= SAMPLE_GATE_MIN

    return SampleGateResult(
        longshot_count=longshot_count,
        favorite_count=favorite_count,
        longshot_passed=longshot_passed,
        favorite_passed=favorite_passed,
        passed=longshot_passed and favorite_passed,
    )


# ── Report Generation ───────────────────────────────────────────────────────


def generate_report(
    *,
    markets: list[ResolvedMarket],
    decile_stats: list[DecileStats],
    gate: SampleGateResult,
    fetched_at: datetime,
) -> str:
    """Generate a Markdown report of the FLB feasibility analysis."""
    lines: list[str] = []
    lines.append("# H1 FLB Data Feasibility Report")
    lines.append("")
    lines.append(f"**Generated:** {fetched_at.isoformat()}")
    lines.append(f"**Total resolved markets analyzed:** {len(markets)}")
    lines.append("")

    # Gate result.
    gate_emoji = "✅" if gate.passed else "❌"
    lines.append(f"## Sample Gate: {gate_emoji}")
    lines.append("")
    lines.append(f"| Bucket | Count | Required | Status |")
    lines.append(f"|--------|-------|----------|--------|")
    ls = "✅" if gate.longshot_passed else "❌"
    fs = "✅" if gate.favorite_passed else "❌"
    lines.append(
        f"| Longshot [0%-10%) | {gate.longshot_count} | ≥{SAMPLE_GATE_MIN} | {ls} |"
    )
    lines.append(
        f"| Favorite [90%-100%] | {gate.favorite_count} | ≥{SAMPLE_GATE_MIN} | {fs} |"
    )
    lines.append("")

    if not gate.passed:
        lines.append(
            "**H1 NOT VIABLE YET.** Insufficient resolved contracts in target "
            "buckets. Collect more data before proceeding with FLB strategy."
        )
        lines.append("")

    # Decile table.
    lines.append("## FLB by Probability Decile")
    lines.append("")
    lines.append(
        "| Decile | Range | N | N_YES | Implied P | Actual Rate | "
        "FLB Gap | 95% CI | Side |"
    )
    lines.append(
        "|--------|-------|---|-------|-----------|-------------|"
        "---------|--------|------|"
    )

    for s in decile_stats:
        range_str = f"[{s.lower:.0%}-{s.upper:.0%})"
        if s.decile == 9:
            range_str = f"[{s.lower:.0%}-{s.upper:.0%}]"
        ci_str = f"[{s.wilson_lower:.1%}, {s.wilson_upper:.1%}]" if s.n > 0 else "—"
        gap_str = f"{s.flb_gap:+.1%}" if s.n > 0 else "—"
        side_display = {
            "buy_yes": "🟢 BUY YES",
            "buy_no": "🔴 BUY NO",
            "no_edge": "⚪ no edge",
        }.get(s.recommended_side, s.recommended_side)

        lines.append(
            f"| {s.decile} | {range_str} | {s.n} | {s.n_yes} | "
            f"{s.implied_prob:.1%} | {s.actual_rate:.1%} | "
            f"{gap_str} | {ci_str} | {side_display} |"
        )
    lines.append("")

    # Side semantics summary.
    lines.append("## Side Semantics (H1 FLB Contrarian)")
    lines.append("")
    lines.append("| Condition | Market Says | FLB Says | Action |")
    lines.append("|-----------|-------------|----------|--------|")
    lines.append(
        "| YES price < 10% (longshot) | Low prob event likely | "
        "Overpriced (actual rate < implied) | **BUY NO** |"
    )
    lines.append(
        "| YES price > 90% (favorite) | High prob event likely | "
        "Underpriced (actual rate > implied) | **BUY YES** |"
    )
    lines.append("")

    # Category breakdown.
    categories: dict[str, int] = {}
    for m in markets:
        categories[m.category] = categories.get(m.category, 0) + 1
    if categories:
        lines.append("## Market Categories")
        lines.append("")
        lines.append("| Category | Count |")
        lines.append("|----------|-------|")
        for cat, count in sorted(categories.items(), key=lambda x: -x[1]):
            lines.append(f"| {cat} | {count} |")
        lines.append("")

    # Volume statistics.
    volumes = [m.volume for m in markets]
    if volumes:
        lines.append("## Volume Statistics")
        lines.append("")
        lines.append(f"- **Min volume:** ${min(volumes):,.0f}")
        lines.append(f"- **Median volume:** ${statistics.median(volumes):,.0f}")
        lines.append(f"- **Max volume:** ${max(volumes):,.0f}")
        lines.append(f"- **Total volume:** ${sum(volumes):,.0f}")
        lines.append("")

    # Limitations.
    lines.append("## Limitations")
    lines.append("")
    lines.append(
        "1. **Entry price proxy:** Uses `lastTradePrice` (last trade before "
        "resolution), NOT a timestamped entry snapshot. For strategy P&L "
        "backtesting, we need price snapshots at a defined entry horizon."
    )
    lines.append(
        "2. **Data source:** Gamma API `closed=true` returns a small window "
        "of recently resolved markets. For ≥100 contracts per target bucket, "
        "Dune Analytics on-chain data or a historical warehouse is required."
    )
    lines.append(
        "3. **Feasibility only:** This is a bias-detection script, not a "
        "strategy backtest. It does not account for fees, slippage, or "
        "execution timing."
    )
    lines.append("")

    return "\n".join(lines)


def save_decile_csv(
    decile_stats: list[DecileStats],
    output_path: Path,
) -> None:
    """Save decile statistics to CSV for downstream analysis."""
    with output_path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "decile", "lower", "upper", "n", "n_yes",
            "implied_prob", "actual_rate", "flb_gap",
            "wilson_lower", "wilson_upper", "recommended_side",
        ])
        for s in decile_stats:
            writer.writerow([
                s.decile, s.lower, s.upper, s.n, s.n_yes,
                f"{s.implied_prob:.6f}", f"{s.actual_rate:.6f}",
                f"{s.flb_gap:.6f}", f"{s.wilson_lower:.6f}",
                f"{s.wilson_upper:.6f}", s.recommended_side,
            ])


# ── Main ─────────────────────────────────────────────────────────────────────


def main() -> int:
    parser = argparse.ArgumentParser(description="H1 FLB Data Feasibility Analysis")
    parser.add_argument(
        "--limit",
        type=int,
        default=500,
        help="Markets per API page (default: 500)",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=20,
        help="Maximum API pages to fetch (default: 20)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output path for the Markdown report (default: stdout)",
    )
    parser.add_argument(
        "--csv",
        type=Path,
        default=None,
        help="Output path for decile CSV data",
    )
    args = parser.parse_args()

    fetched_at = datetime.now(tz=UTC)

    # Step 1: Fetch resolved markets.
    print(f"Fetching resolved Polymarket markets (limit={args.limit}, max_pages={args.max_pages})...",
          file=sys.stderr)
    markets = fetch_resolved_markets(limit=args.limit, max_pages=args.max_pages)
    print(f"Fetched {len(markets)} resolved binary markets.", file=sys.stderr)

    if not markets:
        print("ERROR: No resolved markets found. Check API connectivity.", file=sys.stderr)
        return 2

    # Step 2: Compute FLB by decile.
    decile_stats = compute_decile_stats(markets)

    # Step 3: Check sample gate.
    gate = check_sample_gate(decile_stats)

    # Step 4: Generate report.
    report = generate_report(
        markets=markets,
        decile_stats=decile_stats,
        gate=gate,
        fetched_at=fetched_at,
    )

    if args.output:
        args.output.write_text(report)
        print(f"Report written to {args.output}", file=sys.stderr)
    else:
        print(report)

    # Step 5: Save CSV if requested.
    if args.csv:
        save_decile_csv(decile_stats, args.csv)
        print(f"CSV written to {args.csv}", file=sys.stderr)

    # Return exit code based on gate.
    return 0 if gate.passed else 1


if __name__ == "__main__":
    sys.exit(main())
