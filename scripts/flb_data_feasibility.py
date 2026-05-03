"""H1 FLB Data Feasibility Analysis.

Fetches resolved Polymarket markets from the Gamma API, measures
Favorite-Longshot Bias (FLB) by probability decile with Wilson score
confidence intervals, and evaluates H1 strategy viability.

Each binary market is decomposed into two contract-level observations
(YES at price ``p``, NO at price ``1-p``) so that the sample gate
reflects the full tradable contract set.

Usage:
    uv run python scripts/flb_data_feasibility.py [--limit N] [--output PATH]
    uv run python scripts/flb_data_feasibility.py \
        --source warehouse-csv --input exports/polymarket_resolved_binary.csv

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
WAREHOUSE_REQUIRED_COLUMNS = frozenset({
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
})


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
class ContractObservation:
    """A single tradable contract observation for FLB analysis.

    Each binary market produces two contract observations:
    - YES contract at price ``p`` (pays 1 if market resolves YES)
    - NO contract at price ``1-p`` (pays 1 if market resolves NO)

    Bucketing by contract price ensures the full opportunity set is counted
    in the sample gate (a market with YES at 0.08 also contributes a NO
    contract at 0.92, filling both extreme buckets).
    """

    contract_side: str  # "YES" or "NO"
    entry_price: float  # contract price at entry
    pays_out: bool  # True if this contract paid out at resolution
    market_id: str
    question: str
    volume: float
    category: str


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

        # A settled binary market has one side at exactly 1.0 (the winner)
        # and the other at 0.0 (the loser).  Polymarket's AMM leaves tiny
        # residuals (e.g. 0.999999/0.000001), so we allow a tight tolerance.
        #
        # CRITICAL: the previous 0.01 tolerance admitted closed-but-unsettled
        # markets (e.g. 0.995/0.005) whose prices reflect last-trade proximity
        # to resolution, NOT oracle-settled payouts.  Such prices leak
        # near-resolution information into the label set and bias FLB counts.
        #
        # LIMITATION: Gamma API has no explicit settlement/resolution flag.
        # This price-based heuristic is the best available proxy.  Markets
        # with prices in the 0.001–0.999 range (i.e. not clearly settled)
        # are conservatively rejected.
        _SETTLED_TOLERANCE = 0.001

        # Reject degenerate markets where both prices are zero (cleared data).
        if yes_price == 0.0 and no_price == 0.0:
            return None

        if abs(yes_price - 1.0) < _SETTLED_TOLERANCE:
            resolved_yes = True
        elif abs(no_price - 1.0) < _SETTLED_TOLERANCE:
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


# ── Historical Warehouse Loading ─────────────────────────────────────────────


def load_warehouse_markets(path: Path) -> list[ResolvedMarket]:
    """Load resolved binary markets from an explicit warehouse/Dune CSV export.

    The warehouse path is intentionally stricter than the Gamma fallback:
    settlement must come from an explicit final payout vector
    (``yes_payout,no_payout`` equal to ``1,0`` or ``0,1``).  Near-settled
    prices such as ``0.995,0.005`` are rejected because they are trade prices,
    not oracle settlement labels.
    """
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = set(reader.fieldnames or [])
        missing = WAREHOUSE_REQUIRED_COLUMNS - fieldnames
        if missing:
            missing_display = ", ".join(sorted(missing))
            raise ValueError(f"warehouse CSV missing required columns: {missing_display}")

        markets: list[ResolvedMarket] = []
        for row_number, row in enumerate(reader, start=2):
            markets.append(_parse_warehouse_row(row, row_number=row_number))

    return markets


def _parse_warehouse_row(
    row: dict[str, str | None],
    *,
    row_number: int,
) -> ResolvedMarket:
    """Parse one strict warehouse CSV row into a resolved binary market."""
    entry_yes_price = _required_float(row, "entry_yes_price", row_number=row_number)
    if entry_yes_price <= 0.0 or entry_yes_price >= 1.0:
        raise ValueError(
            f"warehouse row {row_number}: entry_yes_price must be between 0 and 1"
        )

    resolved_yes = _resolved_yes_from_exact_payout(row, row_number=row_number)

    entry_timestamp = _required_text(row, "entry_timestamp", row_number=row_number)
    resolved_at = _required_text(row, "resolved_at", row_number=row_number)
    _validate_iso_datetime(entry_timestamp, column="entry_timestamp", row_number=row_number)
    _validate_iso_datetime(resolved_at, column="resolved_at", row_number=row_number)

    return ResolvedMarket(
        market_id=_required_text(row, "market_id", row_number=row_number),
        question=_required_text(row, "question", row_number=row_number),
        yes_price=entry_yes_price,
        resolved_yes=resolved_yes,
        volume=_required_float(row, "volume", row_number=row_number),
        liquidity=_required_float(row, "liquidity", row_number=row_number),
        end_date=resolved_at,
        category=_required_text(row, "category", row_number=row_number),
    )


def _resolved_yes_from_exact_payout(
    row: dict[str, str | None],
    *,
    row_number: int,
) -> bool:
    """Return resolution from an exact final payout vector."""
    yes_payout = _required_float(row, "yes_payout", row_number=row_number)
    no_payout = _required_float(row, "no_payout", row_number=row_number)

    if yes_payout == 1.0 and no_payout == 0.0:
        return True
    if yes_payout == 0.0 and no_payout == 1.0:
        return False

    raise ValueError(
        f"warehouse row {row_number}: expected exact final payout vector "
        "(yes_payout,no_payout) of (1,0) or (0,1)"
    )


def _required_text(
    row: dict[str, str | None],
    column: str,
    *,
    row_number: int,
) -> str:
    value = row.get(column)
    if value is None or value.strip() == "":
        raise ValueError(f"warehouse row {row_number}: missing {column}")
    return value.strip()


def _required_float(
    row: dict[str, str | None],
    column: str,
    *,
    row_number: int,
) -> float:
    raw_value = _required_text(row, column, row_number=row_number)
    try:
        value = float(raw_value)
    except ValueError as exc:
        raise ValueError(
            f"warehouse row {row_number}: {column} must be a finite number"
        ) from exc
    if not math.isfinite(value):
        raise ValueError(
            f"warehouse row {row_number}: {column} must be a finite number"
        )
    return value


def _validate_iso_datetime(value: str, *, column: str, row_number: int) -> None:
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(
            f"warehouse row {row_number}: {column} must be ISO-8601"
        ) from exc


# ── Contract-Level Conversion ────────────────────────────────────────────────


def markets_to_contracts(markets: list[ResolvedMarket]) -> list[ContractObservation]:
    """Convert resolved markets to contract-level observations.

    Each binary market yields two observations:
    - YES contract at ``yes_price`` — pays out if ``resolved_yes``
    - NO contract at ``1 - yes_price`` — pays out if ``not resolved_yes``

    This doubles the observation count and ensures the sample gate reflects
    the full tradable contract set.  A market with YES at 0.08 (longshot)
    also contributes a NO contract at 0.92 (favorite), filling both extreme
    buckets.
    """
    contracts: list[ContractObservation] = []
    for m in markets:
        contracts.append(ContractObservation(
            contract_side="YES",
            entry_price=m.yes_price,
            pays_out=m.resolved_yes,
            market_id=m.market_id,
            question=m.question,
            volume=m.volume,
            category=m.category,
        ))
        contracts.append(ContractObservation(
            contract_side="NO",
            entry_price=1.0 - m.yes_price,
            pays_out=not m.resolved_yes,
            market_id=m.market_id,
            question=m.question,
            volume=m.volume,
            category=m.category,
        ))
    return contracts


# ── FLB Analysis ─────────────────────────────────────────────────────────────


def compute_decile_stats(
    contracts: list[ContractObservation],
) -> list[DecileStats]:
    """Compute FLB statistics for each probability decile (contract-level).

    Contract observations are bucketed by ``entry_price`` into deciles
    [0%-10%), [10%-20%), ..., [90%-100%]. For each decile we compute the
    mean implied probability, the actual payout rate, the FLB gap, and a
    Wilson score confidence interval.

    Each binary market contributes two observations (YES and NO contracts),
    so the observation count is 2× the market count.
    """
    # Sort contracts by entry_price for decile assignment.
    sorted_contracts = sorted(contracts, key=lambda c: c.entry_price)

    # Assign each contract to a decile.
    decile_buckets: dict[int, list[ContractObservation]] = {
        i: [] for i in range(10)
    }
    for contract in sorted_contracts:
        decile = _assign_decile(contract.entry_price)
        decile_buckets[decile].append(contract)

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

        n_yes = sum(1 for c in bucket if c.pays_out)
        implied_prob = sum(c.entry_price for c in bucket) / n
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
    contracts: list[ContractObservation],
    decile_stats: list[DecileStats],
    gate: SampleGateResult,
    fetched_at: datetime,
    source_label: str = "Gamma API closed markets",
) -> str:
    """Generate a Markdown report of the FLB feasibility analysis."""
    lines: list[str] = []
    lines.append("# H1 FLB Data Feasibility Report")
    lines.append("")
    lines.append(f"**Generated:** {fetched_at.isoformat()}")
    lines.append(f"**Data source:** {source_label}")
    lines.append(f"**Total resolved markets analyzed:** {len(markets)}")
    lines.append(f"**Total contract observations:** {len(contracts)} "
                 "(2 per binary market: YES + NO)")
    lines.append("")

    # Gate result.
    gate_emoji = "✅" if gate.passed else "❌"
    lines.append(f"## Sample Gate: {gate_emoji}")
    lines.append("")
    lines.append("| Bucket | Contract Count | Required | Status |")
    lines.append("|--------|---------------|----------|--------|")
    ls = "✅" if gate.longshot_passed else "❌"
    fs = "✅" if gate.favorite_passed else "❌"
    lines.append(
        f"| Longshot [0%-10%) | {gate.longshot_count} | ≥{SAMPLE_GATE_MIN} | {ls} |"
    )
    lines.append(
        f"| Favorite [90%-100%] | {gate.favorite_count} | ≥{SAMPLE_GATE_MIN} | {fs} |"
    )
    lines.append("")

    if gate.passed:
        lines.append(
            "**H1 DATA VIABLE.** Extreme buckets meet the sample gate. "
            "Proceed to the next backtest slice with fees, slippage, and "
            "timestamped entry rules."
        )
        lines.append("")
    else:
        lines.append(
            "**H1 NOT VIABLE YET.** Insufficient resolved contracts in target "
            "buckets. Next data-source gap: collect a broader Dune or "
            "warehouse export before proceeding with FLB strategy."
        )
        lines.append("")

    # Decile table.
    lines.append("## FLB by Probability Decile (Contract-Level)")
    lines.append("")
    lines.append(
        "| Decile | Range | N | N_Payout | Implied P | Actual Rate | "
        "FLB Gap | 95% CI | Side |"
    )
    lines.append(
        "|--------|-------|---|----------|-----------|-------------|"
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
    lines.append(
        "*N = contract observations (2 per binary market).  "
        "N_Payout = contracts that paid out at resolution.*"
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
        "1. **Contract-level analysis:** Each binary market contributes two "
        "contract observations (YES at `p`, NO at `1-p`). The sample gate "
        "counts contracts, not markets. FLB gap is measured per contract "
        "side, so the same market-level mispricing appears in both the "
        "longshot and favorite buckets from opposite sides."
    )
    lines.append(
        "2. **Entry price proxy:** Uses `lastTradePrice` (last trade before "
        "resolution), NOT a timestamped entry snapshot. For strategy P&L "
        "backtesting, we need price snapshots at a defined entry horizon."
    )
    lines.append(
        "3. **Data source coverage:** Gamma API `closed=true` is only a "
        "small-window fallback. Robust H1 viability requires a Dune Analytics "
        "or historical warehouse export with explicit final payout vectors."
    )
    lines.append(
        "4. **Feasibility only:** This is a bias-detection script, not a "
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
        "--source",
        choices=["gamma", "warehouse-csv"],
        default="gamma",
        help="Historical data source (default: gamma)",
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=None,
        help="Input CSV path when --source=warehouse-csv",
    )
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

    # Step 1: Fetch/load resolved markets.
    if args.source == "gamma":
        source_label = "Gamma API closed markets"
        print(
            "Fetching resolved Polymarket markets "
            f"(limit={args.limit}, max_pages={args.max_pages})...",
            file=sys.stderr,
        )
        markets = fetch_resolved_markets(limit=args.limit, max_pages=args.max_pages)
        print(f"Fetched {len(markets)} resolved binary markets.", file=sys.stderr)
    else:
        if args.input is None:
            parser.error("--input is required when --source=warehouse-csv")
        source_label = f"warehouse CSV: {args.input}"
        print(f"Loading resolved binary markets from {args.input}...", file=sys.stderr)
        markets = load_warehouse_markets(args.input)
        print(f"Loaded {len(markets)} resolved binary markets.", file=sys.stderr)

    if not markets:
        print("ERROR: No resolved markets found.", file=sys.stderr)
        return 2

    # Step 2: Convert to contract-level observations.
    contracts = markets_to_contracts(markets)
    print(f"Generated {len(contracts)} contract observations "
          f"({len(markets)} markets × 2).", file=sys.stderr)

    # Step 3: Compute FLB by decile (contract-level).
    decile_stats = compute_decile_stats(contracts)

    # Step 4: Check sample gate (contract-level counts).
    gate = check_sample_gate(decile_stats)

    # Step 5: Generate report.
    report = generate_report(
        markets=markets,
        contracts=contracts,
        decile_stats=decile_stats,
        gate=gate,
        fetched_at=fetched_at,
        source_label=source_label,
    )

    if args.output:
        args.output.write_text(report)
        print(f"Report written to {args.output}", file=sys.stderr)
    else:
        print(report)

    # Step 6: Save CSV if requested.
    if args.csv:
        save_decile_csv(decile_stats, args.csv)
        print(f"CSV written to {args.csv}", file=sys.stderr)

    # Return exit code based on gate.
    return 0 if gate.passed else 1


if __name__ == "__main__":
    sys.exit(main())
