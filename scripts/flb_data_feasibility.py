"""H1 FLB Data Feasibility Analysis.

Fetches resolved Polymarket markets from the Gamma API, measures
Favorite-Longshot Bias (FLB) by probability decile with Wilson score
confidence intervals, and evaluates H1 strategy viability.

Each binary market is decomposed into two contract-level observations
(YES at price ``p``, NO at price ``1-p``) for decile diagnostics.  The
launch sample gate is stricter: it matches the runtime calibration artifact
and counts original YES-price markets in the two H1 signal buckets.

Usage:
    uv run python scripts/flb_data_feasibility.py [--limit N] [--output PATH]
    uv run python scripts/export_flb_warehouse_from_dune.py \
        --output "$PMS_SECURE_DIR/polymarket_resolved_binary.csv"
    uv run python scripts/flb_data_feasibility.py \
        --source warehouse-csv \
        --input "$PMS_SECURE_DIR/polymarket_resolved_binary.csv"
    uv run python scripts/flb_data_feasibility.py \
        --source warehouse-csv \
        --input "$PMS_SECURE_DIR/polymarket_resolved_binary.csv" \
        --calibration-csv "$PMS_SECURE_DIR/flb-calibration.csv" \
        --calibration-source-label warehouse-flb-v1 \
        --calibration-provenance-json \
          "$PMS_SECURE_DIR/flb-calibration.csv.provenance.json"

Exit codes:
    0 — H1 viable (sample gate passed)
    1 — H1 not viable (insufficient data in runtime signal buckets)
    2 — operator/input error (missing input, malformed warehouse CSV,
        unsafe artifact path, network/IO failure, or no resolved markets)

Limitations:
    - **Entry price proxy:** Uses ``lastTradePrice`` from the Gamma API,
      which is the last trade before resolution — NOT a timestamped entry
      snapshot. This means the "implied probability" is measured at or very
      near resolution time, not at a hypothetical entry point. For a real
      strategy P&L backtest, we would need timestamped price snapshots at
      a defined entry horizon (e.g., 24h before resolution).
    - **Data source:** The Gamma API ``closed=true`` endpoint is paginated
      and may cap page size below the requested ``--limit``. It is still a
      recent public window, so statistically robust FLB measurement
      (≥100 contracts per target bucket) needs Dune Analytics on-chain data
      or a historical warehouse with full Polymarket resolution history.
    - **This script is a feasibility check**, not a strategy backtest.
      It answers: "Does Polymarket data show measurable FLB?" — not
      "Would FLB trading have been profitable after fees and slippage?"
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import math
import os
import statistics
import stat
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import httpx

from scripts.artifact_path_safety import require_path_outside_working_tree
from pms.strategies.flb.artifacts import (
    file_sha256_no_follow,
    flb_calibration_provenance_path,
    flb_calibration_provenance_payload,
)
from pms.strategies.flb.source import require_flb_calibration_source_label

GAMMA_API_BASE = "https://gamma-api.polymarket.com"
GAMMA_CLOSED_MARKET_ORDER = "closedTime"
GAMMA_CLOSED_MARKET_ASCENDING = False
SAMPLE_GATE_MIN = 100  # minimum resolved markets per runtime H1 signal bucket
DECILE_BOUNDARIES = [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
TARGET_BUCKETS = [0, 9]  # decile 0 = [0%, 10%), decile 9 = [90%, 100%]
WILSON_Z = 1.96  # 95% confidence
LONGSHOT_SIGNAL_NAME = "longshot_yes_overpriced_buy_no"
FAVORITE_SIGNAL_NAME = "favorite_yes_underpriced_buy_yes"
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

    Bucketing by contract price exposes the full opportunity set for decile
    diagnostics (a market with YES at 0.08 also contributes a NO contract at
    0.92), but the launch sample gate is signal-specific and uses original
    YES-price market buckets.
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


@dataclass(frozen=True, slots=True)
class FlbCalibrationArtifactRow:
    """Runtime FLB calibration row generated from strict warehouse history."""

    signal_name: str
    probability_estimate: float
    sample_count: int
    source_label: str

    def __post_init__(self) -> None:
        if self.signal_name not in {LONGSHOT_SIGNAL_NAME, FAVORITE_SIGNAL_NAME}:
            msg = f"unsupported FLB calibration signal_name: {self.signal_name}"
            raise ValueError(msg)
        if self.probability_estimate <= 0.0 or self.probability_estimate >= 1.0:
            msg = "probability_estimate must satisfy 0.0 < value < 1.0"
            raise ValueError(msg)
        if not math.isfinite(self.probability_estimate):
            msg = "probability_estimate must be finite"
            raise ValueError(msg)
        if self.sample_count <= 0:
            msg = "sample_count must be positive"
            raise ValueError(msg)
        require_flb_calibration_source_label(self.source_label)


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
                    "order": GAMMA_CLOSED_MARKET_ORDER,
                    "ascending": _bool_param(GAMMA_CLOSED_MARKET_ASCENDING),
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

            # Gamma may cap responses below the requested limit. Advance by
            # the actual row count and let an empty page or max_pages stop us.
            offset += len(payload)

    return markets


def _bool_param(value: bool) -> str:
    return "true" if value else "false"


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
            # Not clearly resolved — skip. This also excludes 50/50
            # resolutions (both prices ≈ 0.5), refunds, and cancelled
            # markets where no single side is the unambiguous winner.
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


def load_warehouse_markets(path: Path) -> tuple[list[ResolvedMarket], int]:
    """Load resolved binary markets from an explicit warehouse/Dune CSV export.

    The warehouse path is intentionally stricter than the Gamma fallback:
    settlement must come from an explicit final payout vector
    (``yes_payout,no_payout`` equal to ``1,0`` or ``0,1``).  Near-settled
    prices such as ``0.995,0.005`` are rejected because they are trade prices,
    not oracle settlement labels.

    Markets with 50/50 resolutions (both payouts 0.5) are silently skipped
    because the binary outcome model cannot represent partial payouts.

    Returns:
        (markets, skipped_50_50_count) — parsed markets and count of skipped
        50/50 resolution rows.
    """
    with io.StringIO(
        _read_text_no_follow(path, label="warehouse CSV input path"),
        newline="",
    ) as f:
        reader = csv.DictReader(f)
        _require_unique_csv_fieldnames(reader.fieldnames)
        fieldnames = set(reader.fieldnames or [])
        missing = WAREHOUSE_REQUIRED_COLUMNS - fieldnames
        if missing:
            missing_display = ", ".join(sorted(missing))
            raise ValueError(f"warehouse CSV missing required columns: {missing_display}")

        markets: list[ResolvedMarket] = []
        market_ids: set[str] = set()
        skipped_50_50 = 0
        for row_number, row in enumerate(reader, start=2):
            market = _parse_warehouse_row(row, row_number=row_number)
            if market is None:
                skipped_50_50 += 1
                continue
            if market.market_id in market_ids:
                raise ValueError(
                    f"warehouse row {row_number}: duplicate market_id "
                    f"{market.market_id!r}; expected one row per resolved binary market"
                )
            market_ids.add(market.market_id)
            markets.append(market)

    return markets, skipped_50_50


def _parse_warehouse_row(
    row: dict[str, str | None],
    *,
    row_number: int,
) -> ResolvedMarket | None:
    """Parse one strict warehouse CSV row into a resolved binary market.

    Returns None for rows that should be skipped without error (e.g. 50/50
    resolutions where neither side is the unambiguous winner).
    """
    entry_yes_price = _required_float(row, "entry_yes_price", row_number=row_number)
    if entry_yes_price <= 0.0 or entry_yes_price >= 1.0:
        raise ValueError(
            f"warehouse row {row_number}: entry_yes_price must be between 0 and 1"
        )

    resolved_yes = _resolved_yes_from_exact_payout(row, row_number=row_number)
    if resolved_yes is None:
        return None  # 50/50 resolution — skip without error

    entry_timestamp = _required_text(row, "entry_timestamp", row_number=row_number)
    resolved_at = _required_text(row, "resolved_at", row_number=row_number)
    entry_dt = _parse_iso_datetime(
        entry_timestamp,
        column="entry_timestamp",
        row_number=row_number,
    )
    resolved_dt = _parse_iso_datetime(
        resolved_at,
        column="resolved_at",
        row_number=row_number,
    )
    if entry_dt >= resolved_dt:
        raise ValueError(
            f"warehouse row {row_number}: entry_timestamp must be before resolved_at"
        )

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
) -> bool | None:
    """Return resolution from an exact final payout vector.

    Returns:
        True — market resolved YES (payout = 1,0).
        False — market resolved NO (payout = 0,1).
        None — non-standard resolution (50/50 tie, refund, cancelled).
          These markets must be excluded from FLB analysis because the
          binary outcome model cannot represent partial payouts.  Including
          them would bias the Brier score and FLB gap toward zero.
    """
    yes_payout = _required_float(row, "yes_payout", row_number=row_number)
    no_payout = _required_float(row, "no_payout", row_number=row_number)

    if yes_payout == 1.0 and no_payout == 0.0:
        return True
    if yes_payout == 0.0 and no_payout == 1.0:
        return False

    # 50/50 resolution (both payouts 0.5), refunds, or cancelled markets.
    # These have no clear "correct" side and must be excluded.
    if yes_payout == 0.5 and no_payout == 0.5:
        return None

    raise ValueError(
        f"warehouse row {row_number}: expected exact final payout vector "
        "(yes_payout,no_payout) of (1,0), (0,1), or (0.5,0.5) for 50/50 resolutions"
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


def _parse_iso_datetime(value: str, *, column: str, row_number: int) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(
            f"warehouse row {row_number}: {column} must be ISO-8601"
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(
            f"warehouse row {row_number}: {column} must include timezone"
        )
    return parsed.astimezone(UTC)


# ── Contract-Level Conversion ────────────────────────────────────────────────


def markets_to_contracts(markets: list[ResolvedMarket]) -> list[ContractObservation]:
    """Convert resolved markets to contract-level observations.

    Each binary market yields two observations:
    - YES contract at ``yes_price`` — pays out if ``resolved_yes``
    - NO contract at ``1 - yes_price`` — pays out if ``not resolved_yes``

    This doubles the observation count so the decile table reflects the full
    tradable contract set.  A market with YES at 0.08 (longshot) also
    contributes a NO contract at 0.92 (favorite), filling both extreme
    diagnostic buckets.
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

    longshot_count: int  # original YES-price markets with YES < 10%
    favorite_count: int  # original YES-price markets with YES > 90%
    longshot_passed: bool
    favorite_passed: bool
    passed: bool  # both buckets meet minimum


def _split_flb_signal_markets(
    markets: Sequence[ResolvedMarket],
) -> tuple[list[ResolvedMarket], list[ResolvedMarket]]:
    """Split markets into the runtime H1 FLB calibration signal buckets."""
    longshot_markets = [
        market
        for market in markets
        if market.yes_price < DECILE_BOUNDARIES[1]
    ]
    favorite_markets = [
        market
        for market in markets
        if market.yes_price > DECILE_BOUNDARIES[9]
    ]
    return longshot_markets, favorite_markets


def check_sample_gate(
    markets: Sequence[ResolvedMarket],
    *,
    min_sample_count: int = SAMPLE_GATE_MIN,
) -> SampleGateResult:
    """Check whether enough data exists in the runtime FLB signal buckets.

    The sample gate requires enough resolved markets in both original
    YES-price buckets used by runtime calibration:

    - longshot signal: YES < 10%, BUY NO
    - favorite signal: YES > 90%, BUY YES
    """
    if min_sample_count <= 0:
        msg = "min_sample_count must be positive"
        raise ValueError(msg)

    longshot_markets, favorite_markets = _split_flb_signal_markets(markets)
    longshot_count = len(longshot_markets)
    favorite_count = len(favorite_markets)

    longshot_passed = longshot_count >= min_sample_count
    favorite_passed = favorite_count >= min_sample_count

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
    lines.append("| Signal | Market Count | Required | Status |")
    lines.append("|--------|--------------|----------|--------|")
    ls = "✅" if gate.longshot_passed else "❌"
    fs = "✅" if gate.favorite_passed else "❌"
    lines.append(
        f"| Longshot: {LONGSHOT_SIGNAL_NAME} (YES < 10%, BUY NO) | "
        f"{gate.longshot_count} | ≥{SAMPLE_GATE_MIN} | {ls} |"
    )
    lines.append(
        f"| Favorite: {FAVORITE_SIGNAL_NAME} (YES > 90%, BUY YES) | "
        f"{gate.favorite_count} | ≥{SAMPLE_GATE_MIN} | {fs} |"
    )
    lines.append("")

    if gate.passed:
        lines.append(
            "**H1 DATA VIABLE.** Runtime FLB calibration signal buckets meet "
            "the sample gate. Proceed to the next backtest slice with fees, "
            "slippage, and timestamped entry rules."
        )
        lines.append("")
    else:
        lines.append(
            "**H1 NOT VIABLE YET.** Insufficient resolved markets in runtime "
            "calibration signal buckets. Next data-source gap: collect a "
            "broader Dune or warehouse export before proceeding with FLB "
            "strategy."
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
        "contract observations (YES at `p`, NO at `1-p`) for the decile "
        "diagnostic table. The launch sample gate counts original YES-price "
        "markets in the two runtime FLB calibration signal buckets, not "
        "synthetic opposite-side contracts."
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
    _prepare_private_output_parent(
        output_path,
        label="FLB decile CSV output parent",
    )
    output = io.StringIO()
    writer = csv.writer(output)
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
    _write_text_no_follow(
        output_path,
        output.getvalue(),
        label="FLB decile CSV output path",
    )


def build_flb_calibration_rows(
    markets: list[ResolvedMarket],
    *,
    source_label: str,
    min_sample_count: int = SAMPLE_GATE_MIN,
    smoothing_alpha: float = 1.0,
    smoothing_beta: float = 1.0,
) -> list[FlbCalibrationArtifactRow]:
    """Build runtime FLB signal probabilities from strict warehouse history.

    Longshot and favorite signals are estimated separately from original YES
    price buckets, not from the mixed contract-level decile table:

    - ``longshot_yes_overpriced_buy_no``: markets with YES price < 10%, using
      the smoothed probability that the NO contract pays out.
    - ``favorite_yes_underpriced_buy_yes``: markets with YES price > 90%, using
      the smoothed probability that the YES contract pays out.
    """
    if min_sample_count <= 0:
        msg = "min_sample_count must be positive"
        raise ValueError(msg)
    if smoothing_alpha <= 0.0 or smoothing_beta <= 0.0:
        msg = "smoothing_alpha and smoothing_beta must be positive"
        raise ValueError(msg)

    longshot_markets = [
        market
        for market in markets
        if market.yes_price < DECILE_BOUNDARIES[1]
    ]
    favorite_markets = [
        market
        for market in markets
        if market.yes_price > DECILE_BOUNDARIES[9]
    ]

    longshot_successes = sum(1 for market in longshot_markets if not market.resolved_yes)
    favorite_successes = sum(1 for market in favorite_markets if market.resolved_yes)
    return [
        _flb_calibration_row(
            signal_name=LONGSHOT_SIGNAL_NAME,
            successes=longshot_successes,
            sample_count=len(longshot_markets),
            source_label=source_label,
            min_sample_count=min_sample_count,
            smoothing_alpha=smoothing_alpha,
            smoothing_beta=smoothing_beta,
        ),
        _flb_calibration_row(
            signal_name=FAVORITE_SIGNAL_NAME,
            successes=favorite_successes,
            sample_count=len(favorite_markets),
            source_label=source_label,
            min_sample_count=min_sample_count,
            smoothing_alpha=smoothing_alpha,
            smoothing_beta=smoothing_beta,
        ),
    ]


def save_flb_calibration_csv(
    rows: list[FlbCalibrationArtifactRow],
    output_path: Path,
) -> None:
    """Save the runtime FLB calibration artifact CSV."""
    _prepare_private_output_parent(
        output_path,
        label="FLB calibration CSV output parent",
    )
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "signal_name",
        "probability_estimate",
        "sample_count",
        "source_label",
    ])
    for row in rows:
        writer.writerow([
            row.signal_name,
            f"{row.probability_estimate:.12f}",
            row.sample_count,
            row.source_label,
        ])
    _write_text_no_follow(
        output_path,
        output.getvalue(),
        label="FLB calibration CSV output path",
    )


def save_flb_calibration_provenance_json(
    rows: list[FlbCalibrationArtifactRow],
    *,
    warehouse_csv_path: Path,
    warehouse_market_count: int,
    calibration_csv_path: Path,
    output_path: Path,
    generated_at: datetime,
) -> None:
    """Save the runtime FLB calibration provenance sidecar."""
    source_labels = {row.source_label for row in rows}
    if len(source_labels) != 1:
        msg = "FLB calibration provenance requires one calibration source label"
        raise ValueError(msg)
    by_signal = {row.signal_name: row for row in rows}
    try:
        longshot_count = by_signal[LONGSHOT_SIGNAL_NAME].sample_count
        favorite_count = by_signal[FAVORITE_SIGNAL_NAME].sample_count
    except KeyError as exc:
        msg = f"FLB calibration provenance missing signal row: {exc.args[0]}"
        raise ValueError(msg) from exc

    payload = flb_calibration_provenance_payload(
        generated_at=generated_at,
        warehouse_csv_sha256=file_sha256_no_follow(
            warehouse_csv_path,
            label="FLB warehouse CSV input path",
        ),
        warehouse_market_count=warehouse_market_count,
        warehouse_longshot_count=longshot_count,
        warehouse_favorite_count=favorite_count,
        calibration_csv_sha256=file_sha256_no_follow(
            calibration_csv_path,
            label="FLB calibration CSV output path",
        ),
        calibration_source_label=next(iter(source_labels)),
    )
    _prepare_private_output_parent(
        output_path,
        label="FLB calibration provenance JSON output parent",
    )
    _write_text_no_follow(
        output_path,
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        label="FLB calibration provenance JSON output path",
    )


def _write_text_no_follow(path: Path, content: str, *, label: str) -> None:
    _require_regular_file_or_absent(path, label=label)
    fd, temp_path = _open_output_temp_file(path, label=label)
    published = False
    try:
        os.fchmod(fd, 0o600)
        os.ftruncate(fd, 0)
        os.lseek(fd, 0, os.SEEK_SET)
        with os.fdopen(fd, "w", encoding="utf-8") as file:
            fd = -1
            file.write(content)
            file.flush()
            os.fsync(file.fileno())
        _require_regular_file_or_absent(path, label=label)
        os.replace(temp_path, path)
        published = True
        _fsync_parent_directory(path)
    finally:
        if fd >= 0:
            os.close(fd)
        if not published:
            _unlink_regular_single_link_file_if_present(temp_path)


def _require_unique_csv_fieldnames(fieldnames: Sequence[str] | None) -> None:
    if fieldnames is None:
        return
    seen: set[str] = set()
    for fieldname in fieldnames:
        if fieldname in seen:
            msg = f"duplicate CSV column: {fieldname}"
            raise ValueError(msg)
        seen.add(fieldname)


def _open_output_temp_file(path: Path, *, label: str) -> tuple[int, Path]:
    _require_regular_file_or_absent(path, label=label)
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    for _ in range(16):
        temp_path = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
        try:
            fd = os.open(temp_path, flags, 0o600)
        except FileExistsError:
            continue
        try:
            _require_open_regular_single_link_file(fd, temp_path, label=label)
            os.fchmod(fd, 0o600)
        except BaseException:
            os.close(fd)
            _unlink_regular_single_link_file_if_present(temp_path)
            raise
        return fd, temp_path
    raise FileExistsError(f"could not create temporary {label} for {path}")


def _unlink_regular_single_link_file_if_present(path: Path) -> None:
    try:
        path_stat = path.lstat()
    except FileNotFoundError:
        return
    if not stat.S_ISREG(path_stat.st_mode) or path_stat.st_nlink != 1:
        return
    path.unlink()


def _fsync_parent_directory(path: Path) -> None:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    try:
        fd = os.open(path.parent, flags)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        return
    finally:
        os.close(fd)


def _prepare_private_output_parent(path: Path, *, label: str) -> None:
    require_path_outside_working_tree(path, label=label, error_type=OSError)
    parent = path.parent
    try:
        mode = parent.lstat().st_mode
    except FileNotFoundError:
        parent.mkdir(parents=True, mode=0o700, exist_ok=False)
        os.chmod(parent, 0o700)
        return
    if not stat.S_ISDIR(mode):
        raise OSError(f"{label} is not a directory: {parent}")
    permissions = stat.S_IMODE(mode)
    if permissions & 0o077:
        raise OSError(
            f"{label} {parent} is too permissive; "
            f"run `chmod 700 {parent}`."
        )
    if not permissions & stat.S_IWUSR:
        raise OSError(
            f"{label} {parent} is not owner-writable; "
            f"run `chmod 700 {parent}`."
        )


def _read_text_no_follow(path: Path, *, label: str) -> str:
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    fd = -1
    try:
        fd = os.open(path, flags, 0o777)
        path_stat = os.fstat(fd)
        if not stat.S_ISREG(path_stat.st_mode):
            raise OSError(f"{label} cannot be read safely: {path}")
        if path_stat.st_nlink != 1:
            raise OSError(f"{label} cannot be read safely: {path}")
        with os.fdopen(fd, "r", encoding="utf-8") as file:
            fd = -1
            return file.read()
    except OSError as exc:
        msg = f"{label} cannot be read safely: {path}"
        raise ValueError(msg) from exc
    finally:
        if fd >= 0:
            os.close(fd)


def _require_open_regular_single_link_file(fd: int, path: Path, *, label: str) -> None:
    path_stat = os.fstat(fd)
    if not stat.S_ISREG(path_stat.st_mode):
        raise OSError(f"{label} is not a regular file: {path}")
    if path_stat.st_nlink != 1:
        raise OSError(f"{label} is not a single-link file: {path}")


def _require_regular_file_or_absent(path: Path, *, label: str) -> None:
    try:
        path_stat = path.lstat()
    except FileNotFoundError:
        return
    if not stat.S_ISREG(path_stat.st_mode):
        raise OSError(f"{label} is not a regular file: {path}")
    if path_stat.st_nlink != 1:
        raise OSError(f"{label} is not a single-link file: {path}")


def _flb_calibration_row(
    *,
    signal_name: str,
    successes: int,
    sample_count: int,
    source_label: str,
    min_sample_count: int,
    smoothing_alpha: float,
    smoothing_beta: float,
) -> FlbCalibrationArtifactRow:
    if sample_count < min_sample_count:
        msg = (
            f"insufficient FLB calibration samples for {signal_name}: "
            f"{sample_count} < {min_sample_count}"
        )
        raise ValueError(msg)
    probability = (
        (successes + smoothing_alpha)
        / (sample_count + smoothing_alpha + smoothing_beta)
    )
    return FlbCalibrationArtifactRow(
        signal_name=signal_name,
        probability_estimate=probability,
        sample_count=sample_count,
        source_label=source_label,
    )


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
    parser.add_argument(
        "--calibration-csv",
        type=Path,
        default=None,
        help=(
            "Output runtime FLB calibration CSV; requires --source=warehouse-csv"
        ),
    )
    parser.add_argument(
        "--calibration-source-label",
        default=None,
        help=(
            "Auditable source slug written into --calibration-csv, for example "
            "warehouse-flb-v1. Required with --calibration-csv."
        ),
    )
    parser.add_argument(
        "--calibration-provenance-json",
        type=Path,
        default=None,
        help=(
            "Output runtime FLB calibration provenance sidecar JSON; use "
            "flb-calibration.csv.provenance.json for launch artifacts."
        ),
    )
    args = parser.parse_args()
    if args.calibration_csv is not None and args.source != "warehouse-csv":
        parser.error("--calibration-csv requires --source=warehouse-csv")
    if args.calibration_source_label is not None and args.calibration_csv is None:
        parser.error("--calibration-source-label requires --calibration-csv")
    if args.calibration_provenance_json is not None and args.calibration_csv is None:
        parser.error("--calibration-provenance-json requires --calibration-csv")
    if args.calibration_provenance_json is not None and args.calibration_csv is not None:
        expected_provenance_path = flb_calibration_provenance_path(
            args.calibration_csv
        )
        if not _path_identities_match(
            args.calibration_provenance_json,
            expected_provenance_path,
        ):
            parser.error(
                "--calibration-provenance-json must be the sidecar next to "
                f"--calibration-csv: {expected_provenance_path}"
            )
    if args.calibration_csv is not None and args.calibration_source_label is None:
        parser.error("--calibration-source-label is required with --calibration-csv")
    if args.calibration_source_label is not None:
        try:
            require_flb_calibration_source_label(args.calibration_source_label)
        except ValueError as exc:
            parser.error(str(exc))
    if args.source == "warehouse-csv" and args.input is None:
        parser.error("--input is required when --source=warehouse-csv")
    _require_distinct_cli_artifact_paths(
        parser,
        input_paths=[
            ("warehouse CSV input path", args.input),
        ],
        output_paths=[
            ("FLB report output path", args.output),
            ("FLB decile CSV output path", args.csv),
            ("FLB calibration CSV output path", args.calibration_csv),
            (
                "FLB calibration provenance JSON output path",
                args.calibration_provenance_json,
            ),
        ],
    )

    fetched_at = datetime.now(tz=UTC)

    # Step 1: Fetch/load resolved markets.
    if args.source == "gamma":
        source_label = "Gamma API closed markets"
        print(
            "Fetching resolved Polymarket markets "
            f"(limit={args.limit}, max_pages={args.max_pages})...",
            file=sys.stderr,
        )
        try:
            markets = fetch_resolved_markets(limit=args.limit, max_pages=args.max_pages)
        except (OSError, ValueError) as exc:
            print(f"ERROR: {exc}", file=sys.stderr)
            return 2
        print(f"Fetched {len(markets)} resolved binary markets.", file=sys.stderr)
    else:
        source_label = f"warehouse CSV: {args.input}"
        print(f"Loading resolved binary markets from {args.input}...", file=sys.stderr)
        try:
            markets, skipped_50_50 = load_warehouse_markets(args.input)
        except (OSError, ValueError) as exc:
            print(f"ERROR: {exc}", file=sys.stderr)
            return 2
        print(f"Loaded {len(markets)} resolved binary markets.", file=sys.stderr)
        if skipped_50_50:
            print(
                f"Skipped {skipped_50_50} market(s) with 50/50 resolution "
                "(no unambiguous winner — excluded from FLB analysis).",
                file=sys.stderr,
            )

    if not markets:
        print("ERROR: No resolved markets found.", file=sys.stderr)
        return 2

    # Step 2: Convert to contract-level observations.
    contracts = markets_to_contracts(markets)
    print(f"Generated {len(contracts)} contract observations "
          f"({len(markets)} markets × 2).", file=sys.stderr)

    # Step 3: Compute FLB by decile (contract-level).
    decile_stats = compute_decile_stats(contracts)

    # Step 4: Check launch sample gate (runtime signal-specific market counts).
    gate = check_sample_gate(markets)

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
        try:
            _prepare_private_output_parent(
                args.output,
                label="FLB report output parent",
            )
            _write_text_no_follow(
                args.output,
                report,
                label="FLB report output path",
            )
        except (OSError, ValueError) as exc:
            print(f"ERROR: {exc}", file=sys.stderr)
            return 2
        print(f"Report written to {args.output}", file=sys.stderr)
    else:
        print(report)

    # Step 6: Save CSV if requested.
    if args.csv:
        try:
            save_decile_csv(decile_stats, args.csv)
        except (OSError, ValueError) as exc:
            print(f"ERROR: {exc}", file=sys.stderr)
            return 2
        print(f"CSV written to {args.csv}", file=sys.stderr)

    if args.calibration_csv:
        if not gate.passed:
            print(_sample_gate_failure_message(gate), file=sys.stderr)
            return 1
        try:
            calibration_rows = build_flb_calibration_rows(
                markets,
                source_label=args.calibration_source_label,
            )
            save_flb_calibration_csv(calibration_rows, args.calibration_csv)
            if args.calibration_provenance_json is not None:
                if args.input is None:
                    msg = "warehouse CSV input path is required for calibration provenance"
                    raise ValueError(msg)
                save_flb_calibration_provenance_json(
                    calibration_rows,
                    warehouse_csv_path=args.input,
                    warehouse_market_count=len(markets),
                    calibration_csv_path=args.calibration_csv,
                    output_path=args.calibration_provenance_json,
                    generated_at=fetched_at,
                )
        except (OSError, ValueError) as exc:
            print(f"ERROR: {exc}", file=sys.stderr)
            return 2
        print(f"FLB calibration CSV written to {args.calibration_csv}", file=sys.stderr)
        if args.calibration_provenance_json is not None:
            print(
                "FLB calibration provenance JSON written to "
                f"{args.calibration_provenance_json}",
                file=sys.stderr,
            )

    # Return exit code based on gate.
    return 0 if gate.passed else 1


def _sample_gate_failure_message(gate: SampleGateResult) -> str:
    failures: list[str] = []
    if not gate.longshot_passed:
        failures.append(
            f"{LONGSHOT_SIGNAL_NAME} {gate.longshot_count} < {SAMPLE_GATE_MIN}"
        )
    if not gate.favorite_passed:
        failures.append(
            f"{FAVORITE_SIGNAL_NAME} {gate.favorite_count} < {SAMPLE_GATE_MIN}"
        )
    return "insufficient FLB calibration samples: " + "; ".join(failures)


def _require_distinct_cli_artifact_paths(
    parser: argparse.ArgumentParser,
    *,
    input_paths: list[tuple[str, Path | None]],
    output_paths: list[tuple[str, Path | None]],
) -> None:
    concrete_inputs = [
        (label, path)
        for label, path in input_paths
        if path is not None
    ]
    concrete_outputs = [
        (label, path)
        for label, path in output_paths
        if path is not None
    ]

    for output_label, output_path in concrete_outputs:
        output_identities = _path_identities(output_path)
        for input_label, input_path in concrete_inputs:
            if not _path_identities_overlap(
                output_identities,
                _path_identities(input_path),
            ):
                continue
            parser.error(
                f"{output_label} must be distinct from "
                f"{input_label}: {output_path}"
            )

    for index, (left_label, left_path) in enumerate(concrete_outputs):
        left_identities = _path_identities(left_path)
        for right_label, right_path in concrete_outputs[index + 1 :]:
            if not _path_identities_overlap(
                left_identities,
                _path_identities(right_path),
            ):
                continue
            parser.error(
                f"{left_label} must be distinct from "
                f"{right_label}: {left_path}"
            )


def _path_identities(path: Path) -> frozenset[Path]:
    expanded = path.expanduser()
    if not expanded.is_absolute():
        expanded = Path.cwd() / expanded
    return frozenset(
        (
            Path(os.path.abspath(expanded)),
            expanded.resolve(strict=False),
        )
    )


def _path_identities_match(left: Path, right: Path) -> bool:
    return bool(_path_identities(left) & _path_identities(right))


def _path_identities_overlap(left: frozenset[Path], right: frozenset[Path]) -> bool:
    return any(
        _paths_overlap(left_path, right_path)
        for left_path in left
        for right_path in right
    )


def _paths_overlap(left: Path, right: Path) -> bool:
    if left == right:
        return True
    try:
        right.relative_to(left)
    except ValueError:
        pass
    else:
        return True
    try:
        left.relative_to(right)
    except ValueError:
        return False
    return True


if __name__ == "__main__":
    sys.exit(main())
