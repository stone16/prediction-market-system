from __future__ import annotations

import argparse
import csv
import json
import os
import tempfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import httpx


DEFAULT_GAMMA_BASE_URL = "https://gamma-api.polymarket.com"


@dataclass(frozen=True, slots=True)
class CategoryPriorCsvRow:
    market_id: str
    category: str
    yes_payout: str
    no_payout: str
    resolved_at: str


@dataclass(frozen=True, slots=True)
class ExportStats:
    written: int
    fetched: int
    skipped: int
    output_path: Path


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Export a no-lookahead category-prior observations CSV from "
            "Polymarket Gamma resolved markets."
        )
    )
    parser.add_argument("--output", required=True, help="CSV output path.")
    parser.add_argument(
        "--gamma-base-url",
        default=DEFAULT_GAMMA_BASE_URL,
        help=f"Gamma API base URL. Default: {DEFAULT_GAMMA_BASE_URL}",
    )
    parser.add_argument("--page-limit", type=int, default=100)
    parser.add_argument("--max-pages", type=int, default=25)
    parser.add_argument("--min-observations", type=int, default=100)
    args = parser.parse_args()

    stats = export_category_prior_observations(
        output_path=Path(args.output),
        gamma_base_url=str(args.gamma_base_url),
        page_limit=int(args.page_limit),
        max_pages=int(args.max_pages),
        min_observations=int(args.min_observations),
    )
    print(
        "category-prior observations exported "
        f"written={stats.written} fetched={stats.fetched} "
        f"skipped={stats.skipped} output={stats.output_path}"
    )


def export_category_prior_observations(
    *,
    output_path: Path,
    gamma_base_url: str = DEFAULT_GAMMA_BASE_URL,
    page_limit: int = 100,
    max_pages: int = 25,
    min_observations: int = 100,
) -> ExportStats:
    if page_limit <= 0:
        msg = "page_limit must be positive"
        raise ValueError(msg)
    if max_pages <= 0:
        msg = "max_pages must be positive"
        raise ValueError(msg)
    if min_observations <= 0:
        msg = "min_observations must be positive"
        raise ValueError(msg)

    rows: list[CategoryPriorCsvRow] = []
    seen_market_ids: set[str] = set()
    fetched = 0
    skipped = 0
    with httpx.Client(base_url=gamma_base_url, timeout=20.0) as client:
        for page_index in range(max_pages):
            page = _fetch_closed_market_page(
                client,
                limit=page_limit,
                offset=page_index * page_limit,
            )
            if not page:
                break
            fetched += len(page)
            for market in page:
                row = observation_row_from_gamma_market(market)
                if row is None:
                    skipped += 1
                    continue
                if row.market_id in seen_market_ids:
                    skipped += 1
                    continue
                seen_market_ids.add(row.market_id)
                rows.append(row)
            if len(page) < page_limit or len(rows) >= min_observations:
                break

    if len(rows) < min_observations:
        msg = (
            "insufficient category-prior observations exported: "
            f"{len(rows)} < {min_observations}"
        )
        raise RuntimeError(msg)

    _write_rows(output_path, rows)
    return ExportStats(
        written=len(rows),
        fetched=fetched,
        skipped=skipped,
        output_path=output_path,
    )


def observation_row_from_gamma_market(
    market: Mapping[str, object],
) -> CategoryPriorCsvRow | None:
    market_id = _first_text(
        market.get("conditionId"),
        market.get("condition_id"),
        market.get("id"),
    )
    if market_id is None:
        return None

    payouts = _binary_payouts_from_outcome_prices(market)
    if payouts is None:
        return None
    yes_payout, no_payout = payouts

    resolved_at = _resolved_at(market)
    if resolved_at is None:
        return None

    return CategoryPriorCsvRow(
        market_id=market_id,
        category=_category_from_market(market),
        yes_payout=yes_payout,
        no_payout=no_payout,
        resolved_at=resolved_at.isoformat().replace("+00:00", "Z"),
    )


def _fetch_closed_market_page(
    client: httpx.Client,
    *,
    limit: int,
    offset: int,
) -> list[Mapping[str, object]]:
    response = client.get(
        "/markets",
        params={
            "closed": "true",
            "limit": str(limit),
            "offset": str(offset),
            "order": "closedTime",
            "ascending": "false",
        },
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, list):
        msg = "Expected Gamma /markets response to be a list"
        raise ValueError(msg)
    return [row for row in payload if isinstance(row, dict)]


def _binary_payouts_from_outcome_prices(
    market: Mapping[str, object],
) -> tuple[str, str] | None:
    try:
        outcomes = _decode_sequence(market.get("outcomes"))
        prices = _decode_sequence(market.get("outcomePrices"))
    except json.JSONDecodeError:
        return None
    if len(outcomes) != 2 or len(prices) != 2:
        return None

    prices_by_outcome: dict[str, Decimal] = {}
    for outcome, price in zip(outcomes, prices, strict=True):
        normalized_outcome = str(outcome).strip().upper()
        if normalized_outcome not in {"YES", "NO"}:
            return None
        parsed_price = _decimal(price)
        if parsed_price not in {Decimal("0"), Decimal("1")}:
            return None
        prices_by_outcome[normalized_outcome] = parsed_price

    if set(prices_by_outcome) != {"YES", "NO"}:
        return None
    yes_payout = prices_by_outcome["YES"]
    no_payout = prices_by_outcome["NO"]
    if yes_payout + no_payout != Decimal("1"):
        return None
    return str(int(yes_payout)), str(int(no_payout))


def _category_from_market(market: Mapping[str, object]) -> str:
    direct_category = _first_text(
        market.get("category"),
        market.get("marketCategory"),
        market.get("market_category"),
    )
    if direct_category is not None:
        return direct_category

    event = _first_mapping(market.get("events"))
    if event is None:
        return "uncategorized"

    event_category = _first_text(event.get("category"))
    if event_category is not None:
        return event_category

    series = _first_mapping(event.get("series"))
    if series is not None:
        series_category = _first_text(
            series.get("slug"),
            series.get("ticker"),
            series.get("title"),
        )
        if series_category is not None:
            return series_category

    event_slug = _first_text(event.get("slug"), event.get("title"), event.get("id"))
    return event_slug if event_slug is not None else "uncategorized"


def _resolved_at(market: Mapping[str, object]) -> datetime | None:
    for key in ("closedTime", "resolutionDate", "resolvedAt", "endDate"):
        value = market.get(key)
        if isinstance(value, str):
            parsed = _parse_datetime(value)
            if parsed is not None:
                return parsed
    return None


def _decode_sequence(value: object) -> Sequence[object]:
    if isinstance(value, str):
        loaded = json.loads(value)
    else:
        loaded = value
    if not isinstance(loaded, list):
        return ()
    return loaded


def _decimal(value: object) -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return Decimal("NaN")


def _parse_datetime(value: str) -> datetime | None:
    raw_value = value.strip()
    if raw_value == "":
        return None
    if raw_value.endswith("Z"):
        raw_value = f"{raw_value[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(raw_value)
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _first_text(*values: object) -> str | None:
    for value in values:
        if isinstance(value, str):
            text = value.strip()
            if text != "":
                return text
        elif isinstance(value, int | float):
            return str(value)
    return None


def _first_mapping(value: object) -> Mapping[str, object] | None:
    if isinstance(value, Mapping):
        return value
    if not isinstance(value, list):
        return None
    for item in value:
        if isinstance(item, Mapping):
            return item
    return None


def _write_rows(output_path: Path, rows: Sequence[CategoryPriorCsvRow]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(
        prefix=f".{output_path.name}.",
        suffix=".tmp",
        dir=output_path.parent,
        text=True,
    )
    temp_path = Path(temp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(
                ("market_id", "category", "yes_payout", "no_payout", "resolved_at")
            )
            for row in rows:
                writer.writerow(
                    (
                        row.market_id,
                        row.category,
                        row.yes_payout,
                        row.no_payout,
                        row.resolved_at,
                    )
                )
        temp_path.chmod(0o600)
        temp_path.replace(output_path)
    except Exception:
        temp_path.unlink(missing_ok=True)
        raise


if __name__ == "__main__":
    main()
