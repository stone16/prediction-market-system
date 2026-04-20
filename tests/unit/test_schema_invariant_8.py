from __future__ import annotations

import re
from pathlib import Path


SCHEMA_PATH = Path("schema.sql")


def _extract_block(schema_text: str, begin: str, end: str) -> str:
    match = re.search(
        rf"(?s){re.escape(begin)}\s*(.*?)\s*{re.escape(end)}",
        schema_text,
    )
    assert match is not None
    return match.group(1)


def test_schema_declares_strategy_identity_and_version_blocks() -> None:
    schema_text = SCHEMA_PATH.read_text()

    assert "-- strategies: inner-ring identity table (Invariants 3, 8)" in schema_text
    assert "CREATE TABLE IF NOT EXISTS strategies" in schema_text
    assert "-- strategy_versions: immutable hash-keyed version rows (Invariant 3)" in schema_text
    assert "CREATE TABLE IF NOT EXISTS strategy_versions" in schema_text


def test_outer_and_middle_ring_tables_remain_strategy_agnostic() -> None:
    schema_text = SCHEMA_PATH.read_text()
    outer_ring = _extract_block(
        schema_text,
        "-- BEGIN OUTER RING",
        "-- END OUTER RING",
    )
    middle_ring = _extract_block(
        schema_text,
        "-- BEGIN MIDDLE RING",
        "-- END MIDDLE RING",
    )

    assert "strategy_id" not in outer_ring
    assert "strategy_version_id" not in outer_ring
    assert "strategy_id" not in middle_ring
    assert "strategy_version_id" not in middle_ring


def _extract_inner_ring_tables(inner_ring: str) -> dict[str, str]:
    """Return a mapping of table_name -> CREATE TABLE body (column definitions
    and constraints only, without trailing semicolon).

    Drives the per-table whitelist assertion below so new tables fail loud
    unless they explicitly opt into one of the two identity shapes.
    """
    pattern = re.compile(
        r"CREATE TABLE IF NOT EXISTS\s+(\w+)\s*\((?P<body>.*?)\)\s*;",
        re.DOTALL,
    )
    return {match.group(1): match.group("body") for match in pattern.finditer(inner_ring)}


# Multi-strategy aggregate tables carry identity via an array or JSONB
# column rather than standalone (strategy_id, strategy_version_id) columns
# because one row spans multiple strategies. Each entry names the column
# that carries the identity so a future refactor can't drop it silently.
_MULTI_STRATEGY_AGGREGATE_TABLES: dict[str, str] = {
    "backtest_runs": "strategy_ids",  # TEXT[] enumerates the job's strategies
    "evaluation_reports": "ranked_strategies",  # JSONB holds (strategy_id, strategy_version_id) per row
}


def test_inner_ring_product_tables_continue_to_carry_strategy_tags() -> None:
    schema_text = SCHEMA_PATH.read_text()
    inner_ring = _extract_block(
        schema_text,
        "-- BEGIN INNER-RING PRODUCT SHELLS",
        "-- END INNER-RING PRODUCT SHELLS",
    )
    tables = _extract_inner_ring_tables(inner_ring)

    expected_per_row_tables = {
        "feedback",
        "eval_records",
        "orders",
        "fills",
        "opportunities",
        "strategy_runs",
        "backtest_live_comparisons",
    }
    assert expected_per_row_tables.issubset(tables.keys())
    assert set(_MULTI_STRATEGY_AGGREGATE_TABLES).issubset(tables.keys())

    for table_name, body in tables.items():
        if table_name in _MULTI_STRATEGY_AGGREGATE_TABLES:
            identity_column = _MULTI_STRATEGY_AGGREGATE_TABLES[table_name]
            assert identity_column in body, (
                f"{table_name} is whitelisted as a multi-strategy aggregate "
                f"but no longer references `{identity_column}`"
            )
            continue
        assert (
            "strategy_id TEXT NOT NULL" in body
        ), f"{table_name} missing `strategy_id TEXT NOT NULL`"
        assert (
            "strategy_version_id TEXT NOT NULL" in body
        ), f"{table_name} missing `strategy_version_id TEXT NOT NULL`"
