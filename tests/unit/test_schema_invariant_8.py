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


def test_inner_ring_product_tables_continue_to_carry_strategy_tags() -> None:
    schema_text = SCHEMA_PATH.read_text()
    inner_ring = _extract_block(
        schema_text,
        "-- BEGIN INNER-RING PRODUCT SHELLS",
        "-- END INNER-RING PRODUCT SHELLS",
    )

    assert "CREATE TABLE IF NOT EXISTS opportunities" in inner_ring
    assert inner_ring.count("strategy_id TEXT NOT NULL") == 5
    assert inner_ring.count("strategy_version_id TEXT NOT NULL") == 5
