from __future__ import annotations

import re
from pathlib import Path


SCHEMA_PATH = Path("schema.sql")


def _extract_outer_ring_block(schema_text: str) -> str:
    match = re.search(
        r"(?s)-- BEGIN OUTER RING\s*(.*?)\s*-- END OUTER RING",
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


def test_outer_ring_tables_remain_strategy_agnostic() -> None:
    outer_ring = _extract_outer_ring_block(SCHEMA_PATH.read_text())

    assert "strategy_id" not in outer_ring
    assert "strategy_version_id" not in outer_ring
