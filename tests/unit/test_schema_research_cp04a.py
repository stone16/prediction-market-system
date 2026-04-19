from __future__ import annotations

import re
from pathlib import Path


SCHEMA_PATH = Path("schema.sql")
RESEARCH_TABLES = [
    "backtest_runs",
    "strategy_runs",
    "evaluation_reports",
    "backtest_live_comparisons",
]


def _extract_block(schema_text: str, begin: str, end: str) -> str:
    match = re.search(
        rf"(?s){re.escape(begin)}\s*(.*?)\s*{re.escape(end)}",
        schema_text,
    )
    assert match is not None
    return match.group(1)


def test_schema_declares_research_backtest_tables() -> None:
    schema_text = SCHEMA_PATH.read_text()

    for table_name in RESEARCH_TABLES:
        assert f"CREATE TABLE IF NOT EXISTS {table_name}" in schema_text

    assert "strategy_ids TEXT[] NOT NULL" in schema_text
    assert "CONSTRAINT strategy_runs_strategy_identity_check" in schema_text
    assert "CONSTRAINT backtest_live_comparisons_strategy_identity_check" in schema_text
    assert "CONSTRAINT evaluation_reports_run_id_ranking_metric_key" in schema_text


def test_research_backtest_tables_stay_out_of_outer_and_middle_rings() -> None:
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

    for table_name in RESEARCH_TABLES:
        assert table_name not in outer_ring
        assert table_name not in middle_ring
