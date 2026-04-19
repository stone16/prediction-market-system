from __future__ import annotations

from pathlib import Path


SCHEMA_PATH = Path(__file__).resolve().parents[2] / "schema.sql"


def test_default_strategy_seed_keeps_llm_forecaster_in_schema_sql() -> None:
    schema_text = SCHEMA_PATH.read_text(encoding="utf-8")

    assert (
        '"forecaster":{"forecasters":[["rules",[["threshold","0.55"]]],'
        '["stats",[["window","15m"]]],["llm",[]]]}'
    ) in schema_text
