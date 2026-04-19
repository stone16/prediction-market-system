from __future__ import annotations

import argparse
import asyncio
from datetime import datetime
import json
import os
from pathlib import Path
import sys
from typing import Any, Callable, TypeVar

import asyncpg

from pms.core.models import EvalRecord, Feedback
from pms.storage.eval_store import insert_eval_record_row
from pms.storage.feedback_store import insert_feedback_row


T = TypeVar("T")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Migrate legacy .data JSONL rows into PostgreSQL.",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        required=True,
        help="Directory containing feedback.jsonl and eval_records.jsonl.",
    )
    parser.add_argument(
        "--database-url",
        default=os.environ.get("DATABASE_URL"),
        help="PostgreSQL DSN. Defaults to DATABASE_URL.",
    )
    return parser.parse_args()


async def _run(args: argparse.Namespace) -> None:
    database_url = args.database_url
    if not isinstance(database_url, str) or not database_url:
        msg = "--database-url is required when DATABASE_URL is unset"
        raise RuntimeError(msg)

    feedback_rows = _load_rows(
        args.data_dir / "feedback.jsonl",
        _feedback_from_payload,
    )
    eval_rows = _load_rows(
        args.data_dir / "eval_records.jsonl",
        _eval_record_from_payload,
    )

    connection = await asyncpg.connect(database_url)
    try:
        async with connection.transaction():
            for feedback in feedback_rows:
                await insert_feedback_row(connection, feedback)
            for record in eval_rows:
                await insert_eval_record_row(connection, record)
            await _assert_row_counts(connection, feedback_rows, eval_rows)
    finally:
        await connection.close()

    print(
        f"migrated {len(feedback_rows)} feedback rows and {len(eval_rows)} eval rows",
        file=sys.stdout,
    )


def _load_rows(path: Path, parser: Callable[[dict[str, Any]], T]) -> list[T]:
    if not path.exists():
        return []

    rows: list[T] = []
    with path.open("r", encoding="utf-8") as stream:
        for line_number, raw_line in enumerate(stream, start=1):
            line = raw_line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"{path.name}:{line_number}: invalid JSON row") from exc
            if not isinstance(payload, dict):
                msg = f"{path.name}:{line_number}: expected JSON object"
                raise ValueError(msg)
            try:
                rows.append(parser(payload))
            except (KeyError, TypeError, ValueError) as exc:
                raise ValueError(
                    f"{path.name}:{line_number}: invalid row payload"
                ) from exc
    return rows


def _feedback_from_payload(payload: dict[str, Any]) -> Feedback:
    metadata = payload.get("metadata")
    return Feedback(
        feedback_id=str(payload["feedback_id"]),
        target=str(payload["target"]),
        source=str(payload["source"]),
        message=str(payload["message"]),
        severity=str(payload["severity"]),
        created_at=_parse_datetime(payload["created_at"]),
        resolved=bool(payload.get("resolved", False)),
        resolved_at=_optional_datetime(payload.get("resolved_at")),
        category=_optional_str(payload.get("category")),
        metadata=metadata if isinstance(metadata, dict) else {},
    )


def _eval_record_from_payload(payload: dict[str, Any]) -> EvalRecord:
    citations = payload.get("citations")
    return EvalRecord(
        market_id=str(payload["market_id"]),
        decision_id=str(payload["decision_id"]),
        strategy_id=str(payload.get("strategy_id", "default")),
        strategy_version_id=str(payload.get("strategy_version_id", "default-v1")),
        prob_estimate=float(payload["prob_estimate"]),
        resolved_outcome=float(payload["resolved_outcome"]),
        brier_score=float(payload["brier_score"]),
        fill_status=str(payload["fill_status"]),
        recorded_at=_parse_datetime(payload["recorded_at"]),
        citations=[str(item) for item in citations] if isinstance(citations, list) else [],
        category=_optional_str(payload.get("category")),
        model_id=_optional_str(payload.get("model_id")),
        pnl=float(payload.get("pnl", 0.0)),
        slippage_bps=float(payload.get("slippage_bps", 0.0)),
        filled=bool(payload.get("filled", True)),
    )


def _parse_datetime(value: Any) -> datetime:
    if isinstance(value, str):
        return datetime.fromisoformat(value)
    if isinstance(value, datetime):
        return value
    msg = f"invalid datetime {value!r}"
    raise ValueError(msg)


def _optional_datetime(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    return _parse_datetime(value)


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


async def _assert_row_counts(
    connection: asyncpg.Connection,
    feedback_rows: list[Feedback],
    eval_rows: list[EvalRecord],
) -> None:
    feedback_ids = [row.feedback_id for row in feedback_rows]
    eval_ids = [row.decision_id for row in eval_rows]

    if feedback_ids:
        feedback_count = await connection.fetchval(
            """
            SELECT COUNT(*)
            FROM feedback
            WHERE feedback_id = ANY($1::text[])
            """,
            feedback_ids,
        )
        if feedback_count != len(feedback_rows):
            msg = (
                "feedback row-count assertion failed: "
                f"expected {len(feedback_rows)}, got {feedback_count}"
            )
            raise RuntimeError(msg)

    if eval_ids:
        eval_count = await connection.fetchval(
            """
            SELECT COUNT(*)
            FROM eval_records
            WHERE decision_id = ANY($1::text[])
            """,
            eval_ids,
        )
        if eval_count != len(eval_rows):
            msg = (
                "eval row-count assertion failed: "
                f"expected {len(eval_rows)}, got {eval_count}"
            )
            raise RuntimeError(msg)


def main() -> None:
    args = _parse_args()
    try:
        asyncio.run(_run(args))
    except Exception as exc:
        print(str(exc), file=sys.stderr)
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
