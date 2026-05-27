from __future__ import annotations

import json
import os
import stat
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


DEFAULT_FIXTURE_TIMESTAMP = datetime(1970, 1, 1, tzinfo=UTC)


@dataclass(frozen=True, slots=True)
class OrderbookSnapshot:
    orderbook: dict[str, Any]
    fetched_at: datetime


def load_orderbook_snapshots(path: Path) -> dict[tuple[str, str], OrderbookSnapshot]:
    snapshots: dict[tuple[str, str], OrderbookSnapshot] = {}
    for line in _read_fixture_text_no_follow(path).splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        if not isinstance(row, dict):
            continue
        market_id = row.get("market_id")
        orderbook = row.get("orderbook")
        if not isinstance(market_id, str) or not isinstance(orderbook, dict):
            continue
        raw_token_id = row.get("token_id")
        token_id = raw_token_id if isinstance(raw_token_id, str) else ""
        snapshots[(market_id, token_id)] = OrderbookSnapshot(
            orderbook=orderbook,
            fetched_at=_fixture_timestamp(row),
        )
    return snapshots


def _read_fixture_text_no_follow(path: Path) -> str:
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    fd = -1
    try:
        fd = os.open(path, flags)
        path_stat = os.fstat(fd)
        if not stat.S_ISREG(path_stat.st_mode):
            raise OSError("not a regular file")
        if path_stat.st_nlink != 1:
            raise OSError("multiple hardlinks")
        with os.fdopen(fd, "r", encoding="utf-8") as file:
            fd = -1
            return file.read()
    except OSError as exc:
        msg = f"backtest fixture cannot be read safely: {path}"
        raise ValueError(msg) from exc
    finally:
        if fd >= 0:
            os.close(fd)


def _fixture_timestamp(row: dict[str, Any]) -> datetime:
    raw_timestamp = row.get("ts", row.get("fetched_at"))
    if not isinstance(raw_timestamp, str):
        return DEFAULT_FIXTURE_TIMESTAMP
    try:
        parsed = datetime.fromisoformat(raw_timestamp.replace("Z", "+00:00"))
    except ValueError:
        return DEFAULT_FIXTURE_TIMESTAMP
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed
