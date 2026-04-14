from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime
import json
from pathlib import Path
from typing import Any

from pms.core.models import EvalRecord


@dataclass
class EvalStore:
    path: Path | None = field(default_factory=lambda: Path(".data/eval_records.jsonl"))
    _items: list[EvalRecord] = field(default_factory=list)

    def append(self, record: EvalRecord) -> None:
        self._items.append(record)
        if self.path is not None:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with self.path.open("a", encoding="utf-8") as stream:
                stream.write(json.dumps(_jsonable(asdict(record))) + "\n")

    def all(self) -> list[EvalRecord]:
        return list(self._items)


def _jsonable(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: _jsonable(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_jsonable(item) for item in value]
    return value
