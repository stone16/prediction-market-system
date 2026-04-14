from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime
import json
from pathlib import Path
from typing import Any

from pms.core.models import Feedback


@dataclass
class FeedbackStore:
    path: Path | None = None
    _items: list[Feedback] = field(default_factory=list)

    def append(self, feedback: Feedback) -> None:
        self._items.append(feedback)
        if self.path is not None:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with self.path.open("a", encoding="utf-8") as stream:
                stream.write(json.dumps(_jsonable(asdict(feedback))) + "\n")

    def all(self) -> list[Feedback]:
        return list(self._items)


def _jsonable(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: _jsonable(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_jsonable(item) for item in value]
    return value
