from __future__ import annotations

from dataclasses import asdict, dataclass, field, replace
from datetime import UTC, datetime
import json
from pathlib import Path
from typing import Any

from pms.core.models import Feedback


@dataclass
class FeedbackStore:
    path: Path | None = field(default_factory=lambda: Path(".data/feedback.jsonl"))
    _items: list[Feedback] = field(default_factory=list)

    def append(self, feedback: Feedback) -> None:
        self._items.append(feedback)
        self._append_to_disk(feedback)

    def all(self) -> list[Feedback]:
        return list(self._items)

    def list(self, *, resolved: bool | None = None) -> list[Feedback]:
        if resolved is None:
            return self.all()
        return [feedback for feedback in self._items if feedback.resolved is resolved]

    def resolve(self, feedback_id: str) -> Feedback | None:
        for index, feedback in enumerate(self._items):
            if feedback.feedback_id == feedback_id:
                resolved = replace(
                    feedback,
                    resolved=True,
                    resolved_at=datetime.now(tz=UTC),
                )
                self._items[index] = resolved
                self._rewrite_disk()
                return resolved
        return None

    def _append_to_disk(self, feedback: Feedback) -> None:
        if self.path is None:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(_jsonable(asdict(feedback))) + "\n")

    def _rewrite_disk(self) -> None:
        if self.path is None:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("w", encoding="utf-8") as stream:
            for feedback in self._items:
                stream.write(json.dumps(_jsonable(asdict(feedback))) + "\n")


def _jsonable(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: _jsonable(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_jsonable(item) for item in value]
    return value
