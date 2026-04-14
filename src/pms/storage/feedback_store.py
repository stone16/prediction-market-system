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

    def __post_init__(self) -> None:
        if self.path is None or not self.path.exists():
            return
        with self.path.open("r", encoding="utf-8") as stream:
            for line in stream:
                line = line.strip()
                if not line:
                    continue
                try:
                    self._items.append(_feedback_from_json(json.loads(line)))
                except (ValueError, KeyError):
                    continue

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


def _feedback_from_json(payload: dict[str, Any]) -> Feedback:
    created_at = _parse_datetime(payload["created_at"])
    resolved_at_raw = payload.get("resolved_at")
    resolved_at = _parse_datetime(resolved_at_raw) if resolved_at_raw else None
    metadata_raw = payload.get("metadata")
    metadata: dict[str, Any] = metadata_raw if isinstance(metadata_raw, dict) else {}
    return Feedback(
        feedback_id=str(payload["feedback_id"]),
        target=str(payload["target"]),
        source=str(payload["source"]),
        message=str(payload["message"]),
        severity=str(payload["severity"]),
        created_at=created_at,
        resolved=bool(payload.get("resolved", False)),
        resolved_at=resolved_at,
        category=payload.get("category"),
        metadata=metadata,
    )


def _parse_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        return datetime.fromisoformat(value)
    msg = f"Unable to parse datetime from {value!r}"
    raise ValueError(msg)
