"""StorageProtocol — minimal interface for future persistence backends.

CP01 only requires the Protocol to exist; concrete SQLite/file backends are
deferred (the v1 MetricsCollector is in-memory only).
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class StorageProtocol(Protocol):
    """Asynchronous key/value storage abstraction."""

    async def save(self, key: str, value: bytes) -> None:
        """Persist ``value`` under ``key``."""
        ...

    async def load(self, key: str) -> bytes | None:
        """Return the value previously stored under ``key`` or ``None``."""
        ...
