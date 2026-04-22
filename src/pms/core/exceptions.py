from __future__ import annotations


class PMSBootError(RuntimeError):
    """Raised when a PMS process cannot complete mandatory startup checks."""


class KalshiStubError(NotImplementedError):
    """Raised when v1 runtime paths encounter the reserved Kalshi venue."""


class SensorDataQualityError(RuntimeError):
    """Raised when sensor input quality degrades beyond the configured threshold."""
