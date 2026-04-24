from __future__ import annotations

from threading import Lock


SENSOR_DISCOVERY_PRICE_FIELDS_POPULATED_RATIO_METRIC = (
    "pms_sensor_discovery_price_fields_populated_ratio"
)


_metrics_lock = Lock()
_metrics: dict[str, float] = {
    SENSOR_DISCOVERY_PRICE_FIELDS_POPULATED_RATIO_METRIC: 0.0,
}


def set_metric(name: str, value: float) -> None:
    with _metrics_lock:
        _metrics[name] = value


def get_metric(name: str) -> float | None:
    with _metrics_lock:
        return _metrics.get(name)


def metrics_snapshot() -> dict[str, float]:
    with _metrics_lock:
        return dict(_metrics)
