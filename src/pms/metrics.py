from __future__ import annotations

from threading import Lock


SENSOR_DISCOVERY_PRICE_FIELDS_POPULATED_RATIO_METRIC = (
    "pms_sensor_discovery_price_fields_populated_ratio"
)
SENSOR_DISCOVERY_SNAPSHOTS_WRITTEN_TOTAL_METRIC = (
    "pms_sensor_discovery_snapshots_written_total"
)
MARKETS_SNAPSHOT_LAG_SECONDS_MAX_METRIC = "pms_markets_snapshot_lag_seconds_max"


_metrics_lock = Lock()
_metrics: dict[str, float] = {
    SENSOR_DISCOVERY_PRICE_FIELDS_POPULATED_RATIO_METRIC: 0.0,
    SENSOR_DISCOVERY_SNAPSHOTS_WRITTEN_TOTAL_METRIC: 0.0,
    MARKETS_SNAPSHOT_LAG_SECONDS_MAX_METRIC: 0.0,
}


def set_metric(name: str, value: float) -> None:
    with _metrics_lock:
        _metrics[name] = value


def increment_metric(name: str, amount: float = 1.0) -> None:
    with _metrics_lock:
        _metrics[name] = _metrics.get(name, 0.0) + amount


def get_metric(name: str) -> float | None:
    with _metrics_lock:
        return _metrics.get(name)


def metrics_snapshot() -> dict[str, float]:
    with _metrics_lock:
        return dict(_metrics)
