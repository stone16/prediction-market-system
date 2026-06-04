from __future__ import annotations

from threading import Lock


SENSOR_DISCOVERY_PRICE_FIELDS_POPULATED_RATIO_METRIC = (
    "pms_sensor_discovery_price_fields_populated_ratio"
)
SENSOR_DISCOVERY_SNAPSHOTS_WRITTEN_TOTAL_METRIC = (
    "pms_sensor_discovery_snapshots_written_total"
)
SENSOR_DISCOVERY_POOL_TIMEOUTS_TOTAL_METRIC = (
    "pms_sensor_discovery_pool_timeouts_total"
)
MARKETS_SNAPSHOT_LAG_SECONDS_MAX_METRIC = "pms_markets_snapshot_lag_seconds_max"
LLM_FORECAST_CALLS_TOTAL_METRIC = "pms_llm_forecast_calls_total"
LLM_ESTIMATED_COST_USDC_TOTAL_METRIC = "pms_llm_estimated_cost_usdc_total"
LLM_DAILY_COST_USDC_METRIC = "pms_llm_daily_cost_usdc"
LLM_DAILY_COST_LIMIT_USDC_METRIC = "pms_llm_daily_cost_limit_usdc"
LLM_BUDGET_EXHAUSTED_TOTAL_METRIC = "pms_llm_budget_exhausted_total"
SELECTION_FUNNEL_DISCOVERED_TOTAL_METRIC = (
    "pms_selection_funnel_discovered_total"
)
SELECTION_FUNNEL_SELECTED_TOTAL_METRIC = "pms_selection_funnel_selected_total"
SELECTION_FUNNEL_ROUTED_TOTAL_METRIC = "pms_selection_funnel_routed_total"
SELECTION_FUNNEL_FORECASTED_TOTAL_METRIC = "pms_selection_funnel_forecasted_total"
SELECTION_FUNNEL_CONTROLLER_EMITTED_TOTAL_METRIC = (
    "pms_selection_funnel_controller_emitted_total"
)
SELECTION_FUNNEL_TRADED_TOTAL_METRIC = "pms_selection_funnel_traded_total"


_metrics_lock = Lock()
_metrics: dict[str, float] = {
    SENSOR_DISCOVERY_PRICE_FIELDS_POPULATED_RATIO_METRIC: 0.0,
    SENSOR_DISCOVERY_SNAPSHOTS_WRITTEN_TOTAL_METRIC: 0.0,
    SENSOR_DISCOVERY_POOL_TIMEOUTS_TOTAL_METRIC: 0.0,
    MARKETS_SNAPSHOT_LAG_SECONDS_MAX_METRIC: 0.0,
    LLM_FORECAST_CALLS_TOTAL_METRIC: 0.0,
    LLM_ESTIMATED_COST_USDC_TOTAL_METRIC: 0.0,
    LLM_DAILY_COST_USDC_METRIC: 0.0,
    LLM_DAILY_COST_LIMIT_USDC_METRIC: 0.0,
    LLM_BUDGET_EXHAUSTED_TOTAL_METRIC: 0.0,
    SELECTION_FUNNEL_DISCOVERED_TOTAL_METRIC: 0.0,
    SELECTION_FUNNEL_SELECTED_TOTAL_METRIC: 0.0,
    SELECTION_FUNNEL_ROUTED_TOTAL_METRIC: 0.0,
    SELECTION_FUNNEL_FORECASTED_TOTAL_METRIC: 0.0,
    SELECTION_FUNNEL_CONTROLLER_EMITTED_TOTAL_METRIC: 0.0,
    SELECTION_FUNNEL_TRADED_TOTAL_METRIC: 0.0,
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
