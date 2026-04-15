from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Self

import yaml
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from pms.core.enums import RunMode


DEFAULT_DATA_DIR = Path(".data")


def data_dir() -> Path:
    """Resolve the per-run data directory.

    Honours ``PMS_DATA_DIR`` env var so dev shells and pytest fixtures can
    isolate state from the committed repo root. Falls back to ``.data``.
    """
    override = os.environ.get("PMS_DATA_DIR")
    if override:
        return Path(override)
    return DEFAULT_DATA_DIR


class PolymarketSettings(BaseModel):
    host: str = "https://clob.polymarket.com"
    websocket_url: str = "wss://ws-subscriptions-clob.polymarket.com/ws/"
    private_key: str | None = None
    api_key: str | None = None
    api_secret: str | None = None
    api_passphrase: str | None = None
    signature_type: int | None = None
    funder_address: str | None = None
    chain_id: int = 137


class LLMSettings(BaseModel):
    enabled: bool = False
    provider: str | None = None
    api_key: str | None = None
    model: str = "claude-3-5-sonnet-latest"


class ControllerSettings(BaseModel):
    min_volume: float = 0.0
    max_slippage_bps: int = 100
    time_in_force: str = "GTC"


class RiskSettings(BaseModel):
    max_position_usdc: float = 100.0
    max_position_per_market: float = 100.0
    max_total_exposure: float = 1000.0
    max_brier_score: float = 0.30
    slippage_threshold_bps: float = 50.0
    min_win_rate: float = 0.50
    min_order_usdc: float = 1.0
    max_drawdown_pct: float | None = None
    max_open_positions: int | None = None


class SensorSettings(BaseModel):
    poll_interval_s: float = 5.0


class PMSSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="PMS_",
        env_nested_delimiter="__",
        extra="ignore",
    )

    mode: RunMode = RunMode.BACKTEST
    live_trading_enabled: bool = False
    polymarket: PolymarketSettings = Field(default_factory=PolymarketSettings)
    llm: LLMSettings = Field(default_factory=LLMSettings)
    risk: RiskSettings = Field(default_factory=RiskSettings)
    sensor: SensorSettings = Field(default_factory=SensorSettings)
    controller: ControllerSettings = Field(default_factory=ControllerSettings)

    @classmethod
    def load(cls, config_path: str | Path | None = "config.yaml") -> Self:
        if config_path is None:
            return cls()

        path = Path(config_path)
        if not path.exists():
            return cls()

        loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
        if loaded is None:
            return cls()
        if not isinstance(loaded, dict):
            msg = f"Expected mapping in config file {path}"
            raise ValueError(msg)

        config_data: dict[str, Any] = loaded
        return cls(**config_data)
