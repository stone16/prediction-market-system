from __future__ import annotations

import getpass
import os
from pathlib import Path
from typing import Any, Self

import yaml
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from pms.core.enums import RunMode


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
    max_slippage_bps: int = 50
    time_in_force: str = "GTC"


class RiskSettings(BaseModel):
    max_position_per_market: float = 100.0
    max_total_exposure: float = 1000.0
    max_drawdown_pct: float | None = None
    max_open_positions: int | None = None
    min_order_usdc: float = 1.0
    slippage_threshold_bps: float = 50.0


class SensorSettings(BaseModel):
    poll_interval_s: float = 5.0
    max_reconnect_interval_s: float = 60.0
    max_subscription_asset_ids: int | None = Field(default=100, ge=1)


class DashboardSettings(BaseModel):
    stale_snapshot_threshold_s: float = 300.0


def _default_database_dsn() -> str:
    override = os.environ.get("DATABASE_URL")
    if override:
        return override
    return f"postgresql://localhost/pms_dev_{getpass.getuser()}"


class DatabaseSettings(BaseModel):
    dsn: str = Field(default_factory=_default_database_dsn)
    pool_min_size: int = 2
    pool_max_size: int = 10


class PMSSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="PMS_",
        env_nested_delimiter="__",
        extra="ignore",
    )

    mode: RunMode = RunMode.BACKTEST
    live_trading_enabled: bool = False
    auto_migrate_default_v2: bool = True
    enforce_schema_check: bool | None = None
    factor_cadence_s: float = 1.0
    api_host: str = "127.0.0.1"
    api_token: str | None = None
    polymarket: PolymarketSettings = Field(default_factory=PolymarketSettings)
    llm: LLMSettings = Field(default_factory=LLMSettings)
    risk: RiskSettings = Field(default_factory=RiskSettings)
    sensor: SensorSettings = Field(default_factory=SensorSettings)
    controller: ControllerSettings = Field(default_factory=ControllerSettings)
    database: DatabaseSettings = Field(default_factory=DatabaseSettings)
    dashboard: DashboardSettings = Field(default_factory=DashboardSettings)

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
