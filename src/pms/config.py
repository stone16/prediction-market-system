from __future__ import annotations

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
    provider: str | None = None
    api_key: str | None = None
    model: str | None = None


class RiskSettings(BaseModel):
    max_position_usdc: float = 100.0
    min_order_usdc: float = 1.0
    max_drawdown_pct: float | None = None
    max_open_positions: int | None = None


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
