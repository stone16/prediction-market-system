from __future__ import annotations

import getpass
import os
from pathlib import Path
from typing import Any, Literal, Self

import yaml
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from pms.core.enums import RunMode
from pms.core.models import LiveTradingDisabledError, VenueCredentials


class MissingPolymarketCredentialsError(LiveTradingDisabledError):
    """Raised when LIVE mode lacks required Polymarket credentials."""


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
    first_live_order_approval_path: str | None = None

    def credentials(self) -> VenueCredentials:
        return VenueCredentials(
            venue="polymarket",
            host=self.host,
            private_key=self.private_key,
            api_key=self.api_key,
            api_secret=self.api_secret,
            api_passphrase=self.api_passphrase,
            signature_type=self.signature_type,
            funder_address=self.funder_address,
            chain_id=self.chain_id,
        )


class LLMSettings(BaseModel):
    enabled: bool = False
    provider: str | None = None
    api_key: str | None = None
    model: str = "claude-3-5-sonnet-latest"


class ControllerSettings(BaseModel):
    min_volume: float = 0.0
    max_slippage_bps: int = 50
    time_in_force: str = "GTC"
    max_book_age_ms: float = 1_000.0
    allowed_book_clock_skew_ms: float = 250.0
    max_spread_bps: float = 100.0
    strict_factor_gates: bool = True
    quote_source: Literal["postgres_snapshot", "venue_direct", "dual"] = (
        "postgres_snapshot"
    )
    direct_quote_min_notional_usdc: float | None = 100.0
    dual_quote_max_price_delta_bps: float = 25.0


class RiskSettings(BaseModel):
    max_position_per_market: float = 100.0
    max_total_exposure: float = 1000.0
    max_drawdown_pct: float | None = None
    max_open_positions: int | None = None
    min_order_usdc: float = 1.0
    slippage_threshold_bps: float = 50.0
    # Maximum share/contract count per order. Catches the low-price-token
    # blow-up case: at limit_price=0.001 and notional=$10, quantity=10000
    # shares — well beyond a small-test risk envelope. None disables.
    max_quantity_shares: float | None = None


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
    live_account_reconciliation_required: bool = True
    live_emergency_audit_path: str = ".data/live-emergency-audit.jsonl"
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


def validate_live_mode_ready(settings: PMSSettings) -> VenueCredentials:
    if not settings.live_trading_enabled:
        msg = "Live trading is disabled. Set live_trading_enabled=true in config."
        raise LiveTradingDisabledError(msg)

    credentials = settings.polymarket.credentials()
    missing = _missing_polymarket_fields(credentials)
    if missing:
        fields = ", ".join(missing)
        msg = f"Missing Polymarket credential fields: {fields}"
        raise MissingPolymarketCredentialsError(msg)
    if settings.controller.time_in_force.upper() == "GTC":
        msg = (
            "LIVE GTC disabled until an open-order ledger reserves "
            "resting order exposure"
        )
        raise LiveTradingDisabledError(msg)
    if settings.mode == RunMode.LIVE and not settings.live_account_reconciliation_required:
        msg = (
            "LIVE account reconciliation must be required before autonomous "
            "live trading can start"
        )
        raise LiveTradingDisabledError(msg)
    return credentials


def _missing_polymarket_fields(credentials: VenueCredentials) -> list[str]:
    missing: list[str] = []
    required_text_fields = {
        "private_key": credentials.private_key,
        "api_key": credentials.api_key,
        "api_secret": credentials.api_secret,
        "api_passphrase": credentials.api_passphrase,
        "funder_address": credentials.funder_address,
    }
    for field_name, value in required_text_fields.items():
        if value is None or value.strip() == "":
            missing.append(field_name)
    if credentials.signature_type is None:
        missing.append("signature_type")
    return missing
