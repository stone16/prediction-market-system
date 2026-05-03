from __future__ import annotations

from pathlib import Path

from pms.config import PMSSettings


ROOT = Path(__file__).resolve().parents[2]


def test_live_soak_config_loads_tight_first_live_risk_caps() -> None:
    settings = PMSSettings.load(ROOT / "config.live-soak.yaml")

    assert settings.risk.max_position_per_market == 5.0
    assert settings.risk.max_total_exposure == 50.0
    assert settings.risk.max_drawdown_pct == 20.0
    assert settings.risk.max_open_positions == 5
    assert settings.risk.max_quantity_shares == 500.0
    assert settings.risk.min_order_usdc == 1.0
    assert settings.risk.slippage_threshold_bps == 50.0


def test_live_soak_config_keeps_credentials_env_only() -> None:
    settings = PMSSettings.load(ROOT / "config.live-soak.yaml")

    assert settings.polymarket.private_key is None
    assert settings.polymarket.api_key is None
    assert settings.polymarket.api_secret is None
    assert settings.polymarket.api_passphrase is None
    assert settings.polymarket.funder_address is None
