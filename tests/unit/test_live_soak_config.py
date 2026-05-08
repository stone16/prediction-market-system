from __future__ import annotations

from pathlib import Path

import pytest

from pms.config import PMSSettings


ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(autouse=True)
def stub_llm_api_key(monkeypatch: pytest.MonkeyPatch) -> None:
    """Provide a stub PMS_LLM__API_KEY so config.live-soak.yaml passes validation.

    The soak YAML enables the LLM forecaster; the validator at
    ``LLMSettings._validate_when_enabled`` requires ``api_key`` to be non-empty
    when ``enabled=True``. In production the key is supplied by the operator's
    shell (``export PMS_LLM__API_KEY=sk-ant-...``); tests stub it so they don't
    depend on operator state.
    """
    monkeypatch.setenv("PMS_LLM__API_KEY", "sk-stub-test-only")


def test_live_soak_config_loads_tight_first_live_risk_caps() -> None:
    settings = PMSSettings.load(ROOT / "config.live-soak.yaml")

    assert settings.risk.max_position_per_market == 5.0
    assert settings.risk.max_total_exposure == 50.0
    assert settings.risk.max_drawdown_pct == 20.0
    assert settings.risk.max_open_positions == 5
    assert settings.risk.max_quantity_shares == 500.0
    assert settings.risk.min_order_usdc == 1.0
    assert settings.risk.slippage_threshold_bps == 50.0


def test_live_soak_config_relaxes_paper_factor_gate_for_phase_a() -> None:
    settings = PMSSettings.load(ROOT / "config.live-soak.yaml")

    assert settings.controller.strict_factor_gates is False


def test_live_soak_config_keeps_credentials_env_only() -> None:
    settings = PMSSettings.load(ROOT / "config.live-soak.yaml")

    assert settings.polymarket.private_key is None
    assert settings.polymarket.api_key is None
    assert settings.polymarket.api_secret is None
    assert settings.polymarket.api_passphrase is None
    assert settings.polymarket.funder_address is None


def test_live_soak_config_enables_llm_forecaster_with_widened_budget() -> None:
    settings = PMSSettings.load(ROOT / "config.live-soak.yaml")

    assert settings.llm.enabled is True
    assert settings.llm.provider == "anthropic"
    assert settings.llm.model == "claude-sonnet-4-6"
    assert settings.llm.max_daily_llm_cost_usdc == 25.0


def test_live_soak_config_keeps_llm_api_key_out_of_yaml(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The committed YAML must not carry api_key; key flows from PMS_LLM__API_KEY."""
    monkeypatch.delenv("PMS_LLM__API_KEY", raising=False)
    with pytest.raises(Exception) as exc_info:
        PMSSettings.load(ROOT / "config.live-soak.yaml")
    assert "api_key is required" in str(exc_info.value)
