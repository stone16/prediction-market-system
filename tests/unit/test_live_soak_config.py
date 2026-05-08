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
    assert settings.llm.max_daily_llm_cost_usdc == 25.0


def test_live_soak_config_yaml_does_not_pin_model_or_credentials() -> None:
    """The committed YAML must not carry api_key / model / base_url so the
    operator can switch between native Anthropic and Anthropic-compatible
    providers (DeepSeek, OpenRouter) by editing only their local .env / shell.

    YAML init args override env vars in pydantic-settings, so any of these
    fields in the committed YAML would silently mute the operator's .env.
    """
    yaml_text = (ROOT / "config.live-soak.yaml").read_text(encoding="utf-8")
    forbidden = ("api_key:", "model:", "base_url:")
    # Carve out the LLM section so we don't accidentally false-positive on
    # the polymarket section (which legitimately has api_key: null etc.).
    llm_section_start = yaml_text.index("llm:")
    llm_section = yaml_text[llm_section_start:]
    for keyword in forbidden:
        assert keyword not in llm_section, (
            f"config.live-soak.yaml llm section must not pin '{keyword}'; "
            f"found one. Remove it so PMS_LLM__* env vars can fill the field."
        )
