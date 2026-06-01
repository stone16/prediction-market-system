from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

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

    assert settings.risk.max_position_per_market == 1.0
    assert settings.risk.max_total_exposure == 50.0
    assert settings.risk.max_drawdown_pct == 20.0
    assert settings.risk.max_daily_loss_usdc == 20.0
    assert settings.risk.max_open_positions == 50
    assert settings.risk.max_exposure_per_risk_group == 15.0
    assert settings.risk.max_quantity_shares == 500.0
    assert settings.risk.min_order_usdc == 1.0
    assert settings.risk.slippage_threshold_bps == 50.0
    assert settings.strategies.flb_entry_execution_cost_bps == 15.0
    assert settings.strategies.flb_fee_rate == 0.07


def test_live_soak_config_relaxes_paper_factor_gate_for_phase_a() -> None:
    settings = PMSSettings.load(ROOT / "config.live-soak.yaml")

    assert settings.controller.strict_factor_gates is False


def test_live_soak_config_uses_paper_snapshot_freshness_window() -> None:
    settings = PMSSettings.load(ROOT / "config.live-soak.yaml")

    assert settings.mode == "paper"
    assert settings.live_trading_enabled is False
    assert settings.controller.quote_source == "postgres_snapshot"
    assert settings.controller.max_book_age_ms == pytest.approx(15_000.0)


def test_live_soak_config_uses_tradeable_paper_strategy() -> None:
    settings = PMSSettings.load(ROOT / "config.live-soak.yaml")

    assert settings.paper_soak_strategy_id == "paper_multi_factor_v1"
    assert settings.paper_soak_archive_default is True


def test_live_soak_config_has_no_dead_top_level_calibration_section() -> None:
    yaml_text = (ROOT / "config.live-soak.yaml").read_text(encoding="utf-8")
    settings = PMSSettings.load(ROOT / "config.live-soak.yaml")

    assert "\ncalibration:" not in yaml_text
    assert settings.position_exit.enabled is True
    assert settings.position_exit.stop_loss_pct == pytest.approx(30.0)
    assert settings.position_exit.profit_take_pct == pytest.approx(50.0)
    assert settings.position_exit.max_holding_days == 7
    assert settings.position_exit.reentry_cooldown_s == pytest.approx(3600.0)


def test_live_soak_config_tunes_gamma_discovery_http_pool() -> None:
    settings = PMSSettings.load(ROOT / "config.live-soak.yaml")

    assert settings.sensor.poll_interval_s == pytest.approx(60.0)
    assert settings.sensor.market_data_ws_max_size_bytes == 8_388_608
    assert settings.sensor.discovery_page_limit == 100
    assert settings.sensor.discovery_max_pages == 30
    assert settings.sensor.discovery_pagination_mode == "keyset"
    assert settings.sensor.discovery_order == "volume24hr"
    assert settings.sensor.discovery_ascending is False
    assert settings.sensor.discovery_http_timeout_s == pytest.approx(10.0)
    assert settings.sensor.discovery_http_pool_timeout_s == pytest.approx(10.0)
    assert settings.sensor.discovery_http_max_connections == 10
    assert settings.sensor.discovery_http_max_keepalive_connections == 5
    assert settings.sensor.discovery_http_keepalive_expiry_s == pytest.approx(120.0)
    assert settings.sensor.max_subscription_asset_ids == 100


def test_live_soak_config_uses_distinct_audit_sinks() -> None:
    settings = PMSSettings.load(ROOT / "config.live-soak.yaml")

    assert settings.live_emergency_audit_path == ".data/live-emergency-audit.jsonl"
    assert settings.live_first_order_audit_path == ".data/first-order-audit.jsonl"
    assert settings.live_first_order_audit_path != settings.live_emergency_audit_path


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


def test_live_config_example_is_non_secret_and_uses_soak_risk_envelope() -> None:
    template_path = ROOT / "config.live.yaml.example"
    payload = yaml.safe_load(template_path.read_text(encoding="utf-8"))

    assert payload["mode"] == "live"
    assert payload["secret_source"] == "local_file"
    assert payload["live_trading_enabled"] is True
    assert payload["live_account_reconciliation_required"] is True
    assert payload["live_paper_soak_report_path"] == (
        "/secure/pms/paper-soak-go-report.md"
    )
    assert re.fullmatch(
        r"\d{4}-\d{2}-\d{2}\.md",
        Path(payload["live_paper_soak_report_path"]).name,
    ) is None, (
        "The LIVE template must not bake in a dated paper-soak artifact path; "
        "operators should regenerate the final GO report at the configured path."
    )
    assert payload["live_operator_rehearsal_report_path"] == (
        "/secure/pms/operator-rehearsal-report.md"
    )
    assert payload["live_execution_model_path"] == (
        "/secure/pms/execution-model.json"
    )
    assert payload["live_paper_backtest_diff_path"] == (
        "/secure/pms/paper-backtest-execution-diff.json"
    )
    assert payload["live_preflight_artifact_path"] == (
        "/secure/pms/credentialed-preflight.json"
    )
    assert payload["live_emergency_audit_path"] == (
        "/secure/pms/live-emergency-audit.jsonl"
    )
    assert payload["live_first_order_audit_path"] == (
        "/secure/pms/first-order-audit.jsonl"
    )

    assert payload["risk"] == {
        "max_position_per_market": 5.0,
        "max_total_exposure": 50.0,
        "max_drawdown_pct": 20.0,
        "max_daily_loss_usdc": 20.0,
        "max_open_positions": 5,
        "max_exposure_per_risk_group": 15.0,
        "min_order_usdc": 1.0,
        "slippage_threshold_bps": 50.0,
        "max_quantity_shares": 500.0,
    }
    assert payload["controller"]["time_in_force"] == "IOC"
    assert payload["controller"]["strict_factor_gates"] is True
    assert payload["controller"]["quote_source"] == "dual"
    assert payload["controller"]["category_prior_observations_path"] == (
        "/secure/pms/category-prior-observations.csv"
    )
    assert payload["controller"]["category_prior_min_global_samples"] == 100
    assert payload["strategies"] == {
        "flb_calibration_path": "/secure/pms/flb-calibration.csv",
        "flb_min_calibration_samples": 100,
        "flb_entry_execution_cost_bps": 15.0,
        "flb_fee_rate": 0.07,
    }
    assert payload["llm"] == {
        "enabled": False,
        "provider": "anthropic",
        "max_daily_llm_cost_usdc": 25.0,
    }
    assert payload["polymarket"] == {
        "operator_approval_mode": "every_order",
        "first_live_order_approval_path": "/secure/pms/first-order.json",
        "operator_approval_max_age_s": 300.0,
    }

    rendered = template_path.read_text(encoding="utf-8")
    forbidden_secret_fields = (
        "private_key:",
        "api_key:",
        "api_secret:",
        "api_passphrase:",
        "funder_address:",
    )
    for field_name in forbidden_secret_fields:
        assert field_name not in rendered


def test_live_config_example_loads_after_local_secret_file_is_staged(
    tmp_path: Path,
) -> None:
    secret_path = tmp_path / "polymarket.local-secrets.yaml"
    secret_path.write_text(
        "\n".join(
            [
                "polymarket:",
                "  private_key: private-key",
                "  api_key: api-key",
                "  api_secret: api-secret",
                "  api_passphrase: passphrase",
                "  signature_type: 1",
                "  funder_address: '0x1111111111111111111111111111111111111111'",
            ]
        ),
        encoding="utf-8",
    )
    secret_path.chmod(0o600)
    template_text = (ROOT / "config.live.yaml.example").read_text(encoding="utf-8")
    config_path = tmp_path / "config.live.yaml"
    config_path.write_text(
        template_text.replace(
            "~/.config/pms/polymarket.local-secrets.yaml",
            str(secret_path),
        ),
        encoding="utf-8",
    )

    settings = PMSSettings.load(config_path)

    assert settings.polymarket.api_key == "api-key"
    assert settings.controller.category_prior_observations_path == (
        "/secure/pms/category-prior-observations.csv"
    )
    assert settings.strategies.flb_calibration_path == (
        "/secure/pms/flb-calibration.csv"
    )
    assert settings.live_execution_model_path == (
        "/secure/pms/execution-model.json"
    )
    assert settings.live_paper_backtest_diff_path == (
        "/secure/pms/paper-backtest-execution-diff.json"
    )
    assert settings.live_exit_criteria_ratified_at is None
    assert settings.live_compliance_reviewed_at is None


def test_gitignore_excludes_operator_live_config_files() -> None:
    ignore_text = (ROOT / ".gitignore").read_text(encoding="utf-8")

    assert "config.live.yaml" in ignore_text
    assert "config.local*.yaml" in ignore_text
