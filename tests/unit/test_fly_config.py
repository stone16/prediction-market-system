from __future__ import annotations

import os
import tomllib
from pathlib import Path

import pytest

from pms.config import PMSSettings
from pms.core.enums import RunMode


def test_fly_binds_api_host_via_env_and_checks_readiness() -> None:
    fly_config = tomllib.loads(Path("fly.toml").read_text(encoding="utf-8"))

    assert fly_config["env"]["PMS_API_HOST"] == "0.0.0.0"
    assert fly_config["env"]["PMS_CONFIG_PATH"] == "/app/config.live-soak.yaml"
    assert fly_config["mounts"]["destination"] == "/secure"
    assert fly_config["http_service"]["checks"][0]["path"] == "/readiness"


def test_docker_cmd_uses_pms_api_host_env_not_deprecated_host_flag() -> None:
    dockerfile = Path("Dockerfile").read_text(encoding="utf-8")

    assert "--host" not in dockerfile
    assert "pms-api" in dockerfile
    assert "COPY alembic.ini ./alembic.ini" in dockerfile
    assert "COPY alembic ./alembic" in dockerfile


def test_docker_image_installs_live_soak_optional_dependencies_once() -> None:
    dockerfile = Path("Dockerfile").read_text(encoding="utf-8")

    assert "RUN uv sync --frozen --no-dev --extra live --extra llm" in dockerfile
    assert '"--no-sync"' in dockerfile


def test_docker_cmd_delegates_config_selection_to_fly_env() -> None:
    dockerfile = Path("Dockerfile").read_text(encoding="utf-8")
    cmd_section = dockerfile.split("CMD", maxsplit=1)[1]

    assert '"--config"' not in cmd_section
    assert 'CMD ["uv", "run", "--no-sync", "--no-dev", "pms-api", "--port", "8000"]' in dockerfile


def test_fly_live_capital_template_is_separate_from_paper_soak_app() -> None:
    paper_config = tomllib.loads(Path("fly.toml").read_text(encoding="utf-8"))
    live_config = tomllib.loads(
        Path("fly.live.toml.example").read_text(encoding="utf-8")
    )
    live_env = live_config["env"]

    assert live_config["app"] != paper_config["app"]
    assert live_env["PMS_MODE"] == "live"
    assert live_env["PMS_SECRET_SOURCE"] == "fly"
    assert live_env["PMS_LIVE_TRADING_ENABLED"] == "true"
    assert live_env["PMS_LIVE_ACCOUNT_RECONCILIATION_REQUIRED"] == "true"
    assert live_env["PMS_LIVE_PAPER_SOAK_REPORT_PATH"] == (
        "/secure/pms/paper-soak-go-report.md"
    )
    assert live_env["PMS_AUTO_START"] == "1"
    assert live_env["PMS_API_HOST"] == "0.0.0.0"
    assert live_env["PMS_CONTROLLER__TIME_IN_FORCE"] == "IOC"
    assert live_env["PMS_CONTROLLER__STRICT_FACTOR_GATES"] == "true"
    assert live_env["PMS_CONTROLLER__QUOTE_SOURCE"] == "dual"
    assert live_env["PMS_CONTROLLER__CATEGORY_PRIOR_OBSERVATIONS_PATH"] == (
        "/secure/pms/category-prior-observations.csv"
    )
    assert live_env["PMS_CONTROLLER__CATEGORY_PRIOR_MIN_GLOBAL_SAMPLES"] == "100"
    assert live_env["PMS_STRATEGIES__FLB_CALIBRATION_PATH"] == (
        "/secure/pms/flb-calibration.csv"
    )
    assert live_env["PMS_STRATEGIES__FLB_MIN_CALIBRATION_SAMPLES"] == "100"
    assert live_env["PMS_STRATEGIES__FLB_ENTRY_EXECUTION_COST_BPS"] == "15.0"
    assert live_env["PMS_STRATEGIES__FLB_FEE_RATE"] == "0.07"
    assert live_env["PMS_POLYMARKET__OPERATOR_APPROVAL_MODE"] == "every_order"
    assert live_env["PMS_LLM__ENABLED"] == "false"
    assert "PMS_CONFIG_PATH" not in live_env
    assert "config.live-soak.yaml" not in Path("fly.live.toml.example").read_text(
        encoding="utf-8"
    )
    assert live_config["http_service"]["checks"][0]["path"] == "/readiness"


def test_fly_live_capital_template_keeps_secrets_out_of_toml() -> None:
    live_config = tomllib.loads(
        Path("fly.live.toml.example").read_text(encoding="utf-8")
    )
    live_env = live_config["env"]

    forbidden_keys = {
        "DATABASE_URL",
        "PMS_API_TOKEN",
        "PMS_DISCORD__WEBHOOK_URL",
        "PMS_POLYMARKET__PRIVATE_KEY",
        "PMS_POLYMARKET__API_KEY",
        "PMS_POLYMARKET__API_SECRET",
        "PMS_POLYMARKET__API_PASSPHRASE",
        "PMS_POLYMARKET__SIGNATURE_TYPE",
        "PMS_POLYMARKET__FUNDER_ADDRESS",
        "PMS_LLM__API_KEY",
    }

    assert forbidden_keys.isdisjoint(live_env)


def test_fly_live_capital_template_env_values_parse_as_live_settings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    live_config = tomllib.loads(
        Path("fly.live.toml.example").read_text(encoding="utf-8")
    )
    live_env: dict[str, str] = dict(live_config["env"])
    live_env.update(
        {
            "PMS_LIVE_EXIT_CRITERIA_RATIFIED_BY": "operator",
            "PMS_LIVE_EXIT_CRITERIA_RATIFIED_AT": "2026-05-25T00:00:00+00:00",
            "PMS_LIVE_COMPLIANCE_REVIEWED_BY": "counsel",
            "PMS_LIVE_COMPLIANCE_REVIEWED_AT": "2026-05-25T00:00:00+00:00",
            "PMS_LIVE_COMPLIANCE_JURISDICTION": "US-operator-approved",
            "PMS_POLYMARKET__PRIVATE_KEY": "private-key",
            "PMS_POLYMARKET__API_KEY": "api-key",
            "PMS_POLYMARKET__API_SECRET": "api-secret",
            "PMS_POLYMARKET__API_PASSPHRASE": "passphrase",
            "PMS_POLYMARKET__SIGNATURE_TYPE": "1",
            "PMS_POLYMARKET__FUNDER_ADDRESS": (
                "0x1111111111111111111111111111111111111111"
            ),
        }
    )

    for key in tuple(os.environ):
        if key.startswith("PMS_") or key == "DATABASE_URL":
            monkeypatch.delenv(key, raising=False)
    for key, value in live_env.items():
        monkeypatch.setenv(key, value)

    settings = PMSSettings.load(None)

    assert settings.mode is RunMode.LIVE
    assert settings.secret_source == "fly"
    assert settings.live_trading_enabled is True
    assert settings.live_account_reconciliation_required is True
    assert settings.risk.max_total_exposure == 50.0
    assert settings.risk.max_daily_loss_usdc == 20.0
    assert settings.controller.time_in_force == "IOC"
    assert settings.controller.strict_factor_gates is True
    assert settings.controller.quote_source == "dual"
    assert settings.controller.category_prior_observations_path == (
        "/secure/pms/category-prior-observations.csv"
    )
    assert settings.live_paper_soak_report_path == (
        "/secure/pms/paper-soak-go-report.md"
    )
    assert settings.live_execution_model_path == "/secure/pms/execution-model.json"
    assert settings.live_paper_backtest_diff_path == (
        "/secure/pms/paper-backtest-execution-diff.json"
    )
    assert settings.strategies.flb_calibration_path == (
        "/secure/pms/flb-calibration.csv"
    )
    assert settings.strategies.flb_entry_execution_cost_bps == 15.0
    assert settings.strategies.flb_fee_rate == 0.07
    assert settings.polymarket.operator_approval_mode == "every_order"
