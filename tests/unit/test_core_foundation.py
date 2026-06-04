from __future__ import annotations

import inspect
import os
from dataclasses import FrozenInstanceError, is_dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, get_type_hints

import pytest
from pydantic import SecretStr

from pms.config import (
    ControllerSettings,
    DiscordSettings,
    MissingPolymarketCredentialsError,
    PMSSettings,
    PolymarketSettings,
    RiskSettings,
    StrategyRuntimeSettings,
    validate_live_mode_ready,
)
from pms.core import interfaces, models
from pms.core.enums import (
    FeedbackSource,
    FeedbackTarget,
    MarketStatus,
    OrderStatus,
    RunMode,
    Side,
)
from tests.support.live_paths import make_live_report_paths, make_private_live_paths


def _utcnow() -> datetime:
    return datetime(2026, 4, 13, 12, 0, tzinfo=UTC)


def test_all_core_entities_are_frozen_dataclasses() -> None:
    entity_classes = [
        models.MarketSignal,
        models.Opportunity,
        models.TradeDecision,
        models.OrderState,
        models.FillRecord,
        models.Position,
        models.Portfolio,
        models.VenueCredentials,
        models.EvalRecord,
        models.Feedback,
    ]

    assert {entity.__name__ for entity in entity_classes} == {
        "MarketSignal",
        "Opportunity",
        "TradeDecision",
        "OrderState",
        "FillRecord",
        "Position",
        "Portfolio",
        "VenueCredentials",
        "EvalRecord",
        "Feedback",
    }
    for entity in entity_classes:
        assert is_dataclass(entity), entity.__name__
        assert getattr(entity, "__dataclass_params__").frozen, entity.__name__


def test_market_signal_is_immutable() -> None:
    signal = models.MarketSignal(
        market_id="pm-1",
        token_id="yes-token",
        venue="polymarket",
        title="Will it rain?",
        yes_price=0.42,
        volume_24h=1200.0,
        resolves_at=_utcnow(),
        orderbook={"bids": [], "asks": []},
        external_signal={},
        fetched_at=_utcnow(),
        market_status=MarketStatus.OPEN.value,
    )

    with pytest.raises(FrozenInstanceError):
        setattr(signal, "yes_price", 0.5)


def test_financial_entity_fields_use_float_boundary_types() -> None:
    expected: dict[type[Any], dict[str, object]] = {
        models.MarketSignal: {"yes_price": float, "volume_24h": float | None},
        models.TradeDecision: {
            "notional_usdc": float,
            "limit_price": float,
            "prob_estimate": float,
            "expected_edge": float,
            "risk_group_id": str | None,
        },
        models.Opportunity: {
            "expected_edge": float,
            "target_size_usdc": float,
        },
        models.OrderState: {
            "requested_notional_usdc": float,
            "filled_notional_usdc": float,
            "remaining_notional_usdc": float,
            "filled_quantity": float,
            "fill_price": float | None,
            "risk_group_id": str | None,
        },
        models.FillRecord: {
            "fill_price": float,
            "fill_notional_usdc": float,
            "fill_quantity": float,
            "fee_bps": int | None,
            "fees": float | None,
            "risk_group_id": str | None,
        },
        models.Position: {
            "shares_held": float,
            "avg_entry_price": float,
            "unrealized_pnl": float,
            "locked_usdc": float,
            "risk_group_id": str | None,
        },
        models.Portfolio: {
            "total_usdc": float,
            "free_usdc": float,
            "locked_usdc": float,
        },
        models.EvalRecord: {
            "prob_estimate": float,
            "baseline_prob_estimate": float | None,
            "resolved_outcome": float,
            "brier_score": float,
            "baseline_brier_score": float | None,
        },
    }

    for entity, fields in expected.items():
        hints = get_type_hints(entity)
        for field_name, expected_type in fields.items():
            assert hints[field_name] == expected_type


def test_float_decimal_rule_is_documented_on_core_models() -> None:
    docstring = inspect.getdoc(models)

    assert docstring is not None
    assert "float" in docstring
    assert "Decimal" in docstring
    assert "entity boundary" in docstring


def test_live_trading_disabled_error_is_runtime_error() -> None:
    assert issubclass(models.LiveTradingDisabledError, RuntimeError)


def test_venue_credentials_repr_redacts_secret_fields() -> None:
    credentials = models.VenueCredentials(
        venue="polymarket",
        host="https://clob.polymarket.com",
        private_key="private-key",
        api_key="api-key",
        api_secret="api-secret",
        api_passphrase="passphrase",
        api_key_id="api-key-id",
        private_key_pem="private-key-pem",
    )

    rendered = repr(credentials)

    assert "private-key" not in rendered
    assert "api-key" not in rendered
    assert "api-secret" not in rendered
    assert "passphrase" not in rendered
    assert "api-key-id" not in rendered
    assert "private-key-pem" not in rendered


def test_live_mode_validation_requires_all_polymarket_credentials() -> None:
    settings = PMSSettings(mode=RunMode.LIVE, live_trading_enabled=True)

    with pytest.raises(MissingPolymarketCredentialsError) as exc_info:
        validate_live_mode_ready(settings)

    message = str(exc_info.value)
    assert "Missing Polymarket credential fields" in message
    assert "private_key" in message
    assert "api_key" in message
    assert "api_secret" in message
    assert "api_passphrase" in message
    assert "signature_type" in message
    assert "funder_address" in message
    assert "private-key" not in message


def test_live_mode_validation_returns_redacted_credentials() -> None:
    attested_at = datetime.now(tz=UTC)
    approval_path, audit_path = make_private_live_paths(prefix="pms-core-live-")
    paper_report_path, rehearsal_report_path = make_live_report_paths(
        prefix="pms-core-live-reports-"
    )
    settings = PMSSettings(
        mode=RunMode.LIVE,
        secret_source="fly",
        live_trading_enabled=True,
        api_token="live-api-token",
        live_exit_criteria_ratified_by="operator",
        live_exit_criteria_ratified_at=attested_at,
        live_compliance_reviewed_by="counsel",
        live_compliance_reviewed_at=attested_at,
        live_compliance_jurisdiction="US-operator-approved",
        live_paper_soak_report_path=paper_report_path,
        live_operator_rehearsal_report_path=rehearsal_report_path,
        live_emergency_audit_path=str(
            Path(approval_path).parent / "live-emergency-audit.jsonl"
        ),
        live_first_order_audit_path=audit_path,
        live_preflight_artifact_path=str(
            Path(approval_path).parent / "credentialed-preflight.json"
        ),
        risk=RiskSettings(
            max_drawdown_pct=20.0,
            max_daily_loss_usdc=20.0,
            max_open_positions=5,
            max_exposure_per_risk_group=250.0,
            max_quantity_shares=500.0,
        ),
        controller=ControllerSettings(time_in_force="IOC", quote_source="dual"),
        discord=DiscordSettings(
            webhook_url=SecretStr("https://discord.example/webhooks/core/unit"),
            alert_dir=str(Path(approval_path).parent / "discord-alerts"),
        ),
        polymarket=PolymarketSettings(
            host="https://clob.polymarket.com",
            private_key="private-key",
            api_key="api-key",
            api_secret="api-secret",
            api_passphrase="passphrase",
            signature_type=1,
            funder_address="0x1111111111111111111111111111111111111111",
            operator_approval_mode="every_order",
            first_live_order_approval_path=approval_path,
        ),
    )

    credentials = validate_live_mode_ready(settings)

    assert credentials.venue == "polymarket"
    assert credentials.host == "https://clob.polymarket.com"
    assert credentials.private_key == "private-key"
    assert "private-key" not in repr(credentials)


def test_live_mode_loads_local_secret_file_before_env_credentials(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    secret_path = tmp_path / "polymarket.local-secrets.yaml"
    secret_path.write_text(
        "\n".join(
            [
                "polymarket:",
                "  private_key: file-private-key",
                "  api_key: file-api-key",
                "  api_secret: file-api-secret",
                "  api_passphrase: file-passphrase",
                "  signature_type: 1",
                "  funder_address: '0x2222222222222222222222222222222222222222'",
            ]
        ),
        encoding="utf-8",
    )
    secret_path.chmod(0o600)
    live_path_root = tmp_path / "live-paths"
    live_path_root.mkdir(mode=0o700)
    live_path_root.chmod(0o700)
    approval_path = live_path_root / "first-order.json"
    audit_path = live_path_root / "first-order-audit.jsonl"
    preflight_artifact_path = live_path_root / "credentialed-preflight.json"
    paper_report_path, rehearsal_report_path = make_live_report_paths(
        prefix="pms-core-secret-reports-"
    )
    config_path = tmp_path / "config.live.yaml"
    config_path.write_text(
        "\n".join(
            [
                    "mode: live",
                    "secret_source: local_file",
                    f"local_secret_file: {secret_path}",
                    "live_trading_enabled: true",
                    "api_token: live-api-token",
                    "live_exit_criteria_ratified_by: operator",
                f"live_exit_criteria_ratified_at: {datetime.now(tz=UTC).isoformat()}",
                "live_compliance_reviewed_by: counsel",
                f"live_compliance_reviewed_at: {datetime.now(tz=UTC).isoformat()}",
                "live_compliance_jurisdiction: US-operator-approved",
                f"live_paper_soak_report_path: {paper_report_path}",
                f"live_operator_rehearsal_report_path: {rehearsal_report_path}",
                f"live_emergency_audit_path: {live_path_root / 'live-emergency-audit.jsonl'}",
                f"live_first_order_audit_path: {audit_path}",
                f"live_preflight_artifact_path: {preflight_artifact_path}",
                "risk:",
                "  max_drawdown_pct: 20.0",
                "  max_daily_loss_usdc: 20.0",
                "  max_open_positions: 5",
                "  max_exposure_per_risk_group: 250.0",
                "  max_quantity_shares: 500.0",
                "controller:",
                "  time_in_force: IOC",
                "  quote_source: dual",
                "polymarket:",
                "  operator_approval_mode: every_order",
                f"  first_live_order_approval_path: {approval_path}",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("PMS_POLYMARKET__PRIVATE_KEY", "shell-private-key")
    monkeypatch.setenv(
        "PMS_DISCORD__WEBHOOK_URL",
        "https://discord.example/webhooks/core/local-secret",
    )
    monkeypatch.setenv(
        "PMS_DISCORD__ALERT_DIR",
        str(live_path_root / "discord-alerts"),
    )

    settings = PMSSettings.load(config_path)
    credentials = validate_live_mode_ready(settings)

    assert settings.secret_source == "local_file"
    assert settings.local_secret_file == str(secret_path)
    assert credentials.private_key == "file-private-key"
    assert credentials.api_key == "file-api-key"
    assert credentials.funder_address == "0x2222222222222222222222222222222222222222"


@pytest.mark.parametrize(
    ("config_lines", "expected_match"),
    (
        (
            ("polymarket:", "  first_live_order_approval_path: {config_path}"),
            "operator approval path",
        ),
        (("live_first_order_audit_path: {config_path}",), "first-order audit path"),
        (("live_emergency_audit_path: {config_path}",), "emergency audit path"),
        (
            ("live_preflight_artifact_path: {config_path}",),
            "preflight artifact path",
        ),
        (
            (
                "secret_source: local_file",
                "local_secret_file: {config_path}",
                "polymarket: {{}}",
            ),
            "local secret file",
        ),
        (("live_paper_soak_report_path: {config_path}",), "paper soak GO report"),
        (
            ("live_operator_rehearsal_report_path: {config_path}",),
            "operator rehearsal report",
        ),
        (("live_execution_model_path: {config_path}",), "execution-model artifact"),
        (
            ("live_paper_backtest_diff_path: {config_path}",),
            "paper-vs-backtest execution diff artifact",
        ),
        (
            ("controller:", "  category_prior_observations_path: {config_path}"),
            "category-prior artifact",
        ),
        (
            ("strategies:", "  flb_calibration_path: {config_path}"),
            "FLB calibration artifact",
        ),
        (
            (
                "discord:",
                "  webhook_url: https://discord.example/webhooks/unit/config",
                "  alert_dir: {config_path}",
            ),
            "discord alert directory",
        ),
    ),
)
def test_live_config_load_rejects_protected_path_reusing_config_file(
    tmp_path: Path,
    config_lines: tuple[str, ...],
    expected_match: str,
) -> None:
    config_path = tmp_path / "config.live.yaml"
    rendered_lines = [
        "mode: live",
        *(
            line.format(config_path=config_path)
            for line in config_lines
        ),
    ]
    config_path.write_text("\n".join(rendered_lines) + "\n", encoding="utf-8")
    if any(line.startswith("local_secret_file:") for line in rendered_lines):
        config_path.chmod(0o600)

    with pytest.raises(ValueError, match=expected_match):
        PMSSettings.load(config_path)


def test_live_config_load_rejects_env_protected_path_reusing_missing_config_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_path = tmp_path / "config.live.yaml"
    monkeypatch.setenv("PMS_MODE", "live")
    monkeypatch.setenv("PMS_LIVE_PREFLIGHT_ARTIFACT_PATH", str(config_path))

    with pytest.raises(ValueError, match="preflight artifact path"):
        PMSSettings.load(config_path)

    assert not config_path.exists()


def test_live_mode_opens_local_secret_file_with_no_follow_when_available(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    no_follow_flag = getattr(os, "O_NOFOLLOW", 0)
    if no_follow_flag == 0:
        pytest.skip("os.O_NOFOLLOW is unavailable on this platform")

    secret_path = tmp_path / "polymarket.local-secrets.yaml"
    secret_path.write_text(
        "\n".join(
            [
                "polymarket:",
                "  private_key: file-private-key",
                "  api_key: file-api-key",
                "  api_secret: file-api-secret",
                "  api_passphrase: file-passphrase",
                "  signature_type: 1",
                "  funder_address: '0x2222222222222222222222222222222222222222'",
            ]
        ),
        encoding="utf-8",
    )
    secret_path.chmod(0o600)
    config_path = tmp_path / "config.live.yaml"
    config_path.write_text(
        "\n".join(
            [
                "mode: live",
                "secret_source: local_file",
                f"local_secret_file: {secret_path}",
                "live_trading_enabled: true",
            ]
        ),
        encoding="utf-8",
    )
    observed: list[tuple[Path, int]] = []
    real_open = os.open

    def recording_open(
        path_arg: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        flags: int,
        mode: int = 0o777,
    ) -> int:
        observed.append((Path(os.fsdecode(os.fspath(path_arg))), flags))
        return real_open(path_arg, flags, mode)

    monkeypatch.setattr(os, "open", recording_open)

    settings = PMSSettings.load(config_path)

    observed_by_path = {path: flags for path, flags in observed}
    assert settings.polymarket.private_key == "file-private-key"
    assert observed_by_path[secret_path] & no_follow_flag


def test_live_mode_rejects_local_secret_file_hardlink_swap_during_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    secret_path = tmp_path / "polymarket.local-secrets.yaml"
    secret_path.write_text(
        "\n".join(
            [
                "polymarket:",
                "  private_key: file-private-key",
                "  api_key: file-api-key",
                "  api_secret: file-api-secret",
                "  api_passphrase: file-passphrase",
                "  signature_type: 1",
                "  funder_address: '0x2222222222222222222222222222222222222222'",
            ]
        ),
        encoding="utf-8",
    )
    secret_path.chmod(0o600)
    replacement_source = tmp_path / "replacement-source.yaml"
    replacement_source.write_text(
        "\n".join(
            [
                "polymarket:",
                "  private_key: swapped-private-key",
                "  api_key: swapped-api-key",
                "  api_secret: swapped-api-secret",
                "  api_passphrase: swapped-passphrase",
                "  signature_type: 1",
                "  funder_address: '0x3333333333333333333333333333333333333333'",
            ]
        ),
        encoding="utf-8",
    )
    replacement_source.chmod(0o600)
    config_path = tmp_path / "config.live.yaml"
    config_path.write_text(
        "\n".join(
            [
                "mode: live",
                "secret_source: local_file",
                f"local_secret_file: {secret_path}",
                "live_trading_enabled: true",
            ]
        ),
        encoding="utf-8",
    )
    real_open = os.open
    swapped = False

    def swapping_open(
        path_arg: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        flags: int,
        mode: int = 0o777,
    ) -> int:
        nonlocal swapped
        observed_path = Path(os.fsdecode(os.fspath(path_arg)))
        if observed_path == secret_path and not swapped:
            swapped = True
            secret_path.unlink()
            os.link(replacement_source, secret_path)
        return real_open(path_arg, flags, mode)

    monkeypatch.setattr(os, "open", swapping_open)

    with pytest.raises(ValueError):
        PMSSettings.load(config_path)

    assert swapped is True


def test_local_secret_file_must_not_be_group_or_world_readable(tmp_path: Path) -> None:
    secret_path = tmp_path / "polymarket.local-secrets.yaml"
    secret_path.write_text("polymarket: {}\n", encoding="utf-8")
    secret_path.chmod(0o644)
    config_path = tmp_path / "config.live.yaml"
    config_path.write_text(
        "\n".join(
            [
                "mode: live",
                "secret_source: local_file",
                f"local_secret_file: {secret_path}",
                "live_trading_enabled: true",
            ]
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="chmod 600"):
        PMSSettings.load(config_path)


def test_local_secret_file_inside_repo_is_rejected_before_loading(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    (repo_root / ".git").mkdir()
    secret_path = repo_root / "polymarket.local-secrets.yaml"
    secret_path.write_text("not a mapping\n", encoding="utf-8")
    secret_path.chmod(0o600)
    config_path = repo_root / "config.live.yaml"
    config_path.write_text(
        "\n".join(
            [
                "mode: live",
                "secret_source: local_file",
                f"local_secret_file: {secret_path}",
                "live_trading_enabled: true",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.chdir(repo_root)

    with pytest.raises(ValueError, match="outside the working tree"):
        PMSSettings.load(config_path)


def test_polymarket_credentials_must_not_be_inline_in_config_file(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "config.live.yaml"
    config_path.write_text(
        "\n".join(
            [
                "mode: live",
                "secret_source: fly",
                "live_trading_enabled: true",
                "polymarket:",
                "  private_key: inline-private-key",
                "  api_key: inline-api-key",
                "  api_secret: inline-api-secret",
                "  api_passphrase: inline-passphrase",
                "  signature_type: 1",
                "  funder_address: '0x1111111111111111111111111111111111111111'",
            ]
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Polymarket credential fields must not"):
        PMSSettings.load(config_path)


def test_llm_api_key_must_not_be_inline_in_config_file(tmp_path: Path) -> None:
    config_path = tmp_path / "config.live.yaml"
    config_path.write_text(
        "\n".join(
            [
                "mode: live",
                "llm:",
                "  enabled: true",
                "  provider: anthropic",
                "  api_key: inline-llm-key",
            ]
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="LLM api_key must not be set in config files"):
        PMSSettings.load(config_path)


def test_llm_api_key_null_placeholder_must_not_be_in_config_file(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "config.live.yaml"
    config_path.write_text(
        "\n".join(
            [
                "mode: live",
                "llm:",
                "  enabled: false",
                "  api_key: null",
            ]
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="LLM api_key must not be set in config files"):
        PMSSettings.load(config_path)


def test_core_enums_use_stable_wire_values() -> None:
    assert [mode.value for mode in RunMode] == ["backtest", "paper", "live"]
    assert [side.value for side in Side] == ["BUY", "SELL"]
    assert "live" in {status.value for status in OrderStatus}
    assert OrderStatus.CANCELED.value == OrderStatus.CANCELLED.value == "cancelled"
    assert "open" in {status.value for status in MarketStatus}
    assert [target.value for target in FeedbackTarget] == [
        "sensor",
        "controller",
        "actuator",
        "evaluator",
    ]
    assert [source.value for source in FeedbackSource] == [
        "actuator",
        "evaluator",
        "human",
    ]


def test_core_protocol_interfaces_are_declared() -> None:
    for protocol_name in (
        "ISensor",
        "IController",
        "IActuator",
        "IEvaluator",
        "IForecaster",
        "ICalibrator",
        "ISizer",
    ):
        protocol = getattr(interfaces, protocol_name)
        assert getattr(protocol, "_is_protocol", False), protocol_name

    assert "__aiter__" in interfaces.ISensor.__dict__
    assert "decide" in interfaces.IController.__dict__
    assert "execute" in interfaces.IActuator.__dict__
    assert "evaluate" in interfaces.IEvaluator.__dict__
    assert "forecast" in interfaces.IForecaster.__dict__
    assert "calibrate" in interfaces.ICalibrator.__dict__
    assert "size" in interfaces.ISizer.__dict__


def test_config_defaults_and_env_loading(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PMS_MODE", raising=False)
    monkeypatch.delenv("PMS_SECRET_SOURCE", raising=False)
    monkeypatch.delenv("PMS_CONTROLLER__DECISION_COOLDOWN_S", raising=False)
    monkeypatch.delenv("PMS_DATABASE__EXPIRED_DECISION_RETENTION_S", raising=False)
    monkeypatch.delenv("PMS_SENSOR__PERSIST_DISCOVERY_PRICE_SNAPSHOTS", raising=False)
    monkeypatch.delenv("PMS_SENSOR__PERSIST_PRICE_CHANGES", raising=False)
    default_settings = PMSSettings()

    assert default_settings.mode is RunMode.BACKTEST
    assert default_settings.secret_source is None
    assert default_settings.live_trading_enabled is False
    assert default_settings.risk.max_position_per_market == 100.0
    assert default_settings.controller.decision_cooldown_s == 60.0
    assert default_settings.database.expired_decision_retention_s == 24 * 60 * 60
    assert default_settings.sensor.persist_discovery_price_snapshots is False
    assert default_settings.sensor.persist_price_changes is False
    assert set(RiskSettings.model_fields) == {
        "max_position_per_market",
        "max_total_exposure",
        "max_drawdown_pct",
        "max_daily_loss_usdc",
        "max_open_positions",
        "max_exposure_per_risk_group",
        "min_order_usdc",
        "slippage_threshold_bps",
        "max_quantity_shares",
    }

    monkeypatch.setenv("PMS_MODE", "paper")
    monkeypatch.setenv("PMS_SECRET_SOURCE", "fly")
    monkeypatch.setenv("PMS_CONTROLLER__DECISION_COOLDOWN_S", "15")
    monkeypatch.setenv("PMS_DATABASE__EXPIRED_DECISION_RETENTION_S", "3600")
    monkeypatch.setenv("PMS_SENSOR__PERSIST_PRICE_CHANGES", "true")
    env_settings = PMSSettings()

    assert env_settings.mode is RunMode.PAPER
    assert env_settings.secret_source == "fly"
    assert env_settings.controller.decision_cooldown_s == 15.0
    assert env_settings.database.expired_decision_retention_s == 3600.0
    assert env_settings.sensor.persist_price_changes is True


def test_database_dsn_honours_database_url_override(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql://localhost/pms_override")

    settings = PMSSettings()

    assert settings.database.dsn == "postgresql://localhost/pms_override"


def test_strategy_runtime_settings_reject_non_finite_flb_costs() -> None:
    with pytest.raises(ValueError):
        StrategyRuntimeSettings(flb_entry_execution_cost_bps=float("inf"))

    with pytest.raises(ValueError):
        StrategyRuntimeSettings(flb_fee_rate=float("nan"))


def test_config_loads_optional_yaml_file(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "\n".join(
            [
                "mode: live",
                "live_trading_enabled: true",
                "polymarket:",
                "  host: https://clob.example.test",
                "risk:",
                "  max_position_per_market: 25.0",
            ]
        ),
        encoding="utf-8",
    )

    settings = PMSSettings.load(config_path)

    assert settings.mode is RunMode.LIVE
    assert settings.live_trading_enabled is True
    assert settings.polymarket.host == "https://clob.example.test"
    assert settings.risk.max_position_per_market == 25.0


def test_config_load_rejects_duplicate_yaml_key(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "\n".join(
            [
                "mode: paper",
                "mode: live",
            ]
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="duplicate YAML key: mode"):
        PMSSettings.load(config_path)


def test_config_load_rejects_duplicate_local_secret_yaml_key(
    tmp_path: Path,
) -> None:
    secret_path = tmp_path / "polymarket.local-secrets.yaml"
    secret_path.write_text(
        "\n".join(
            [
                "polymarket:",
                "  private_key: forged-private-key",
                "  private_key: file-private-key",
                "  api_key: file-api-key",
                "  api_secret: file-api-secret",
                "  api_passphrase: file-passphrase",
                "  signature_type: 1",
                "  funder_address: '0x2222222222222222222222222222222222222222'",
            ]
        ),
        encoding="utf-8",
    )
    secret_path.chmod(0o600)
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "\n".join(
            [
                "secret_source: local_file",
                f"local_secret_file: {secret_path}",
            ]
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="duplicate YAML key: private_key"):
        PMSSettings.load(config_path)


def test_config_load_rejects_symlink_config_file(tmp_path: Path) -> None:
    target_path = tmp_path / "target-config.yaml"
    target_path.write_text("mode: live\n", encoding="utf-8")
    config_path = tmp_path / "config.yaml"
    config_path.symlink_to(target_path)

    with pytest.raises(ValueError, match="Config file cannot be read safely"):
        PMSSettings.load(config_path)


def test_config_load_opens_config_file_with_no_follow_when_available(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    no_follow_flag = getattr(os, "O_NOFOLLOW", 0)
    if no_follow_flag == 0:
        pytest.skip("os.O_NOFOLLOW is unavailable on this platform")

    config_path = tmp_path / "config.yaml"
    config_path.write_text("mode: paper\n", encoding="utf-8")
    observed: list[tuple[Path, int]] = []
    real_open = os.open

    def recording_open(
        path_arg: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        flags: int,
        mode: int = 0o777,
    ) -> int:
        observed.append((Path(os.fsdecode(os.fspath(path_arg))), flags))
        return real_open(path_arg, flags, mode)

    monkeypatch.setattr(os, "open", recording_open)

    settings = PMSSettings.load(config_path)

    observed_by_path = {path: flags for path, flags in observed}
    assert settings.mode is RunMode.PAPER
    assert observed_by_path[config_path] & no_follow_flag


def test_config_load_rejects_config_file_hardlink_swap_during_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text("mode: paper\n", encoding="utf-8")
    replacement_source = tmp_path / "replacement-config.yaml"
    replacement_source.write_text("mode: live\n", encoding="utf-8")
    real_open = os.open
    swapped = False

    def swapping_open(
        path_arg: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        flags: int,
        mode: int = 0o777,
    ) -> int:
        nonlocal swapped
        observed_path = Path(os.fsdecode(os.fspath(path_arg)))
        if observed_path == config_path and not swapped:
            swapped = True
            config_path.unlink()
            os.link(replacement_source, config_path)
        return real_open(path_arg, flags, mode)

    monkeypatch.setattr(os, "open", swapping_open)

    with pytest.raises(ValueError, match="Config file cannot be read safely"):
        PMSSettings.load(config_path)

    assert swapped is True
