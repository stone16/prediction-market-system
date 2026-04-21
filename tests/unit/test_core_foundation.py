from __future__ import annotations

import inspect
from dataclasses import FrozenInstanceError, is_dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, get_type_hints

import pytest

from pms.config import PMSSettings, RiskSettings
from pms.core import interfaces, models
from pms.core.enums import (
    FeedbackSource,
    FeedbackTarget,
    MarketStatus,
    OrderStatus,
    RunMode,
    Side,
)


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
        },
        models.FillRecord: {
            "fill_price": float,
            "fill_notional_usdc": float,
            "fill_quantity": float,
            "fee_bps": int | None,
            "fees": float | None,
        },
        models.Position: {
            "shares_held": float,
            "avg_entry_price": float,
            "unrealized_pnl": float,
            "locked_usdc": float,
        },
        models.Portfolio: {
            "total_usdc": float,
            "free_usdc": float,
            "locked_usdc": float,
        },
        models.EvalRecord: {
            "prob_estimate": float,
            "resolved_outcome": float,
            "brier_score": float,
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


def test_core_enums_use_stable_wire_values() -> None:
    assert [mode.value for mode in RunMode] == ["backtest", "paper", "live"]
    assert [side.value for side in Side] == ["BUY", "SELL"]
    assert "live" in {status.value for status in OrderStatus}
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
    default_settings = PMSSettings()

    assert default_settings.mode is RunMode.BACKTEST
    assert default_settings.live_trading_enabled is False
    assert default_settings.risk.max_position_per_market == 100.0
    assert set(RiskSettings.model_fields) == {
        "max_position_per_market",
        "max_total_exposure",
        "max_drawdown_pct",
        "max_open_positions",
        "min_order_usdc",
        "slippage_threshold_bps",
    }

    monkeypatch.setenv("PMS_MODE", "paper")
    env_settings = PMSSettings()

    assert env_settings.mode is RunMode.PAPER


def test_database_dsn_honours_database_url_override(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql://localhost/pms_override")

    settings = PMSSettings()

    assert settings.database.dsn == "postgresql://localhost/pms_override"


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
