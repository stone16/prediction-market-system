from __future__ import annotations

import json
import os
from hashlib import sha256
from pathlib import Path
from typing import Any

import pytest

from pms.api.app import create_app
import pms.api.__main__ as api_main
from pms.core.enums import RunMode


def test_main_invokes_uvicorn_with_settings_host_and_cli_defaults(
    monkeypatch: Any,
) -> None:
    calls: dict[str, Any] = {}

    def fake_run(app: str, **kwargs: Any) -> None:
        calls["app"] = app
        calls.update(kwargs)

    monkeypatch.setattr("uvicorn.run", fake_run)
    monkeypatch.delenv("PMS_API_HOST", raising=False)
    monkeypatch.delenv("PMS_API_TOKEN", raising=False)

    exit_code = api_main.main([])

    assert exit_code == 0

    assert calls == {
        "app": "pms.api.app:create_app",
        "factory": True,
        "host": "127.0.0.1",
        "port": 8000,
        "log_level": "info",
        "reload": False,
    }


def test_main_refuses_non_loopback_bind_without_api_token(
    monkeypatch: Any,
    capsys: Any,
) -> None:
    calls: dict[str, Any] = {}

    def fake_run(app: str, **kwargs: Any) -> None:
        calls["app"] = app
        calls.update(kwargs)

    monkeypatch.setattr("uvicorn.run", fake_run)
    monkeypatch.setenv("PMS_API_HOST", "0.0.0.0")
    monkeypatch.delenv("PMS_API_TOKEN", raising=False)

    exit_code = api_main.main([])
    captured = capsys.readouterr()

    assert exit_code == 1
    assert calls == {}
    assert "PMS_API_TOKEN" in captured.err
    assert "PMS_API_HOST" in captured.err


def test_main_refuses_live_loopback_without_api_token(
    monkeypatch: Any,
    capsys: Any,
) -> None:
    calls: dict[str, Any] = {}

    def fake_run(app: str, **kwargs: Any) -> None:
        calls["app"] = app
        calls.update(kwargs)

    monkeypatch.setattr("uvicorn.run", fake_run)
    monkeypatch.setenv("PMS_MODE", "live")
    monkeypatch.setenv("PMS_API_HOST", "127.0.0.1")
    monkeypatch.delenv("PMS_API_TOKEN", raising=False)

    exit_code = api_main.main([])
    captured = capsys.readouterr()

    assert exit_code == 1
    assert calls == {}
    assert "PMS_API_TOKEN" in captured.err
    assert "live mode" in captured.err


def test_main_refuses_live_loopback_with_blank_api_token(
    monkeypatch: Any,
    capsys: Any,
) -> None:
    calls: dict[str, Any] = {}

    def fake_run(app: str, **kwargs: Any) -> None:
        calls["app"] = app
        calls.update(kwargs)

    monkeypatch.setattr("uvicorn.run", fake_run)
    monkeypatch.setenv("PMS_MODE", "live")
    monkeypatch.setenv("PMS_API_HOST", "127.0.0.1")
    monkeypatch.setenv("PMS_API_TOKEN", "   ")

    exit_code = api_main.main([])
    captured = capsys.readouterr()

    assert exit_code == 1
    assert calls == {}
    assert "PMS_API_TOKEN" in captured.err
    assert "live mode" in captured.err


def test_main_auto_start_requires_discord_webhook(
    monkeypatch: Any,
    capsys: Any,
) -> None:
    calls: dict[str, Any] = {}

    def fake_run(app: str, **kwargs: Any) -> None:
        calls["app"] = app
        calls.update(kwargs)

    monkeypatch.setattr("uvicorn.run", fake_run)
    monkeypatch.setenv("PMS_AUTO_START", "1")
    monkeypatch.delenv("PMS_DISCORD__WEBHOOK_URL", raising=False)

    exit_code = api_main.main([])
    captured = capsys.readouterr()

    assert exit_code == 1
    assert calls == {}
    assert "PMS_DISCORD__WEBHOOK_URL" in captured.err


def test_main_auto_start_rejects_blank_discord_webhook(
    monkeypatch: Any,
    capsys: Any,
) -> None:
    calls: dict[str, Any] = {}

    def fake_run(app: str, **kwargs: Any) -> None:
        calls["app"] = app
        calls.update(kwargs)

    monkeypatch.setattr("uvicorn.run", fake_run)
    monkeypatch.setenv("PMS_AUTO_START", "1")
    monkeypatch.setenv("PMS_DISCORD__WEBHOOK_URL", "   ")

    exit_code = api_main.main([])
    captured = capsys.readouterr()

    assert exit_code == 1
    assert calls == {}
    assert "PMS_DISCORD__WEBHOOK_URL" in captured.err


def test_main_redacts_invalid_discord_webhook_config_error(
    monkeypatch: Any,
    capsys: Any,
) -> None:
    calls: dict[str, Any] = {}
    webhook_secret = "super-secret-webhook-token"

    def fake_run(app: str, **kwargs: Any) -> None:
        calls["app"] = app
        calls.update(kwargs)

    monkeypatch.setattr("uvicorn.run", fake_run)
    monkeypatch.setenv(
        "PMS_DISCORD__WEBHOOK_URL",
        f"http://discord.com/api/webhooks/{webhook_secret}",
    )

    exit_code = api_main.main([])
    captured = capsys.readouterr()

    assert exit_code == 1
    assert calls == {}
    assert "config load failed" in captured.err
    assert "<redacted-discord-webhook-url>" in captured.err
    assert webhook_secret not in captured.err
    assert "discord.com/api/webhooks" not in captured.err


def test_main_uses_settings_host_even_when_host_flag_is_passed(
    monkeypatch: Any,
) -> None:
    calls: dict[str, Any] = {}

    def fake_run(app: str, **kwargs: Any) -> None:
        calls["app"] = app
        calls.update(kwargs)

    monkeypatch.setattr("uvicorn.run", fake_run)
    monkeypatch.setenv("PMS_API_HOST", "0.0.0.0")
    monkeypatch.setenv("PMS_API_TOKEN", "cp18-token")

    exit_code = api_main.main(["--host", "127.0.0.1", "--port", "9000"])

    assert exit_code == 0
    assert calls["host"] == "0.0.0.0"
    assert calls["port"] == 9000


def test_main_loads_config_file_and_exports_path_for_app_factory(
    monkeypatch: Any,
    tmp_path: Path,
) -> None:
    calls: dict[str, Any] = {}
    config_path = tmp_path / "live-soak.yaml"
    config_path.write_text(
        "\n".join(
            [
                "api_host: 127.0.0.1",
                "mode: paper",
                "risk:",
                "  max_total_exposure: 50.0",
            ]
        ),
        encoding="utf-8",
    )

    def fake_run(app: str, **kwargs: Any) -> None:
        calls["app"] = app
        calls.update(kwargs)

    monkeypatch.setattr("uvicorn.run", fake_run)
    monkeypatch.delenv("PMS_CONFIG_PATH", raising=False)

    exit_code = api_main.main(["--config", str(config_path)])

    assert exit_code == 0
    assert calls["host"] == "127.0.0.1"
    assert calls["app"] == "pms.api.app:create_app"
    assert calls["factory"] is True
    assert calls["reload"] is False
    assert calls["log_level"] == "info"
    assert calls["port"] == 8000
    assert os.environ["PMS_CONFIG_PATH"] == str(config_path)


def test_create_app_uses_config_path_for_default_runner(tmp_path: Path) -> None:
    config_path = tmp_path / "live-soak.yaml"
    config_path.write_text(
        "\n".join(
            [
                "mode: paper",
                "risk:",
                "  max_position_per_market: 5.0",
                "  max_total_exposure: 50.0",
            ]
        ),
        encoding="utf-8",
    )

    app = create_app(auto_start=False, config_path=str(config_path))

    assert app.state.runner.config.mode is RunMode.PAPER
    assert app.state.runner.config.risk.max_position_per_market == 5.0
    assert app.state.runner.config.risk.max_total_exposure == 50.0


def test_create_app_fails_closed_when_h1_flb_calibration_artifact_is_missing(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "live-soak.yaml"
    missing_calibration_path = tmp_path / "missing-flb-calibration.csv"
    config_path.write_text(
        "\n".join(
            [
                "mode: paper",
                "paper_soak_strategy_id: h1_flb",
                "strategies:",
                f"  flb_calibration_path: {missing_calibration_path}",
            ]
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="FLB calibration CSV does not exist"):
        create_app(auto_start=False, config_path=str(config_path))


def test_create_app_starts_with_staged_h1_flb_calibration_artifact(
    tmp_path: Path,
) -> None:
    calibration_path = tmp_path / "flb-calibration.csv"
    calibration_path.write_text(
        "\n".join(
            [
                "signal_name,probability_estimate,sample_count,source_label",
                "longshot_yes_overpriced_buy_no,0.99,150,warehouse-flb-v1",
                "favorite_yes_underpriced_buy_yes,0.97,151,warehouse-flb-v1",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    Path(f"{calibration_path}.provenance.json").write_text(
        json.dumps(
            {
                "artifact_type": "flb_calibration_provenance",
                "generated_by": "scripts/flb_data_feasibility.py",
                "source": "warehouse-csv",
                "generated_at": "2026-06-01T00:00:00+00:00",
                "warehouse_csv_sha256": sha256(
                    b"unit warehouse provenance fixture"
                ).hexdigest(),
                "warehouse_market_count": 301,
                "warehouse_longshot_count": 150,
                "warehouse_favorite_count": 151,
                "calibration_csv_sha256": sha256(
                    calibration_path.read_bytes()
                ).hexdigest(),
                "calibration_source_label": "warehouse-flb-v1",
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    config_path = tmp_path / "live-soak.yaml"
    config_path.write_text(
        "\n".join(
            [
                "mode: paper",
                "paper_soak_strategy_id: h1_flb",
                "strategies:",
                f"  flb_calibration_path: {calibration_path}",
            ]
        ),
        encoding="utf-8",
    )

    app = create_app(auto_start=False, config_path=str(config_path))

    assert app.state.runner.config.paper_soak_strategy_id == "h1_flb"
