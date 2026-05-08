"""Tests for ``.env`` auto-loading by ``PMSSettings``.

The PMS pydantic-settings model is configured with ``env_file=".env"`` so that
operators can keep LLM / dashboard / database overrides in a single per-checkout
file instead of polluting the global shell. These tests pin that wiring so a
future config refactor cannot silently regress it (which would re-introduce the
"why is paper soak running with zero alpha" failure mode).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from pms.config import PMSSettings


ROOT = Path(__file__).resolve().parents[2]


def test_dotenv_supplies_llm_api_key_when_yaml_omits_it(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text("PMS_LLM__API_KEY=sk-from-dotenv-file\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("PMS_LLM__API_KEY", raising=False)

    settings = PMSSettings.load(ROOT / "config.live-soak.yaml")

    assert settings.llm.api_key == "sk-from-dotenv-file"
    assert settings.llm.enabled is True
    assert settings.llm.provider == "anthropic"


def test_dotenv_can_set_alternative_provider_and_base_url(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Operators using DeepSeek / OpenRouter / etc. can override base_url + model."""
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            [
                "PMS_LLM__API_KEY=sk-deepseek-test",
                "PMS_LLM__MODEL=DeepSeek-V4-Pro",
                "PMS_LLM__BASE_URL=https://api.deepseek.com/anthropic",
                "",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    for var in ("PMS_LLM__API_KEY", "PMS_LLM__MODEL", "PMS_LLM__BASE_URL"):
        monkeypatch.delenv(var, raising=False)

    settings = PMSSettings.load(ROOT / "config.live-soak.yaml")

    assert settings.llm.api_key == "sk-deepseek-test"
    assert settings.llm.model == "DeepSeek-V4-Pro"
    assert settings.llm.base_url == "https://api.deepseek.com/anthropic"


def test_real_env_var_overrides_dotenv(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Process env wins over .env, matching pydantic-settings precedence."""
    env_file = tmp_path / ".env"
    env_file.write_text("PMS_LLM__API_KEY=sk-from-dotenv\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("PMS_LLM__API_KEY", "sk-from-process-env")

    settings = PMSSettings.load(ROOT / "config.live-soak.yaml")

    assert settings.llm.api_key == "sk-from-process-env"


def test_missing_dotenv_is_not_an_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Operators without a .env file should still get a clean config load,
    provided they supply credentials another way (env var / CI secret)."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("PMS_LLM__API_KEY", "sk-no-dotenv-needed")

    settings = PMSSettings.load(ROOT / "config.live-soak.yaml")

    assert settings.llm.api_key == "sk-no-dotenv-needed"
