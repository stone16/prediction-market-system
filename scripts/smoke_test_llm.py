"""LLM smoke test — verifies PMS can reach the configured provider end-to-end.

Loads PMSSettings exactly the way pms-api does (config.live-soak.yaml + .env
auto-discovery + process env), instantiates the same Anthropic SDK client that
LLMForecaster would, and sends a single 'reply with one word' probe. Prints
provider / model / base_url, the raw response, token usage, and the budget
that would gate a real soak run.

Usage::

    uv run python scripts/smoke_test_llm.py

Returns 0 on success, non-zero on auth / model / network failure with a
diagnostic stdout message. Intentionally bypasses MarketSignal and the
forecast contract so a failure here pinpoints the wire layer (key, base_url,
SDK install) rather than higher-level integration mistakes.
"""

from __future__ import annotations

from importlib import import_module
from typing import Any

from pms.config import PMSSettings


_PROBE_PROMPT = "Reply with the single word 'pong' and nothing else."
_DEFAULT_MAX_TOKENS = 1024  # Generous: reasoning models (DeepSeek V4-Pro,
# o1/o3-style) emit 100s of internal-thinking tokens before any visible text.


def _redact(value: str | None, *, head: int = 7, tail: int = 4) -> str:
    if not value:
        return "<unset>"
    if len(value) <= head + tail:
        return "<redacted>"
    return f"{value[:head]}...{value[-tail:]}"


def _print_settings(settings: PMSSettings) -> None:
    print("=" * 60)
    print("LLM settings (resolved):")
    print(f"  enabled                 : {settings.llm.enabled}")
    print(f"  provider                : {settings.llm.provider}")
    print(f"  model                   : {settings.llm.model!r}")
    print(f"  base_url                : {settings.llm.base_url!r}")
    print(f"  api_key                 : {_redact(settings.llm.api_key)}")
    print(f"  timeout_s               : {settings.llm.timeout_s}")
    print(f"  max_tokens              : {settings.llm.max_tokens}")
    print(f"  max_daily_llm_cost_usdc : ${settings.llm.max_daily_llm_cost_usdc}")
    print("=" * 60)


def _smoke_anthropic(settings: PMSSettings) -> int:
    try:
        anthropic = import_module("anthropic")
    except ImportError:
        print(
            "FAIL: anthropic SDK not installed. Run: "
            "uv sync --extra live --extra llm"
        )
        return 2

    if not callable(getattr(anthropic, "Anthropic", None)):
        print("FAIL: anthropic.Anthropic is not callable; SDK install is broken.")
        return 2

    kwargs: dict[str, Any] = {
        "api_key": settings.llm.api_key,
        "timeout": settings.llm.timeout_s,
    }
    if settings.llm.base_url:
        kwargs["base_url"] = settings.llm.base_url

    print("Constructing Anthropic SDK client with:")
    print(f"  base_url={kwargs.get('base_url', '<SDK default>')}")
    print(f"  timeout={kwargs['timeout']}s")
    print()

    client = anthropic.Anthropic(**kwargs)

    print(f"Sending probe prompt to model={settings.llm.model!r}...")
    print(f"  prompt: {_PROBE_PROMPT!r}")
    print()

    try:
        response = client.messages.create(
            model=settings.llm.model,
            max_tokens=_DEFAULT_MAX_TOKENS,
            messages=[{"role": "user", "content": _PROBE_PROMPT}],
        )
    except Exception as exc:  # noqa: BLE001 — diagnostic surface
        cls_name = exc.__class__.__name__
        print(f"FAIL: {cls_name}: {exc}")
        if "401" in str(exc) or "authentication" in str(exc).lower():
            print("  hint: check PMS_LLM__API_KEY; the server rejected it.")
        elif "404" in str(exc) or "model" in str(exc).lower():
            print(
                f"  hint: model {settings.llm.model!r} may not exist on this "
                f"provider. Check the model list at the provider's docs."
            )
        elif "base_url" in str(exc).lower() or "connect" in str(exc).lower():
            print(
                f"  hint: base_url={settings.llm.base_url!r} may be "
                f"unreachable. Try `curl {settings.llm.base_url}/v1/messages` "
                f"to confirm DNS + TLS."
            )
        return 1

    content = ""
    block_types: list[str] = []
    for block in response.content:
        block_types.append(type(block).__name__)
        if hasattr(block, "text"):
            content += block.text

    usage = response.usage
    input_tokens = usage.input_tokens
    output_tokens = usage.output_tokens

    print("=" * 60)
    print("OK — provider responded.")
    print(f"  response.id   : {response.id}")
    print(f"  response.model: {response.model}")
    print(f"  block types   : {block_types}")
    print(f"  text          : {content!r}")
    print(f"  input_tokens  : {input_tokens}")
    print(f"  output_tokens : {output_tokens}")
    print(f"  stop_reason   : {response.stop_reason}")
    print("=" * 60)

    if "pong" not in content.lower():
        print(
            "WARN: response does not contain 'pong'. The wire layer works "
            "but the model is verbose / not following the instruction."
        )
        return 0  # still a wire success
    return 0


def main() -> int:
    settings = PMSSettings.load("config.live-soak.yaml")
    _print_settings(settings)
    print()

    if not settings.llm.enabled:
        print("FAIL: llm.enabled is False; cannot smoke test.")
        return 2
    if settings.llm.provider != "anthropic":
        print(
            f"FAIL: this smoke harness only supports provider=anthropic "
            f"(saw {settings.llm.provider!r}). Use a different smoke for openai."
        )
        return 2
    if not settings.llm.api_key:
        print("FAIL: llm.api_key is empty.")
        return 2

    return _smoke_anthropic(settings)


if __name__ == "__main__":
    raise SystemExit(main())
