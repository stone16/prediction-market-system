from __future__ import annotations

import json
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from importlib import import_module
from typing import Protocol, Self, cast

from pms.config import LLMSettings
from pms.core.models import MarketSignal


class _LLMClient(Protocol):
    """Marker Protocol — the concrete shape is provider-specific."""


class LLMTimeoutError(RuntimeError):
    """Wraps SDK timeout errors. Caught by predict() and downgraded to None."""


class LLMTransientError(RuntimeError):
    """Wraps SDK transient errors (rate limit / 5xx / network)."""


class LLMParseError(RuntimeError):
    """Raised when LLM response JSON cannot be parsed or is missing keys."""


class LLMForecastResult(tuple[float, float, str]):
    model_id: str

    def __new__(
        cls,
        prob_estimate: float,
        confidence: float,
        rationale: str,
        model_id: str,
    ) -> Self:
        instance = super().__new__(cls, (prob_estimate, confidence, rationale))
        instance.model_id = model_id
        return instance


@dataclass
class LLMForecaster:
    config: LLMSettings | None = None
    client: object | None = None
    _cache: dict[str, tuple[float, LLMForecastResult]] = field(
        default_factory=dict
    )
    _cache_lock: threading.Lock = field(default_factory=threading.Lock)

    def __post_init__(self) -> None:
        if self.config is None:
            self.config = LLMSettings()

    def predict(self, signal: MarketSignal) -> LLMForecastResult | None:
        if self.config is None or not self.config.enabled:
            return None
        cached = self._cache_get(signal.market_id)
        if cached is not None:
            return cached
        client = self._client()
        if client is None:
            return None
        try:
            raw = self._call(client, signal)
            result = self._parse(raw, signal)
        except (LLMTimeoutError, LLMTransientError, LLMParseError):
            return None
        self._cache_put(signal.market_id, result)
        return result

    async def forecast(self, signal: MarketSignal) -> float:
        import asyncio

        result = await asyncio.to_thread(self.predict, signal)
        return signal.yes_price if result is None else result[0]

    # --- private ---

    def _client(self) -> _LLMClient | None:
        if self.client is not None:
            return cast(_LLMClient, self.client)  # type: ignore[redundant-cast]
        if self.config is None or self.config.provider is None:
            return None
        if self.config.provider == "anthropic":
            try:
                anthropic_module = import_module("anthropic")
            except ImportError:
                return None
            client_factory = getattr(anthropic_module, "Anthropic", None)
            if not callable(client_factory):
                return None
            factory = cast(Callable[..., object], client_factory)
            kwargs: dict[str, object] = {
                "api_key": self.config.api_key,
                "timeout": self.config.timeout_s,
            }
            if self.config.base_url:
                kwargs["base_url"] = self.config.base_url
            self.client = factory(**kwargs)
        else:
            try:
                openai_module = import_module("openai")
            except ImportError:
                return None
            client_factory = getattr(openai_module, "OpenAI", None)
            if not callable(client_factory):
                return None
            factory = cast(Callable[..., object], client_factory)
            self.client = factory(
                api_key=self.config.api_key,
                base_url=self.config.base_url,
                timeout=self.config.timeout_s,
            )
        return cast(_LLMClient, self.client)  # type: ignore[redundant-cast]

    def _call(self, client: _LLMClient, signal: MarketSignal) -> str:
        assert self.config is not None  # narrowed by predict() pre-check
        if self.config.provider == "anthropic":
            return self._call_anthropic(client, signal)
        if self.config.provider == "openai":
            return self._call_openai(client, signal)
        raise LLMTransientError(f"unknown provider: {self.config.provider}")

    def _call_anthropic(
        self, client: _LLMClient, signal: MarketSignal
    ) -> str:
        assert self.config is not None
        try:
            response = cast(object, client).messages.create(  # type: ignore[attr-defined,redundant-cast]
                model=self.config.model,
                max_tokens=self.config.max_tokens,
                messages=[
                    {"role": "user", "content": _prompt(signal)}
                ],
            )
        except (LLMTimeoutError, LLMTransientError, LLMParseError):
            raise
        except Exception as exc:
            anthropic_module = _safe_import("anthropic")
            if anthropic_module is not None:
                if isinstance(
                    exc,
                    getattr(anthropic_module, "APITimeoutError", ()),
                ):
                    raise LLMTimeoutError(str(exc)) from exc
                if isinstance(
                    exc,
                    (
                        getattr(anthropic_module, "RateLimitError", ()),
                        getattr(anthropic_module, "APIConnectionError", ()),
                        getattr(anthropic_module, "InternalServerError", ()),
                    ),
                ):
                    raise LLMTransientError(str(exc)) from exc
            raise
        return _response_text_anthropic(response)

    def _call_openai(self, client: _LLMClient, signal: MarketSignal) -> str:
        assert self.config is not None
        try:
            response = cast(object, client).chat.completions.create(  # type: ignore[attr-defined,redundant-cast]
                model=self.config.model,
                messages=[
                    {"role": "user", "content": _prompt(signal)}
                ],
                max_tokens=self.config.max_tokens,
                response_format={"type": "json_object"},
            )
        except (LLMTimeoutError, LLMTransientError, LLMParseError):
            raise
        except Exception as exc:
            openai_module = _safe_import("openai")
            if openai_module is not None:
                if isinstance(
                    exc,
                    getattr(openai_module, "APITimeoutError", ()),
                ):
                    raise LLMTimeoutError(str(exc)) from exc
                if isinstance(
                    exc,
                    (
                        getattr(openai_module, "RateLimitError", ()),
                        getattr(openai_module, "APIConnectionError", ()),
                        getattr(openai_module, "InternalServerError", ()),
                    ),
                ):
                    raise LLMTransientError(str(exc)) from exc
            raise
        return _response_text_openai(response)

    def _parse(self, raw: str, signal: MarketSignal) -> LLMForecastResult:
        try:
            loaded = _load_json(raw)
        except (json.JSONDecodeError, ValueError) as exc:
            raise LLMParseError(f"unparseable LLM response: {exc}") from exc
        for key in ("prob_estimate", "confidence", "rationale"):
            if key not in loaded:
                raise LLMParseError(
                    f"LLM response missing required key: {key}"
                )
        try:
            prob = _clamp(_as_float(loaded["prob_estimate"]))
            conf = _clamp(_as_float(loaded["confidence"]))
        except (TypeError, ValueError) as exc:
            raise LLMParseError(f"non-numeric LLM field: {exc}") from exc
        rationale = str(loaded["rationale"])
        assert self.config is not None
        return LLMForecastResult(
            prob_estimate=prob,
            confidence=conf,
            rationale=rationale,
            model_id=self.config.model,
        )

    def _cache_get(self, market_id: str) -> LLMForecastResult | None:
        if self.config is None or self.config.cache_ttl_s <= 0:
            return None
        with self._cache_lock:
            entry = self._cache.get(market_id)
            if entry is None:
                return None
            ts, result = entry
            if time.monotonic() - ts > self.config.cache_ttl_s:
                del self._cache[market_id]
                return None
            return result

    def _cache_put(
        self, market_id: str, result: LLMForecastResult
    ) -> None:
        if self.config is None or self.config.cache_ttl_s <= 0:
            return
        with self._cache_lock:
            self._cache[market_id] = (time.monotonic(), result)
            if len(self._cache) > 1000:
                oldest = min(
                    self._cache, key=lambda k: self._cache[k][0]
                )
                del self._cache[oldest]


# --- module-level helpers (kept module-level so monkeypatch works) ---


def _safe_import(name: str) -> object | None:
    try:
        return import_module(name)
    except ImportError:
        return None


def _prompt(signal: MarketSignal) -> str:
    orderbook = {
        "bids": _top_five(signal.orderbook.get("bids")),
        "asks": _top_five(signal.orderbook.get("asks")),
    }
    return "\n".join(
        [
            f"market_title: {signal.title}",
            f"market_id: {signal.market_id}",
            f"venue: {signal.venue}",
            f"yes_price: {signal.yes_price}",
            "orderbook_top_5: "
            + json.dumps(orderbook, sort_keys=True, separators=(",", ":")),
            "external_signal: "
            + json.dumps(
                signal.external_signal, sort_keys=True, default=str
            ),
            (
                "Respond with a JSON object only. No prose. "
                "Keys: prob_estimate (0..1 float), confidence (0..1 float), "
                "rationale (one short sentence)."
            ),
        ]
    )


def _top_five(levels: object) -> object:
    if isinstance(levels, list):
        return levels[:5]
    return []


def _load_json(text: str) -> dict[str, object]:
    try:
        loaded = json.loads(text)
    except json.JSONDecodeError:
        start = text.find("{")
        end = text.rfind("}")
        if start == -1 or end == -1 or start > end:
            raise
        loaded = json.loads(text[start : end + 1])
    if not isinstance(loaded, dict):
        raise ValueError("expected JSON object")
    return loaded


def _response_text_anthropic(response: object) -> str:
    content = getattr(response, "content", "")
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for block in content:
            text = getattr(block, "text", None)
            if isinstance(block, dict):
                text = block.get("text")
            if isinstance(text, str):
                parts.append(text)
        return "\n".join(parts)
    return str(content)


def _response_text_openai(response: object) -> str:
    choices = getattr(response, "choices", None)
    if not choices:
        return ""
    message = getattr(choices[0], "message", None)
    content = getattr(message, "content", "") if message is not None else ""
    return content if isinstance(content, str) else str(content)


def _as_float(value: object) -> float:
    if isinstance(value, str | int | float):
        return float(value)
    raise ValueError(f"Expected numeric value, got {type(value).__name__}")


def _clamp(value: float) -> float:
    return min(max(value, 0.0), 1.0)
