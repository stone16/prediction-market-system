from __future__ import annotations

import json
import logging
import math
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from decimal import Decimal
from importlib import import_module
from typing import Protocol, Self, cast
from weakref import ReferenceType, ref

from pms.config import LLMSettings
from pms.core.models import MarketSignal

logger = logging.getLogger(__name__)

_SYSTEM_PROMPT = (
    "You are a calibrated prediction-market forecaster. Estimate the true "
    "resolution probability, not the current market price. Use the market, "
    "orderbook, and external signals as evidence. Return valid JSON only with "
    "prob_estimate, confidence, and rationale."
)
_ESTIMATED_CALL_COST_USDC = Decimal("0.002")


@dataclass
class _BudgetTracker:
    lock: threading.Lock = field(default_factory=threading.Lock)
    cost_day: date | None = None
    daily_cost_usdc: Decimal = Decimal("0")


_BUDGET_TRACKERS: dict[int, tuple[ReferenceType[LLMSettings], _BudgetTracker]] = {}
_BUDGET_TRACKERS_LOCK = threading.Lock()


class _LLMClient(Protocol):
    """Marker protocol; concrete SDK shapes are provider-specific."""


class LLMTimeoutError(RuntimeError):
    """Wraps SDK timeout errors. predict() downgrades these to None."""


class LLMTransientError(RuntimeError):
    """Wraps SDK transient errors such as rate limit, 5xx, and network errors."""


class LLMParseError(RuntimeError):
    """Raised when a provider response cannot satisfy the forecast contract."""


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
    _cache: dict[str, tuple[float, LLMForecastResult]] = field(default_factory=dict)
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
        estimated_cost = self._estimated_call_cost_usdc()
        if not self._reserve_budget(estimated_cost):
            logger.info(
                "llm_forecaster_budget_exhausted",
                extra={
                    "market_id": signal.market_id,
                    "provider": self.config.provider,
                    "model": self.config.model,
                    "estimated_cost_usdc": float(estimated_cost),
                    "daily_cost_usdc": self._daily_cost_usdc_float(),
                    "max_daily_llm_cost_usdc": self.config.max_daily_llm_cost_usdc,
                },
            )
            return None
        try:
            client = self._client()
        except Exception:
            self._refund_budget(estimated_cost)
            raise
        if client is None:
            self._refund_budget(estimated_cost)
            return None
        try:
            raw = self._call(client, signal)
        except (LLMTimeoutError, LLMTransientError):
            self._refund_budget(estimated_cost)
            return None
        except Exception:
            self._refund_budget(estimated_cost)
            raise
        self._record_cost(estimated_cost, signal)
        try:
            result = self._parse(raw)
        except LLMParseError:
            return None
        self._cache_put(signal.market_id, result)
        return result

    async def forecast(self, signal: MarketSignal) -> float:
        import asyncio

        result = await asyncio.to_thread(self.predict, signal)
        return signal.yes_price if result is None else result[0]

    def _client(self) -> object | None:
        if self.client is not None:
            return self.client
        if self.config is None or self.config.provider is None:
            return None
        if self.config.provider == "anthropic":
            self.client = self._anthropic_client()
        else:
            self.client = self._openai_client()
        return self.client

    def _anthropic_client(self) -> object | None:
        assert self.config is not None
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
        return factory(**kwargs)

    def _openai_client(self) -> object | None:
        assert self.config is not None
        try:
            openai_module = import_module("openai")
        except ImportError:
            return None
        client_factory = getattr(openai_module, "OpenAI", None)
        if not callable(client_factory):
            return None
        factory = cast(Callable[..., object], client_factory)
        return factory(
            api_key=self.config.api_key,
            base_url=self.config.base_url,
            timeout=self.config.timeout_s,
        )

    def _call(self, client: object, signal: MarketSignal) -> str:
        assert self.config is not None
        if self.config.provider == "anthropic":
            return self._call_anthropic(client, signal)
        if self.config.provider == "openai":
            return self._call_openai(client, signal)
        raise LLMTransientError(f"unknown provider: {self.config.provider}")

    def _call_anthropic(self, client: object, signal: MarketSignal) -> str:
        assert self.config is not None
        try:
            response = getattr(client, "messages").create(
                model=self.config.model,
                max_tokens=self.config.max_tokens,
                system=_SYSTEM_PROMPT,
                messages=[{"role": "user", "content": _prompt(signal)}],
            )
        except (LLMTimeoutError, LLMTransientError, LLMParseError):
            raise
        except Exception as exc:
            anthropic_module = _safe_import("anthropic")
            if anthropic_module is not None:
                if isinstance(exc, getattr(anthropic_module, "APITimeoutError", ())):
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

    def _call_openai(self, client: object, signal: MarketSignal) -> str:
        assert self.config is not None
        try:
            response = getattr(client, "chat").completions.create(
                model=self.config.model,
                messages=[
                    {"role": "system", "content": _SYSTEM_PROMPT},
                    {"role": "user", "content": _prompt(signal)},
                ],
                max_tokens=self.config.max_tokens,
                response_format={"type": "json_object"},
            )
        except (LLMTimeoutError, LLMTransientError, LLMParseError):
            raise
        except Exception as exc:
            openai_module = _safe_import("openai")
            if openai_module is not None:
                if isinstance(exc, getattr(openai_module, "APITimeoutError", ())):
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

    def _parse(self, raw: str) -> LLMForecastResult:
        try:
            loaded = _load_json(raw)
        except (json.JSONDecodeError, ValueError) as exc:
            raise LLMParseError(f"unparseable LLM response: {exc}") from exc
        for key in ("prob_estimate", "confidence", "rationale"):
            if key not in loaded:
                raise LLMParseError(f"LLM response missing required key: {key}")
        try:
            probability = _clamp_probability(_as_float(loaded["prob_estimate"]))
            confidence = _clamp(_as_float(loaded["confidence"]))
        except (TypeError, ValueError) as exc:
            raise LLMParseError(f"non-numeric LLM field: {exc}") from exc
        assert self.config is not None
        return LLMForecastResult(
            prob_estimate=probability,
            confidence=confidence,
            rationale=str(loaded["rationale"]),
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

    def _cache_put(self, market_id: str, result: LLMForecastResult) -> None:
        if self.config is None or self.config.cache_ttl_s <= 0:
            return
        with self._cache_lock:
            self._cache[market_id] = (time.monotonic(), result)
            if len(self._cache) > 1000:
                oldest = min(self._cache, key=lambda k: self._cache[k][0])
                del self._cache[oldest]

    def _estimated_call_cost_usdc(self) -> Decimal:
        return _ESTIMATED_CALL_COST_USDC

    def _reserve_budget(self, estimated_cost_usdc: Decimal) -> bool:
        assert self.config is not None
        tracker = self._budget_tracker()
        with tracker.lock:
            today = _today_utc()
            if tracker.cost_day != today:
                tracker.cost_day = today
                tracker.daily_cost_usdc = Decimal("0")
            max_daily = self.config.max_daily_llm_cost_usdc
            max_daily_decimal = (
                None if max_daily is None else Decimal(str(max_daily))
            )
            if (
                max_daily_decimal is not None
                and tracker.daily_cost_usdc + estimated_cost_usdc > max_daily_decimal
            ):
                return False
            tracker.daily_cost_usdc += estimated_cost_usdc
            return True

    def _refund_budget(self, estimated_cost_usdc: Decimal) -> None:
        tracker = self._budget_tracker()
        with tracker.lock:
            tracker.daily_cost_usdc = max(
                Decimal("0"),
                tracker.daily_cost_usdc - estimated_cost_usdc,
            )

    def _record_cost(self, estimated_cost_usdc: Decimal, signal: MarketSignal) -> None:
        assert self.config is not None
        daily_cost = self._daily_cost_usdc_float()
        logger.info(
            "llm_forecaster_cost_recorded",
            extra={
                "market_id": signal.market_id,
                "provider": self.config.provider,
                "model": self.config.model,
                "estimated_cost_usdc": float(estimated_cost_usdc),
                "daily_cost_usdc": daily_cost,
                "max_daily_llm_cost_usdc": self.config.max_daily_llm_cost_usdc,
            },
        )

    def _daily_cost_usdc_float(self) -> float:
        tracker = self._budget_tracker()
        with tracker.lock:
            return float(tracker.daily_cost_usdc)

    def _budget_tracker(self) -> _BudgetTracker:
        assert self.config is not None
        key = id(self.config)
        with _BUDGET_TRACKERS_LOCK:
            entry = _BUDGET_TRACKERS.get(key)
            if entry is not None and entry[0]() is self.config:
                return entry[1]
            else:
                tracker = _BudgetTracker()
                _BUDGET_TRACKERS[key] = (ref(self.config), tracker)
            return tracker


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
            "# Market",
            f"title: {signal.title}",
            f"market_id: {signal.market_id}",
            f"venue: {signal.venue}",
            f"yes_price: {signal.yes_price}",
            "",
            "# Orderbook",
            json.dumps(orderbook, sort_keys=True, separators=(",", ":")),
            "",
            "# External Signals",
            json.dumps(signal.external_signal, sort_keys=True, default=str),
            "",
            "# Output",
            "Return JSON only with prob_estimate, confidence, and rationale.",
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
        numeric = float(value)
        if not math.isfinite(numeric):
            raise ValueError("Expected finite numeric value")
        return numeric
    raise ValueError(f"Expected numeric value, got {type(value).__name__}")


def _clamp(value: float) -> float:
    return min(max(value, 0.0), 1.0)


def _clamp_probability(value: float) -> float:
    return min(max(value, 0.01), 0.99)


def _today_utc() -> date:
    return datetime.now(tz=UTC).date()
