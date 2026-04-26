from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass
from importlib import import_module
from typing import Protocol, Self, cast

from pms.config import LLMSettings
from pms.core.models import MarketSignal


class _MessagesClient(Protocol):
    def create(self, **kwargs: object) -> object: ...


class _ClaudeClient(Protocol):
    messages: _MessagesClient


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

    def __post_init__(self) -> None:
        if self.config is None:
            self.config = LLMSettings()

    def predict(self, signal: MarketSignal) -> LLMForecastResult | None:
        if self.config is None or not self.config.enabled:
            return None
        return LLMForecastResult(
            prob_estimate=signal.yes_price,
            confidence=0.0,
            rationale="pre-s5-neutral",
            model_id="neutral",
        )

    async def forecast(self, signal: MarketSignal) -> float:
        result = self.predict(signal)
        return signal.yes_price if result is None else result[0]

    def _client(self, api_key: str) -> _ClaudeClient | None:
        if self.client is not None:
            return cast(_ClaudeClient, self.client)
        try:
            anthropic_module = import_module("anthropic")
        except ImportError:
            return None
        client_factory = getattr(anthropic_module, "Anthropic", None)
        if not callable(client_factory):
            return None
        factory = cast(Callable[..., _ClaudeClient], client_factory)
        created_client = factory(api_key=api_key)
        self.client = created_client
        return created_client


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
            + json.dumps(signal.external_signal, sort_keys=True, default=str),
        ]
    )


def _top_five(levels: object) -> object:
    if isinstance(levels, list):
        return levels[:5]
    return []


def _parse_response(response: object) -> dict[str, object]:
    text = _response_text(response)
    try:
        loaded = json.loads(text)
    except json.JSONDecodeError:
        start = text.find("{")
        end = text.rfind("}")
        if start == -1 or end == -1 or start > end:
            raise
        loaded = json.loads(text[start : end + 1])
    if not isinstance(loaded, dict):
        raise ValueError("Expected Claude forecast response to be a JSON object")
    for key in ("prob_estimate", "confidence", "rationale"):
        if key not in loaded:
            raise ValueError(f"Claude forecast response missing {key}")
    return loaded


def _response_text(response: object) -> str:
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


def _as_float(value: object) -> float:
    if isinstance(value, str | int | float):
        return float(value)
    raise ValueError(f"Expected numeric value, got {type(value).__name__}")


def _clamp(value: float) -> float:
    return min(max(value, 0.0), 1.0)
