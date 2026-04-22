from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Awaitable, Callable, cast

import asyncpg
import httpx

from pms.core.enums import Venue
from pms.core.exceptions import KalshiStubError
from pms.core.models import Market, MarketSignal, Outcome, Token
from pms.core.models import Venue as VenueValue
from pms.core.venue_support import kalshi_stub_error, normalize_venue
from pms.storage.market_data_store import PostgresMarketDataStore


logger = logging.getLogger(__name__)


@dataclass
class MarketDiscoverySensor:
    store: PostgresMarketDataStore
    http_client: httpx.AsyncClient
    poll_interval_s: float = 60.0
    on_poll_complete: Callable[[], Awaitable[None]] | None = None

    _INITIAL_BACKOFF_S = 1.0
    _MAX_BACKOFF_S = 30.0

    def __aiter__(self) -> AsyncIterator[MarketSignal]:
        return self._iterate()

    async def _iterate(self) -> AsyncIterator[MarketSignal]:
        sentinel: MarketSignal | None = None
        if sentinel is not None:  # pragma: no cover - keeps this as an async generator.
            yield sentinel

        backoff = self._INITIAL_BACKOFF_S
        while True:
            try:
                await self.poll_once()
                if self.on_poll_complete is not None:
                    try:
                        await self.on_poll_complete()
                    except Exception as error:  # noqa: BLE001
                        logger.warning("discovery poll completion hook failed: %s", error)
                backoff = self._INITIAL_BACKOFF_S
                await asyncio.sleep(self.poll_interval_s)
            except httpx.HTTPStatusError as error:
                if error.response.status_code != 429:
                    logger.warning("discovery poll HTTP error: %s", error)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2.0, self._MAX_BACKOFF_S)
            except (
                httpx.HTTPError,
                OSError,
                asyncio.TimeoutError,
                json.JSONDecodeError,
                asyncpg.PostgresError,
            ) as error:
                logger.warning("discovery poll transient error: %s", error)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2.0, self._MAX_BACKOFF_S)

    async def poll_once(self) -> None:
        response = await self.http_client.get("/markets")
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, list):
            msg = "Expected Gamma API /markets response to be a list"
            raise ValueError(msg)

        fetched_at = datetime.now(tz=UTC)
        for row in payload:
            if not isinstance(row, dict):
                continue
            try:
                market = _gamma_market_to_market(row, fetched_at)
                await self.store.write_market(market)
                tokens = _gamma_market_to_tokens(row, market.condition_id)
            except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
                logger.warning("skipping malformed Gamma market row: %s", error)
                continue
            except asyncpg.PostgresError as error:
                logger.warning(
                    "discovery write_market failed for %s: %s",
                    row.get("conditionId") or row.get("condition_id"),
                    error,
                )
                continue

            if not tokens:
                logger.info(
                    "market %s missing clobTokenIds; skipping token upserts",
                    market.condition_id,
                )
                continue

            for token in tokens:
                try:
                    await self.store.write_token(token)
                except asyncpg.PostgresError as error:
                    logger.warning(
                        "discovery write_token failed for %s: %s",
                        token.token_id,
                        error,
                    )
                    continue

    async def aclose(self) -> None:
        await self.http_client.aclose()


def _gamma_market_to_market(row: dict[str, Any], fetched_at: datetime) -> Market:
    condition_id = str(row.get("conditionId") or row.get("condition_id") or "")
    if condition_id == "":
        msg = "Gamma market row is missing conditionId"
        raise KeyError(msg)

    venue = normalize_venue(
        _first_non_empty_value(row.get("venue"), Venue.POLYMARKET.value),
        context="MarketDiscoverySensor._gamma_market_to_market",
    )
    if venue == Venue.KALSHI.value:
        raise kalshi_stub_error("MarketDiscoverySensor._gamma_market_to_market")

    created_at = _optional_datetime(row.get("createdAt")) or fetched_at
    return Market(
        condition_id=condition_id,
        slug=str(row.get("slug") or condition_id),
        question=str(row.get("question") or ""),
        venue=cast(VenueValue, venue),
        resolves_at=_optional_datetime(row.get("endDateIso")),
        created_at=created_at,
        last_seen_at=fetched_at,
        volume_24h=_optional_float(
            _first_non_empty_value(row.get("volume24hr"), row.get("volume_24h"))
        ),
    )


def _gamma_market_to_tokens(
    row: dict[str, Any],
    condition_id: str,
) -> list[Token]:
    raw_token_ids = row.get("clobTokenIds")
    if raw_token_ids in {None, ""}:
        return []

    loaded = json.loads(raw_token_ids) if isinstance(raw_token_ids, str) else raw_token_ids
    if not isinstance(loaded, list):
        msg = "clobTokenIds must decode to a list"
        raise ValueError(msg)

    outcomes = _gamma_market_outcomes(row)
    if len(loaded) != len(outcomes):
        msg = "Gamma market row has mismatched clobTokenIds/outcomes lengths"
        raise ValueError(msg)

    tokens: list[Token] = []
    for token_id, outcome in zip(loaded, outcomes, strict=True):
        if token_id in {None, ""}:
            continue
        tokens.append(
            Token(
                token_id=str(token_id),
                condition_id=condition_id,
                outcome=outcome,
            )
        )
    return tokens


def _gamma_market_outcomes(row: dict[str, Any]) -> tuple[Outcome, Outcome]:
    raw_outcomes = row.get("outcomes")
    if raw_outcomes in {None, ""}:
        msg = "Gamma market row is missing outcomes"
        raise ValueError(msg)
    loaded = json.loads(raw_outcomes) if isinstance(raw_outcomes, str) else raw_outcomes
    if not isinstance(loaded, list):
        msg = "outcomes must decode to a list"
        raise ValueError(msg)
    normalized = tuple(str(outcome).strip().upper() for outcome in loaded)
    if set(normalized) != {"YES", "NO"} or len(normalized) != 2:
        msg = "Gamma market row must expose exactly YES/NO outcomes"
        raise ValueError(msg)
    return cast(tuple[Outcome, Outcome], normalized)


def _optional_datetime(value: object) -> datetime | None:
    if value is None or value == "":
        return None
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed


def _first_non_empty_value(*values: object) -> object | None:
    for value in values:
        if value is not None and value != "":
            return value
    return None


def _optional_float(value: object) -> float | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        msg = "expected a float-compatible value"
        raise TypeError(msg)
    return float(value)
