from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import httpx

from pms.core.models import Market, MarketSignal, Outcome, Token
from pms.storage.market_data_store import PostgresMarketDataStore


logger = logging.getLogger(__name__)


@dataclass
class MarketDiscoverySensor:
    store: PostgresMarketDataStore
    http_client: httpx.AsyncClient
    poll_interval_s: float = 60.0

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
                backoff = self._INITIAL_BACKOFF_S
                await asyncio.sleep(self.poll_interval_s)
            except httpx.HTTPStatusError as error:
                if error.response.status_code != 429:
                    raise
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

            if not tokens:
                logger.info(
                    "market %s missing clobTokenIds; skipping token upserts",
                    market.condition_id,
                )
                continue

            for token in tokens:
                await self.store.write_token(token)

    async def aclose(self) -> None:
        await self.http_client.aclose()


def _gamma_market_to_market(row: dict[str, Any], fetched_at: datetime) -> Market:
    condition_id = str(
        row.get("conditionId") or row.get("condition_id") or row.get("id") or ""
    )
    if condition_id == "":
        msg = "Gamma market row is missing conditionId"
        raise KeyError(msg)

    created_at = _optional_datetime(row.get("createdAt")) or fetched_at
    return Market(
        condition_id=condition_id,
        slug=str(row.get("slug") or condition_id),
        question=str(row.get("question") or ""),
        venue="polymarket",
        resolves_at=_optional_datetime(row.get("endDateIso")),
        created_at=created_at,
        last_seen_at=fetched_at,
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

    tokens: list[Token] = []
    outcomes: tuple[Outcome, Outcome] = ("YES", "NO")
    for index, outcome in enumerate(outcomes):
        if index >= len(loaded):
            continue
        token_id = loaded[index]
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


def _optional_datetime(value: object) -> datetime | None:
    if value is None or value == "":
        return None
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed
