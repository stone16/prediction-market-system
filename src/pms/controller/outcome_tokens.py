from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from pms.core.models import Token


@dataclass(frozen=True, slots=True)
class OutcomeTokens:
    yes_token_id: str | None
    no_token_id: str | None


class OutcomeTokenResolver(Protocol):
    async def resolve(
        self,
        *,
        market_id: str,
        signal_token_id: str | None,
    ) -> OutcomeTokens: ...


@dataclass(frozen=True, slots=True)
class NullOutcomeTokenResolver:
    async def resolve(
        self,
        *,
        market_id: str,
        signal_token_id: str | None,
    ) -> OutcomeTokens:
        del market_id
        return OutcomeTokens(yes_token_id=signal_token_id, no_token_id=None)


class TokenLookup(Protocol):
    async def read_tokens_for_market(self, market_id: str) -> list[Token]: ...


@dataclass(frozen=True, slots=True)
class MarketDataOutcomeTokenResolver:
    store: TokenLookup

    async def resolve(
        self,
        *,
        market_id: str,
        signal_token_id: str | None,
    ) -> OutcomeTokens:
        tokens = await self.store.read_tokens_for_market(market_id)
        yes_token_id = signal_token_id
        no_token_id: str | None = None
        for token in tokens:
            if token.outcome == "YES" and yes_token_id is None:
                yes_token_id = token.token_id
            if token.outcome == "NO":
                no_token_id = token.token_id
        return OutcomeTokens(yes_token_id=yes_token_id, no_token_id=no_token_id)
