from __future__ import annotations

import pytest

from pms.controller.outcome_tokens import MarketDataOutcomeTokenResolver
from pms.core.models import Token


class StaticTokenLookup:
    async def read_tokens_for_market(self, market_id: str) -> list[Token]:
        return [
            Token(token_id=f"{market_id}-yes", condition_id=market_id, outcome="YES"),
            Token(token_id=f"{market_id}-no", condition_id=market_id, outcome="NO"),
        ]


@pytest.mark.asyncio
async def test_market_data_outcome_token_resolver_uses_stored_yes_token() -> None:
    resolver = MarketDataOutcomeTokenResolver(StaticTokenLookup())

    tokens = await resolver.resolve(
        market_id="market-outcomes",
        signal_token_id="market-outcomes-no",
    )

    assert tokens.yes_token_id == "market-outcomes-yes"
    assert tokens.no_token_id == "market-outcomes-no"
