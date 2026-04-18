from __future__ import annotations

from collections.abc import Sequence
import logging
from typing import Protocol

import asyncpg

from pms.core.models import Market, Token
from pms.market_selection.merge import MergePolicy, MergeResult, StrategyMarketSet
from pms.storage.market_data_store import PostgresMarketDataStore
from pms.strategies.projections import MarketSelectionSpec


logger = logging.getLogger(__name__)


class StrategySelectionRegistry(Protocol):
    async def list_market_selections(
        self,
    ) -> list[tuple[str, str, MarketSelectionSpec]]: ...


class MarketSelector:
    def __init__(
        self,
        pool: asyncpg.Pool,
        registry: StrategySelectionRegistry,
        merge_policy: MergePolicy,
    ) -> None:
        self._store = PostgresMarketDataStore(pool)
        self._registry = registry
        self._merge_policy = merge_policy

    async def select(self) -> MergeResult:
        strategy_specs = await self._registry.list_market_selections()
        logger.info(
            "market selector processed %d active strategies",
            len(strategy_specs),
        )
        if not strategy_specs:
            logger.warning(
                "no active_version_id rows found; data sensor will idle until "
                "a strategy is activated",
            )

        selections: list[StrategyMarketSet] = []
        for strategy_id, strategy_version_id, spec in strategy_specs:
            eligible_markets = await self._store.read_eligible_markets(
                spec.venue,
                spec.resolution_time_max_horizon_days,
            )
            selections.append(
                StrategyMarketSet(
                    strategy_id=strategy_id,
                    strategy_version_id=strategy_version_id,
                    asset_ids=_asset_ids_from_eligible_markets(eligible_markets),
                )
            )
        return self._merge_policy.merge(selections)


def _asset_ids_from_eligible_markets(
    eligible_markets: Sequence[tuple[Market, Sequence[Token]]],
) -> frozenset[str]:
    return frozenset(
        token.token_id
        for _, tokens in eligible_markets
        for token in tokens
    )
