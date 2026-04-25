from __future__ import annotations

from collections.abc import Sequence
import logging
from typing import Protocol

from pms.core.enums import Venue
from pms.core.exceptions import KalshiStubError
from pms.core.interfaces import MarketDataStore, StrategySelectionRegistry
from pms.core.models import Market, Token
from pms.core.venue_support import kalshi_stub_error, normalize_venue
from pms.market_selection.merge import MergePolicy, MergeResult, StrategyMarketSet


logger = logging.getLogger(__name__)


class UserSubscriptionStore(Protocol):
    async def read_user_subscriptions(self) -> set[str]: ...


class MarketSelector:
    def __init__(
        self,
        store: MarketDataStore,
        registry: StrategySelectionRegistry,
        merge_policy: MergePolicy,
        market_subscription_store: UserSubscriptionStore | None = None,
    ) -> None:
        self._store = store
        self._registry = registry
        self._merge_policy = merge_policy
        self._market_subscription_store = market_subscription_store

    async def select(self) -> MergeResult:
        selections = await self.select_per_strategy()
        logger.info(
            "market selector processed %d active strategies",
            len(selections),
        )
        if not selections:
            logger.warning(
                "no active_version_id rows found; data sensor will idle until "
                "a strategy is activated",
            )
        merged = self._merge_policy.merge(selections)
        user_asset_ids = await self._read_user_subscriptions()
        if not user_asset_ids:
            return merged
        return MergeResult(
            asset_ids=sorted(frozenset(merged.asset_ids) | user_asset_ids),
            conflicts=merged.conflicts,
        )

    async def select_per_strategy(self) -> list[StrategyMarketSet]:
        strategy_specs = await self._registry.list_market_selections()
        selections: list[StrategyMarketSet] = []
        for strategy_id, strategy_version_id, spec in strategy_specs:
            venue = normalize_venue(
                spec.venue,
                context="MarketSelector.select_per_strategy",
            )
            if venue == Venue.KALSHI.value:
                raise kalshi_stub_error("MarketSelector.select_per_strategy")
            eligible_markets = await self._store.read_eligible_markets(
                venue,
                spec.resolution_time_max_horizon_days,
                spec.volume_min_usdc,
            )
            selections.append(
                StrategyMarketSet(
                    strategy_id=strategy_id,
                    strategy_version_id=strategy_version_id,
                    asset_ids=_asset_ids_from_eligible_markets(eligible_markets),
                )
            )
        return selections

    async def _read_user_subscriptions(self) -> frozenset[str]:
        if self._market_subscription_store is None:
            return frozenset()
        return frozenset(await self._market_subscription_store.read_user_subscriptions())


def _asset_ids_from_eligible_markets(
    eligible_markets: Sequence[tuple[Market, Sequence[Token]]],
) -> frozenset[str]:
    return frozenset(
        token.token_id
        for _, tokens in eligible_markets
        for token in tokens
        if token.outcome == "YES"
    )
