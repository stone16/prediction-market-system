from __future__ import annotations

from collections.abc import Sequence
import logging

from pms.core.enums import Venue
from pms.core.exceptions import KalshiStubError
from pms.core.interfaces import MarketDataStore, StrategySelectionRegistry
from pms.core.models import Market, Token
from pms.core.venue_support import kalshi_stub_error, normalize_venue
from pms.market_selection.merge import MergePolicy, MergeResult, StrategyMarketSet


logger = logging.getLogger(__name__)


class MarketSelector:
    def __init__(
        self,
        store: MarketDataStore,
        registry: StrategySelectionRegistry,
        merge_policy: MergePolicy,
    ) -> None:
        self._store = store
        self._registry = registry
        self._merge_policy = merge_policy

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
        return self._merge_policy.merge(selections)

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


def _asset_ids_from_eligible_markets(
    eligible_markets: Sequence[tuple[Market, Sequence[Token]]],
) -> frozenset[str]:
    return frozenset(
        token.token_id
        for _, tokens in eligible_markets
        for token in tokens
    )
