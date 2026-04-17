from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from pms.core.models import BookLevel, BookSnapshot, PriceChange, Token
from pms.storage.market_data_store import PostgresMarketDataStore


@dataclass(frozen=True)
class DepthLevelPayload:
    price: float
    size: float


@dataclass(frozen=True)
class SignalDepthPayload:
    best_bid: float | None
    best_ask: float | None
    bids: list[DepthLevelPayload]
    asks: list[DepthLevelPayload]
    last_update_ts: str | None
    stale: bool


class SignalDepthNotFoundError(LookupError):
    """Raised when the requested market does not exist in storage."""


async def get_signal_depth(
    store: PostgresMarketDataStore,
    *,
    market_id: str,
    limit: int,
    stale_snapshot_threshold_s: float,
    now: datetime | None = None,
) -> SignalDepthPayload:
    market = await store.read_market(market_id)
    if market is None:
        msg = f"Unknown market_id: {market_id}"
        raise SignalDepthNotFoundError(msg)

    tokens = await store.read_tokens_for_market(market_id)
    preferred_token = _preferred_token(tokens)
    snapshot = await _preferred_snapshot(store, market_id=market_id, preferred_token=preferred_token, tokens=tokens)

    if preferred_token is None and snapshot is None:
        return SignalDepthPayload(
            best_bid=None,
            best_ask=None,
            bids=[],
            asks=[],
            last_update_ts=None,
            stale=False,
        )

    if snapshot is not None:
        active_token_id = snapshot.token_id
    else:
        assert preferred_token is not None
        active_token_id = preferred_token.token_id
    snapshot_levels = (
        await store.read_levels_for_snapshot(snapshot.id)
        if snapshot is not None
        else []
    )
    deltas = await _read_relevant_deltas(
        store,
        market_id=market_id,
        token_id=active_token_id,
        snapshot=snapshot,
    )
    bids, asks = _reconstruct_levels(
        snapshot_levels=snapshot_levels,
        deltas=deltas,
        limit=limit,
    )
    reference_now = now or datetime.now(tz=UTC)
    last_update = _last_update_ts(snapshot=snapshot, deltas=deltas)
    stale = snapshot is not None and (
        reference_now - snapshot.ts
    ).total_seconds() > stale_snapshot_threshold_s
    best_bid = bids[0].price if bids else None
    best_ask = asks[0].price if asks else None
    return SignalDepthPayload(
        best_bid=best_bid,
        best_ask=best_ask,
        bids=bids,
        asks=asks,
        last_update_ts=last_update.isoformat() if last_update is not None else None,
        stale=stale,
    )


async def _preferred_snapshot(
    store: PostgresMarketDataStore,
    *,
    market_id: str,
    preferred_token: Token | None,
    tokens: list[Token],
) -> BookSnapshot | None:
    snapshots: list[BookSnapshot] = []
    if preferred_token is not None:
        preferred_snapshot = await store.read_latest_snapshot(market_id, preferred_token.token_id)
        if preferred_snapshot is not None:
            return preferred_snapshot
    for token in tokens:
        snapshot = await store.read_latest_snapshot(market_id, token.token_id)
        if snapshot is not None:
            snapshots.append(snapshot)
    if not snapshots:
        return None
    return max(snapshots, key=lambda item: (item.ts, item.id))


async def _read_relevant_deltas(
    store: PostgresMarketDataStore,
    *,
    market_id: str,
    token_id: str,
    snapshot: BookSnapshot | None,
) -> list[PriceChange]:
    since_ts = snapshot.ts if snapshot is not None else datetime.min.replace(tzinfo=UTC)
    return await store.read_price_changes_since(market_id, token_id, since_ts)


def _preferred_token(tokens: list[Token]) -> Token | None:
    if not tokens:
        return None
    for token in tokens:
        if token.outcome == "YES":
            return token
    return tokens[0]


def _reconstruct_levels(
    *,
    snapshot_levels: list[BookLevel],
    deltas: list[PriceChange],
    limit: int,
) -> tuple[list[DepthLevelPayload], list[DepthLevelPayload]]:
    bid_levels: dict[float, float] = {}
    ask_levels: dict[float, float] = {}
    for level in snapshot_levels:
        if level.side == "BUY":
            bid_levels[level.price] = level.size
        else:
            ask_levels[level.price] = level.size
    for delta in deltas:
        levels = bid_levels if delta.side == "BUY" else ask_levels
        if delta.size <= 0:
            levels.pop(delta.price, None)
            continue
        levels[delta.price] = delta.size
    return _sorted_levels(bid_levels, ask_levels, limit=limit)


def _sorted_levels(
    bid_levels: dict[float, float],
    ask_levels: dict[float, float],
    *,
    limit: int,
) -> tuple[list[DepthLevelPayload], list[DepthLevelPayload]]:
    bounded_limit = max(limit, 0)
    bids = [
        DepthLevelPayload(price=price, size=size)
        for price, size in sorted(bid_levels.items(), key=lambda item: item[0], reverse=True)[:bounded_limit]
    ]
    asks = [
        DepthLevelPayload(price=price, size=size)
        for price, size in sorted(ask_levels.items(), key=lambda item: item[0])[:bounded_limit]
    ]
    return bids, asks


def _last_update_ts(
    *,
    snapshot: BookSnapshot | None,
    deltas: list[PriceChange],
) -> datetime | None:
    # last_update_ts = MAX(GREATEST(book_snapshots.ts, latest_applied_price_changes.ts)).
    # Using only book_snapshots.ts hides fresher applied deltas; using only
    # price_changes.ts erases valid snapshot-only books.
    timestamps = [delta.ts for delta in deltas]
    if snapshot is not None:
        timestamps.append(snapshot.ts)
    if not timestamps:
        return None
    return max(timestamps)
