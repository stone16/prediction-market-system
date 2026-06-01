from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
import math
from typing import Any, Callable, Literal, Protocol

import httpx

from pms.core.models import BookLevel, BookSnapshot


class DirectBookSnapshotReader(Protocol):
    async def read_latest_snapshot(
        self,
        market_id: str,
        token_id: str,
    ) -> BookSnapshot | None: ...

    async def read_levels_for_snapshot(self, snapshot_id: int) -> list[BookLevel]: ...


class VenueBookClient(Protocol):
    async def read_order_book(
        self,
        market_id: str,
        token_id: str,
    ) -> VenueBook: ...


@dataclass(frozen=True, slots=True)
class VenueBook:
    market_id: str
    token_id: str
    ts: datetime
    hash: str | None
    bids: tuple[BookLevel, ...]
    asks: tuple[BookLevel, ...]


@dataclass
class RefreshingDirectBookSnapshotReader:
    primary: DirectBookSnapshotReader
    venue_client: VenueBookClient
    max_snapshot_age_ms: float
    allowed_clock_skew_ms: float = 250.0
    clock: Callable[[], datetime] = field(default_factory=lambda: _utc_now)
    _levels_by_synthetic_snapshot_id: dict[int, list[BookLevel]] = field(
        default_factory=dict,
        init=False,
    )
    _next_synthetic_snapshot_id: int = field(default=-1, init=False)

    async def read_latest_snapshot(
        self,
        market_id: str,
        token_id: str,
    ) -> BookSnapshot | None:
        snapshot = await self.primary.read_latest_snapshot(market_id, token_id)
        if snapshot is not None and not self._is_stale(snapshot.ts):
            return snapshot

        try:
            venue_book = await self.venue_client.read_order_book(market_id, token_id)
        except Exception:
            if snapshot is not None:
                return snapshot
            raise

        snapshot_id = self._allocate_synthetic_snapshot_id()
        levels = [
            *(
                BookLevel(
                    snapshot_id=snapshot_id,
                    market_id=level.market_id,
                    side=level.side,
                    price=level.price,
                    size=level.size,
                )
                for level in venue_book.bids
            ),
            *(
                BookLevel(
                    snapshot_id=snapshot_id,
                    market_id=level.market_id,
                    side=level.side,
                    price=level.price,
                    size=level.size,
                )
                for level in venue_book.asks
            ),
        ]
        self._levels_by_synthetic_snapshot_id[snapshot_id] = levels
        return BookSnapshot(
            id=snapshot_id,
            market_id=market_id,
            token_id=token_id,
            ts=venue_book.ts,
            hash=venue_book.hash,
            source="venue_direct",
        )

    async def read_levels_for_snapshot(self, snapshot_id: int) -> list[BookLevel]:
        synthetic_levels = self._levels_by_synthetic_snapshot_id.get(snapshot_id)
        if synthetic_levels is not None:
            return list(synthetic_levels)
        return await self.primary.read_levels_for_snapshot(snapshot_id)

    def _is_stale(self, ts: datetime) -> bool:
        now = _call_clock(self.clock)
        raw_age_ms = (_aware_utc(now) - _aware_utc(ts)).total_seconds() * 1000.0
        if raw_age_ms < -self.allowed_clock_skew_ms:
            return True
        return max(0.0, raw_age_ms) > self.max_snapshot_age_ms

    def _allocate_synthetic_snapshot_id(self) -> int:
        snapshot_id = self._next_synthetic_snapshot_id
        self._next_synthetic_snapshot_id -= 1
        return snapshot_id


@dataclass(frozen=True, slots=True)
class PolymarketPublicBookClient:
    host: str = "https://clob.polymarket.com"
    timeout_s: float = 5.0
    clock: Callable[[], datetime] = field(default_factory=lambda: _utc_now)

    async def read_order_book(
        self,
        market_id: str,
        token_id: str,
    ) -> VenueBook:
        async with httpx.AsyncClient(http2=False, timeout=self.timeout_s) as client:
            response = await client.get(
                f"{self.host.rstrip('/')}/book",
                params={"token_id": token_id},
            )
            response.raise_for_status()
            payload = response.json()
        if not isinstance(payload, Mapping):
            msg = "Polymarket book response must be a JSON object"
            raise ValueError(msg)
        return venue_book_from_clob_payload(
            payload,
            market_id=market_id,
            token_id=token_id,
            observed_at=_call_clock(self.clock),
        )


def venue_book_from_clob_payload(
    payload: Mapping[str, Any],
    *,
    market_id: str,
    token_id: str,
    observed_at: datetime,
) -> VenueBook:
    raw_token_id = _optional_text(payload.get("asset_id"))
    if raw_token_id is not None and raw_token_id != token_id:
        msg = "Polymarket book response asset_id does not match requested token"
        raise ValueError(msg)
    observed_ts = _aware_utc(observed_at)
    bids = tuple(
        _book_levels_from_payload_side(
            payload.get("bids"),
            market_id=market_id,
            side="BUY",
        )
    )
    asks = tuple(
        _book_levels_from_payload_side(
            payload.get("asks"),
            market_id=market_id,
            side="SELL",
        )
    )
    return VenueBook(
        market_id=market_id,
        token_id=token_id,
        ts=observed_ts,
        hash=_optional_text(payload.get("hash")),
        bids=bids,
        asks=asks,
    )


def _book_levels_from_payload_side(
    raw_levels: object,
    *,
    market_id: str,
    side: Literal["BUY", "SELL"],
) -> list[BookLevel]:
    if raw_levels is None:
        return []
    if not isinstance(raw_levels, Sequence) or isinstance(raw_levels, (str, bytes)):
        msg = "Polymarket book levels must be a sequence"
        raise ValueError(msg)
    levels: list[BookLevel] = []
    for raw_level in raw_levels:
        if not isinstance(raw_level, Mapping):
            msg = "Polymarket book level must be a JSON object"
            raise ValueError(msg)
        price = _finite_float(raw_level.get("price"), field_name="price")
        size = _finite_float(raw_level.get("size"), field_name="size")
        if not (0.0 < price <= 1.0):
            msg = "Polymarket book level price must satisfy 0.0 < price <= 1.0"
            raise ValueError(msg)
        if size <= 0.0:
            msg = "Polymarket book level size must be positive"
            raise ValueError(msg)
        levels.append(
            BookLevel(
                snapshot_id=0,
                market_id=market_id,
                side=side,
                price=price,
                size=size,
            )
        )
    return levels


def _finite_float(value: object, *, field_name: str) -> float:
    try:
        parsed = float(str(value))
    except (TypeError, ValueError) as exc:
        msg = f"Polymarket book level {field_name} is not numeric"
        raise ValueError(msg) from exc
    if not math.isfinite(parsed):
        msg = f"Polymarket book level {field_name} is not finite"
        raise ValueError(msg)
    return parsed


def _optional_text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _call_clock(clock: Callable[[], datetime]) -> datetime:
    return clock()


def _aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _utc_now() -> datetime:
    return datetime.now(tz=UTC)
