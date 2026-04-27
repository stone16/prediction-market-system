from __future__ import annotations

import asyncio
import importlib
import json
import logging
import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable, Final, Literal, Protocol, cast
from uuid import uuid4

from pms.config import PMSSettings, validate_live_mode_ready
from pms.core.enums import OrderStatus, RunMode
from pms.core.models import (
    BookLevel,
    BookSnapshot,
    LiveTradingDisabledError,
    Market,
    OrderState,
    Position,
    Portfolio,
    ReconciliationReport,
    TradeDecision,
    VenueAccountSnapshot,
    VenueCredentials,
)


logger = logging.getLogger(__name__)


class OperatorApprovalRequiredError(LiveTradingDisabledError):
    """Raised when the first live order has not been approved by an operator."""


class PolymarketSubmissionUnknownError(RuntimeError):
    """Raised when a Polymarket order submission timed out — the order may
    have reached the venue. Operators must reconcile before retrying.
    """

    order_state: OrderState | None

    def __init__(
        self,
        message: str,
        *,
        order_state: OrderState | None = None,
    ) -> None:
        super().__init__(message)
        self.order_state = order_state


@dataclass(frozen=True)
class LiveOrderPreview:
    max_notional_usdc: float
    venue: str
    market_id: str
    token_id: str | None
    side: str
    limit_price: float
    max_slippage_bps: int
    outcome: str = "YES"
    market_slug: str | None = None
    question: str | None = None


@dataclass(frozen=True)
class PolymarketOrderRequest:
    market_id: str
    token_id: str
    side: str
    price: float
    size: float
    notional_usdc: float
    estimated_quantity: float
    order_type: str
    time_in_force: str
    max_slippage_bps: int


@dataclass(frozen=True)
class PolymarketOrderResult:
    order_id: str
    status: str
    raw_status: str
    filled_notional_usdc: float
    remaining_notional_usdc: float
    fill_price: float | None
    filled_quantity: float


@dataclass(frozen=True)
class LivePreSubmitQuote:
    market_status: str
    book_age_ms: float
    executable_notional_usdc: float
    best_executable_price: float
    spread_bps: float
    quote_hash: str
    book_ts: datetime
    source: Literal["postgres_snapshot", "venue_direct", "dual"] = "postgres_snapshot"


@dataclass(frozen=True)
class LiveVenueBook:
    market_id: str
    token_id: str
    bids: tuple[BookLevel, ...]
    asks: tuple[BookLevel, ...]
    book_ts: datetime
    quote_hash: str
    market_status: str = "open"


class PolymarketClient(Protocol):
    async def submit_order(
        self,
        order: PolymarketOrderRequest,
        credentials: VenueCredentials,
    ) -> PolymarketOrderResult: ...


class LiveQuoteProvider(Protocol):
    async def quote(
        self,
        order: PolymarketOrderRequest,
        credentials: VenueCredentials,
    ) -> LivePreSubmitQuote: ...


class LiveOrderBookClient(Protocol):
    async def read_order_book(
        self,
        order: PolymarketOrderRequest,
        credentials: VenueCredentials,
    ) -> LiveVenueBook: ...


class LiveVenueAccountClient(Protocol):
    async def read_account_snapshot(
        self,
        credentials: VenueCredentials,
    ) -> VenueAccountSnapshot: ...


class BookQuoteStore(Protocol):
    async def read_market(self, market_id: str) -> Market | None: ...

    async def read_latest_snapshot(
        self,
        market_id: str,
        token_id: str,
    ) -> BookSnapshot | None: ...

    async def read_levels_for_snapshot(self, snapshot_id: int) -> list[BookLevel]: ...


@dataclass(frozen=True)
class MissingLiveQuoteProvider:
    async def quote(
        self,
        order: PolymarketOrderRequest,
        credentials: VenueCredentials,
    ) -> LivePreSubmitQuote:
        del order, credentials
        msg = (
            "Polymarket pre-submit quote guard is not configured; "
            "LIVE submit requires a fresh venue book check"
        )
        raise LiveTradingDisabledError(msg)


@dataclass(frozen=True)
class PolymarketBookQuoteProvider:
    store: BookQuoteStore
    clock: Callable[[], datetime] = field(default_factory=lambda: _utc_now)
    allowed_clock_skew_ms: float = 250.0

    async def quote(
        self,
        order: PolymarketOrderRequest,
        credentials: VenueCredentials,
    ) -> LivePreSubmitQuote:
        del credentials
        now = self.clock()
        market = await self.store.read_market(order.market_id)
        if market is None:
            msg = f"Polymarket market data missing for {order.market_id}"
            raise LiveTradingDisabledError(msg)
        snapshot = await self.store.read_latest_snapshot(order.market_id, order.token_id)
        if snapshot is None:
            msg = f"Polymarket book snapshot missing for token {order.token_id}"
            raise LiveTradingDisabledError(msg)
        _raise_if_future_book_ts(
            snapshot.ts,
            now=now,
            allowed_clock_skew_ms=self.allowed_clock_skew_ms,
        )
        levels = await self.store.read_levels_for_snapshot(snapshot.id)
        return _quote_from_levels(
            order=order,
            market_status=_market_status(market, now),
            bid_levels=[level for level in levels if level.side == "BUY"],
            ask_levels=[level for level in levels if level.side == "SELL"],
            quote_hash=snapshot.hash or f"snapshot:{snapshot.id}",
            book_ts=snapshot.ts,
            now=now,
            source="postgres_snapshot",
        )


@dataclass(frozen=True)
class PolymarketDirectQuoteProvider:
    book_client: LiveOrderBookClient
    clock: Callable[[], datetime] = field(default_factory=lambda: _utc_now)
    allowed_clock_skew_ms: float = 250.0

    async def quote(
        self,
        order: PolymarketOrderRequest,
        credentials: VenueCredentials,
    ) -> LivePreSubmitQuote:
        now = self.clock()
        book = await self.book_client.read_order_book(order, credentials)
        _raise_if_future_book_ts(
            book.book_ts,
            now=now,
            allowed_clock_skew_ms=self.allowed_clock_skew_ms,
        )
        return _quote_from_levels(
            order=order,
            market_status=book.market_status,
            bid_levels=book.bids,
            ask_levels=book.asks,
            quote_hash=book.quote_hash,
            book_ts=book.book_ts,
            now=now,
            source="venue_direct",
        )


@dataclass(frozen=True)
class PolymarketRoutingQuoteProvider:
    snapshot_provider: LiveQuoteProvider
    direct_provider: LiveQuoteProvider
    settings: PMSSettings

    async def quote(
        self,
        order: PolymarketOrderRequest,
        credentials: VenueCredentials,
    ) -> LivePreSubmitQuote:
        return await self.quote_for_order(
            order,
            credentials,
            first_live_order=False,
        )

    async def quote_for_order(
        self,
        order: PolymarketOrderRequest,
        credentials: VenueCredentials,
        *,
        first_live_order: bool,
    ) -> LivePreSubmitQuote:
        source = self.settings.controller.quote_source
        if (
            source == "venue_direct"
            or first_live_order
            or _requires_direct_quote(order, self.settings)
        ):
            return await self.direct_provider.quote(order, credentials)
        if source == "dual":
            snapshot_quote = await self.snapshot_provider.quote(order, credentials)
            direct_quote = await self.direct_provider.quote(order, credentials)
            _validate_dual_quote_match(
                snapshot_quote,
                direct_quote,
                self.settings,
            )
            return replace(direct_quote, source="dual")
        return await self.snapshot_provider.quote(order, credentials)


class FirstLiveOrderGate(Protocol):
    async def approve_first_order(self, preview: LiveOrderPreview) -> bool: ...

    async def consume(self, preview: LiveOrderPreview) -> None: ...


@dataclass(frozen=True)
class DenyFirstLiveOrderGate:
    async def approve_first_order(self, preview: LiveOrderPreview) -> bool:
        del preview
        return False

    async def consume(self, preview: LiveOrderPreview) -> None:
        del preview


@dataclass
class FirstLiveOrderApprovalState:
    approved: bool = False


@dataclass(frozen=True)
class FileFirstLiveOrderGate:
    path: Path

    async def approve_first_order(self, preview: LiveOrderPreview) -> bool:
        if not self.path.exists():
            return False
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return False
        if not isinstance(payload, dict):
            return False
        return _approval_payload_matches(cast(dict[str, object], payload), preview)

    async def consume(self, preview: LiveOrderPreview) -> None:
        # Atomically consume the approval artefact so it cannot be replayed
        # by a future restart or a concurrent `approve_first_order` call.
        del preview
        try:
            self.path.unlink()
        except FileNotFoundError:
            pass


@dataclass(frozen=True)
class PolymarketSDKClient:
    async def submit_order(
        self,
        order: PolymarketOrderRequest,
        credentials: VenueCredentials,
    ) -> PolymarketOrderResult:
        return await asyncio.to_thread(self._submit_order_sync, order, credentials)

    async def read_order_book(
        self,
        order: PolymarketOrderRequest,
        credentials: VenueCredentials,
    ) -> LiveVenueBook:
        return await asyncio.to_thread(self._read_order_book_sync, order, credentials)

    async def read_account_snapshot(
        self,
        credentials: VenueCredentials,
    ) -> VenueAccountSnapshot:
        return await asyncio.to_thread(self._read_account_snapshot_sync, credentials)

    def _submit_order_sync(
        self,
        order: PolymarketOrderRequest,
        credentials: VenueCredentials,
    ) -> PolymarketOrderResult:
        try:
            sdk = importlib.import_module("py_clob_client_v2")
        except ModuleNotFoundError:
            msg = (
                "Polymarket live SDK is not installed. Install the live extra "
                "with `uv sync --extra live` before enabling LIVE mode."
            )
            raise LiveTradingDisabledError(msg) from None

        try:
            client = _build_sdk_client(sdk, credentials)
            response = _post_sdk_order(sdk, client, order)
        except (asyncio.CancelledError, KeyboardInterrupt):
            # Cooperative cancellation — caller decides what to do. The
            # underlying request may still be in flight on Polymarket; the
            # caller is responsible for reconciliation if it suppresses this.
            raise
        except (asyncio.TimeoutError, TimeoutError) as exc:
            # Bare timeout — the venue never responded. Surface as
            # submission_unknown so operators reconcile before retrying.
            redacted = _redacted_exception_message(exc, credentials)
            logger.warning(
                "Polymarket live order submission timed out (%s): %s",
                type(exc).__name__,
                redacted,
            )
            msg = (
                "Polymarket live order submission timed out; order status is "
                "unknown — reconcile with venue before retrying"
            )
            raise PolymarketSubmissionUnknownError(msg) from None
        except Exception as exc:  # noqa: BLE001
            redacted = _redacted_exception_message(exc, credentials)
            # `py_clob_client_v2.exceptions.PolyApiException` wraps httpx
            # request errors (timeouts, connection drops) with `resp=None`,
            # and HTTP-level venue errors with a populated httpx Response.
            # The bare `except (TimeoutError, ...)` block above misses
            # these wrapped transport failures — without this branch a
            # real timeout still routes to `LiveTradingDisabledError` =
            # venue_rejection, defeating the f2 contract that timeouts
            # must surface as submission_unknown. Duck-typed (class name
            # + attribute) so polymarket.py keeps no hard dependency on
            # the SDK's exception class.
            if _is_sdk_transport_failure(exc):
                logger.warning(
                    "Polymarket live order submission transport failure (%s): %s; "
                    "treating as submission_unknown",
                    type(exc).__name__,
                    redacted,
                )
                msg = (
                    "Polymarket live order submission transport failure; "
                    "order status is unknown — reconcile with venue before retrying"
                )
                raise PolymarketSubmissionUnknownError(msg) from None
            logger.warning(
                "Polymarket live order submission failed (%s): %s",
                type(exc).__name__,
                redacted,
            )
            msg = (
                "Polymarket live order submission failed "
                f"({type(exc).__name__}); venue error redacted"
            )
            raise LiveTradingDisabledError(msg) from None

        return _order_result_from_sdk_response(order, response)

    def _read_order_book_sync(
        self,
        order: PolymarketOrderRequest,
        credentials: VenueCredentials,
    ) -> LiveVenueBook:
        try:
            sdk = importlib.import_module("py_clob_client_v2")
        except ModuleNotFoundError:
            msg = (
                "Polymarket live SDK is not installed. Install the live extra "
                "with `uv sync --extra live` before enabling LIVE mode."
            )
            raise LiveTradingDisabledError(msg) from None

        try:
            client = _build_sdk_client(sdk, credentials)
            response = _get_sdk_order_book(client, order.token_id)
        except Exception as exc:  # noqa: BLE001
            redacted = _redacted_exception_message(exc, credentials)
            logger.warning(
                "Polymarket live order book fetch failed (%s): %s",
                type(exc).__name__,
                redacted,
            )
            msg = (
                "Polymarket live order book fetch failed "
                f"({type(exc).__name__}); venue error redacted"
            )
            raise LiveTradingDisabledError(msg) from None

        return _venue_book_from_sdk_response(
            response,
            market_id=order.market_id,
            token_id=order.token_id,
        )

    def _read_account_snapshot_sync(
        self,
        credentials: VenueCredentials,
    ) -> VenueAccountSnapshot:
        try:
            sdk = importlib.import_module("py_clob_client_v2")
        except ModuleNotFoundError:
            msg = (
                "Polymarket live SDK is not installed. Install the live extra "
                "with `uv sync --extra live` before enabling LIVE mode."
            )
            raise LiveTradingDisabledError(msg) from None

        try:
            client = _build_sdk_client(sdk, credentials)
            open_orders = _order_states_from_open_orders(
                _get_sdk_open_orders(client),
                credentials=credentials,
            )
            positions = _positions_from_sdk_positions(
                _get_sdk_positions(client, credentials),
                credentials=credentials,
            )
            balances = _get_sdk_balances(client, sdk)
        except Exception as exc:  # noqa: BLE001
            redacted = _redacted_exception_message(exc, credentials)
            logger.warning(
                "Polymarket live account snapshot failed (%s): %s",
                type(exc).__name__,
                redacted,
            )
            msg = (
                "Polymarket live account snapshot failed "
                f"({type(exc).__name__}); venue error redacted"
            )
            raise LiveTradingDisabledError(msg) from None

        return VenueAccountSnapshot(
            balances=balances,
            open_orders=tuple(open_orders),
            positions=tuple(positions),
        )


@dataclass(frozen=True)
class PolymarketVenueAccountReconciler:
    client: LiveVenueAccountClient = field(default_factory=PolymarketSDKClient)
    position_tolerance_shares: float = 1e-6
    notional_tolerance_usdc: float = 1e-4
    cash_tolerance_usdc: float = 1e-4

    async def snapshot(self, credentials: VenueCredentials) -> VenueAccountSnapshot:
        return await self.client.read_account_snapshot(credentials)

    async def compare(
        self,
        db_portfolio: Portfolio,
        venue_snapshot: VenueAccountSnapshot,
    ) -> ReconciliationReport:
        mismatches: list[str] = []
        if venue_snapshot.open_orders:
            mismatches.append(
                f"venue has {len(venue_snapshot.open_orders)} open orders; "
                "PMS has no durable live open-order ledger yet"
            )
        usdc_balance = _venue_usdc_balance(venue_snapshot.balances)
        if (
            usdc_balance is not None
            and usdc_balance + self.cash_tolerance_usdc < db_portfolio.free_usdc
        ):
            mismatches.append(
                "venue USDC balance below PMS free cash: "
                f"venue={usdc_balance:.8f} DB={db_portfolio.free_usdc:.8f}"
            )
        mismatches.extend(
            _compare_positions(
                db_portfolio.open_positions,
                venue_snapshot.positions,
                share_tolerance=self.position_tolerance_shares,
                notional_tolerance=self.notional_tolerance_usdc,
            )
        )
        return ReconciliationReport(ok=not mismatches, mismatches=tuple(mismatches))


@dataclass(frozen=True)
class MissingPolymarketClient:
    async def submit_order(
        self,
        order: PolymarketOrderRequest,
        credentials: VenueCredentials,
    ) -> PolymarketOrderResult:
        del order, credentials
        msg = (
            "Polymarket live client is not configured. Inject a PolymarketClient "
            "or install and wire a venue client before enabling LIVE mode."
        )
        raise LiveTradingDisabledError(msg)


@dataclass(frozen=True)
class PolymarketActuator:
    settings: PMSSettings = field(default_factory=PMSSettings)
    client: PolymarketClient = field(default_factory=MissingPolymarketClient)
    operator_gate: FirstLiveOrderGate = field(default_factory=DenyFirstLiveOrderGate)
    quote_provider: LiveQuoteProvider = field(default_factory=MissingLiveQuoteProvider)
    _approval_state: FirstLiveOrderApprovalState = field(
        default_factory=FirstLiveOrderApprovalState,
        init=False,
        repr=False,
        compare=False,
    )
    _approval_lock: asyncio.Lock = field(
        default_factory=asyncio.Lock,
        init=False,
        repr=False,
        compare=False,
    )

    async def execute(
        self,
        decision: TradeDecision,
        portfolio: Portfolio | None = None,
    ) -> OrderState:
        if not self.settings.live_trading_enabled:
            raise LiveTradingDisabledError("Polymarket live trading is disabled")
        del portfolio
        credentials = validate_live_mode_ready(self.settings)
        request = _order_request(decision)

        # Fast path: once the first order has been confirmed by the venue
        # the floodgates are open and subsequent submits do not serialize
        # on `_approval_lock`. This keeps live throughput unaffected after
        # the one-time gate.
        if self._first_order_approved():
            return await self._submit_with_quote_guard(
                decision=decision,
                request=request,
                credentials=credentials,
            )

        # Slow path: hold `_approval_lock` across the *entire* approval +
        # submit + commit window. The previous implementation released
        # the lock before `submit_order()` and only flipped
        # `_approval_state.approved` after the venue replied — leaving a
        # window where a second concurrent task could re-read the same
        # approval file (which `consume()` had not yet unlinked) and
        # authorize a parallel first-order submit. Holding the lock
        # through submit serializes only the first-order path while
        # preserving "consume-on-success" semantics: if `submit_order`
        # raises, the lock releases without flipping the flag and the
        # next caller will re-prompt the gate.
        async with self._approval_lock:
            # Double-check after acquiring — another task may have just
            # opened the floodgates while we were waiting.
            if self._first_order_approved():
                return await self._submit_with_quote_guard(
                    decision=decision,
                    request=request,
                    credentials=credentials,
                )

            preview = await self._live_order_preview(decision)
            approved = await self.operator_gate.approve_first_order(preview)
            if not approved:
                msg = (
                    "First Polymarket live order requires operator approval: "
                    f"venue={preview.venue} market={preview.market_id} "
                    f"token={preview.token_id} side={preview.side} "
                    f"outcome={preview.outcome} "
                    f"max_notional_usdc={preview.max_notional_usdc} "
                    f"limit_price={preview.limit_price} "
                    f"max_slippage_bps={preview.max_slippage_bps}"
                )
                raise OperatorApprovalRequiredError(msg)

            # Submit while still holding the lock. If this raises, the
            # `async with` releases the lock and `_approval_state.approved`
            # stays False — the next caller will see no commit and
            # re-prompt the gate (correct consume-on-success behavior).
            order_state = await self._submit_with_quote_guard(
                decision=decision,
                request=request,
                credentials=credentials,
            )

            # First venue submission succeeded: lock in first-order
            # completion and consume the operator-side approval artefact
            # so it cannot be replayed by a future restart.
            self._approval_state.approved = True
            try:
                await self.operator_gate.consume(preview)
            except Exception as exc:  # noqa: BLE001
                logger.warning("first-order gate consume failed: %s", exc)

        return order_state

    def _first_order_approved(self) -> bool:
        return self._approval_state.approved

    async def _live_order_preview(self, decision: TradeDecision) -> LiveOrderPreview:
        market_slug, question = await self._preview_market_metadata(decision.market_id)
        return LiveOrderPreview(
            max_notional_usdc=decision.notional_usdc,
            venue=decision.venue,
            market_id=decision.market_id,
            token_id=decision.token_id,
            side=decision.side,
            limit_price=decision.limit_price,
            max_slippage_bps=decision.max_slippage_bps,
            outcome=decision.outcome,
            market_slug=market_slug,
            question=question,
        )

    async def _preview_market_metadata(
        self,
        market_id: str,
    ) -> tuple[str | None, str | None]:
        quote_provider = self.quote_provider
        if isinstance(quote_provider, PolymarketRoutingQuoteProvider):
            quote_provider = quote_provider.snapshot_provider
        if not isinstance(quote_provider, PolymarketBookQuoteProvider):
            return None, None
        market = await quote_provider.store.read_market(market_id)
        if market is None:
            return None, None
        return market.slug, market.question

    async def _submit_with_quote_guard(
        self,
        *,
        decision: TradeDecision,
        request: PolymarketOrderRequest,
        credentials: VenueCredentials,
    ) -> OrderState:
        quote_for_order = getattr(self.quote_provider, "quote_for_order", None)
        if callable(quote_for_order):
            quote = await quote_for_order(
                request,
                credentials,
                first_live_order=not self._first_order_approved(),
            )
        else:
            quote = await self.quote_provider.quote(request, credentials)
        _validate_pre_submit_quote(request, quote, self.settings)
        result = await self.client.submit_order(request, credentials)
        order_state = _order_state_from_result(decision, result, quote=quote)
        _raise_if_unexpected_resting_non_gtc(
            settings=self.settings,
            request=request,
            order_state=order_state,
        )
        return order_state


def _order_request(decision: TradeDecision) -> PolymarketOrderRequest:
    if decision.token_id is None:
        msg = "Polymarket live execution requires decision.token_id"
        raise LiveTradingDisabledError(msg)
    estimated_quantity = _decision_quantity(decision)
    return PolymarketOrderRequest(
        market_id=decision.market_id,
        token_id=decision.token_id,
        side=decision.action if decision.action is not None else decision.side,
        price=decision.limit_price,
        size=_sdk_order_size(decision, estimated_quantity=estimated_quantity),
        notional_usdc=decision.notional_usdc,
        estimated_quantity=estimated_quantity,
        order_type=decision.order_type,
        time_in_force=decision.time_in_force.value,
        max_slippage_bps=decision.max_slippage_bps,
    )


def _order_state_from_result(
    decision: TradeDecision,
    result: PolymarketOrderResult,
    *,
    quote: LivePreSubmitQuote | None = None,
) -> OrderState:
    now = datetime.now(tz=UTC)
    status = result.status or OrderStatus.LIVE.value
    return OrderState(
        order_id=result.order_id or f"polymarket-{uuid4().hex}",
        decision_id=decision.decision_id,
        status=status,
        market_id=decision.market_id,
        token_id=decision.token_id,
        venue=decision.venue,
        requested_notional_usdc=decision.notional_usdc,
        filled_notional_usdc=result.filled_notional_usdc,
        remaining_notional_usdc=result.remaining_notional_usdc,
        fill_price=result.fill_price,
        submitted_at=now,
        last_updated_at=now,
        raw_status=result.raw_status,
        strategy_id=decision.strategy_id,
        strategy_version_id=decision.strategy_version_id,
        filled_quantity=result.filled_quantity,
        pre_submit_quote={} if quote is None else _quote_payload(quote),
        action=decision.action,
        outcome=decision.outcome,
        time_in_force=decision.time_in_force.value,
        intent_key=decision.intent_key,
    )


def _raise_if_unexpected_resting_non_gtc(
    *,
    settings: PMSSettings,
    request: PolymarketOrderRequest,
    order_state: OrderState,
) -> None:
    if (
        settings.mode == RunMode.LIVE
        and request.time_in_force != "GTC"
        and order_state.status == OrderStatus.LIVE.value
        and order_state.remaining_notional_usdc > 1e-9
    ):
        msg = "Non-GTC live order appears resting; requires venue reconciliation"
        raise PolymarketSubmissionUnknownError(msg, order_state=order_state)


def _validate_pre_submit_quote(
    request: PolymarketOrderRequest,
    quote: LivePreSubmitQuote,
    settings: PMSSettings,
) -> None:
    if quote.market_status.lower() != "open":
        msg = f"Polymarket market is not open at submit: {quote.market_status}"
        raise LiveTradingDisabledError(msg)
    if quote.book_age_ms > settings.controller.max_book_age_ms:
        msg = (
            "Polymarket book is stale at submit: "
            f"{quote.book_age_ms:.0f}ms > {settings.controller.max_book_age_ms:.0f}ms"
        )
        raise LiveTradingDisabledError(msg)
    if quote.executable_notional_usdc + 1e-9 < request.notional_usdc:
        msg = (
            "Polymarket executable depth is below requested notional at submit: "
            f"{quote.executable_notional_usdc:.2f} < {request.notional_usdc:.2f}"
        )
        raise LiveTradingDisabledError(msg)
    if request.side == "BUY" and quote.best_executable_price > request.price:
        msg = (
            "Polymarket best executable price exceeds limit at submit: "
            f"{quote.best_executable_price:.4f} > {request.price:.4f}"
        )
        raise LiveTradingDisabledError(msg)
    if request.side == "SELL" and quote.best_executable_price < request.price:
        msg = (
            "Polymarket best executable price is below limit at submit: "
            f"{quote.best_executable_price:.4f} < {request.price:.4f}"
        )
        raise LiveTradingDisabledError(msg)
    if quote.spread_bps > settings.controller.max_spread_bps:
        msg = (
            "Polymarket spread exceeds pre-submit guard: "
            f"{quote.spread_bps:.1f}bps > {settings.controller.max_spread_bps:.1f}bps"
        )
        raise LiveTradingDisabledError(msg)


def _quote_payload(quote: LivePreSubmitQuote) -> dict[str, object]:
    return {
        "market_status": quote.market_status,
        "book_age_ms": quote.book_age_ms,
        "executable_notional_usdc": quote.executable_notional_usdc,
        "best_executable_price": quote.best_executable_price,
        "spread_bps": quote.spread_bps,
        "quote_hash": quote.quote_hash,
        "book_ts": quote.book_ts.isoformat(),
        "source": quote.source,
    }


def _utc_now() -> datetime:
    return datetime.now(tz=UTC)


def _market_status(market: Market, now: datetime) -> str:
    if market.closed is True:
        return "closed"
    if market.active is False:
        return "inactive"
    if market.accepting_orders is False:
        return "not_accepting_orders"
    if market.resolves_at is not None and market.resolves_at <= now:
        return "closed"
    return "open"


def _quote_from_levels(
    *,
    order: PolymarketOrderRequest,
    market_status: str,
    bid_levels: Sequence[BookLevel],
    ask_levels: Sequence[BookLevel],
    quote_hash: str,
    book_ts: datetime,
    now: datetime,
    source: Literal["postgres_snapshot", "venue_direct", "dual"],
) -> LivePreSubmitQuote:
    sorted_bids = sorted(
        bid_levels,
        key=lambda level: level.price,
        reverse=True,
    )
    sorted_asks = sorted(ask_levels, key=lambda level: level.price)
    executable_levels = (
        [level for level in sorted_asks if level.price <= order.price]
        if order.side == "BUY"
        else [level for level in sorted_bids if level.price >= order.price]
    )
    best_executable_price = (
        executable_levels[0].price if executable_levels else order.price
    )
    executable_notional_usdc = sum(
        max(0.0, level.price) * max(0.0, level.size)
        for level in executable_levels
    )
    best_bid = sorted_bids[0].price if sorted_bids else None
    best_ask = sorted_asks[0].price if sorted_asks else None
    return LivePreSubmitQuote(
        market_status=market_status,
        book_age_ms=max(0.0, (now - book_ts).total_seconds() * 1000.0),
        executable_notional_usdc=executable_notional_usdc,
        best_executable_price=best_executable_price,
        spread_bps=_spread_bps(best_bid=best_bid, best_ask=best_ask),
        quote_hash=quote_hash,
        book_ts=book_ts,
        source=source,
    )


def _raise_if_future_book_ts(
    book_ts: datetime,
    *,
    now: datetime,
    allowed_clock_skew_ms: float,
) -> None:
    skew_ms = (book_ts - now).total_seconds() * 1000.0
    if skew_ms <= allowed_clock_skew_ms:
        return
    msg = (
        "Polymarket book snapshot timestamp is in the future; "
        f"refusing live submit ({skew_ms:.0f}ms > {allowed_clock_skew_ms:.0f}ms)"
    )
    raise LiveTradingDisabledError(msg)


def _requires_direct_quote(
    order: PolymarketOrderRequest,
    settings: PMSSettings,
) -> bool:
    threshold = settings.controller.direct_quote_min_notional_usdc
    return threshold is not None and order.notional_usdc >= threshold


def _validate_dual_quote_match(
    snapshot_quote: LivePreSubmitQuote,
    direct_quote: LivePreSubmitQuote,
    settings: PMSSettings,
) -> None:
    delta_bps = _price_delta_bps(
        snapshot_quote.best_executable_price,
        direct_quote.best_executable_price,
    )
    if delta_bps > settings.controller.dual_quote_max_price_delta_bps:
        msg = (
            "Polymarket dual quote mismatch: best executable price delta "
            f"{delta_bps:.1f}bps > "
            f"{settings.controller.dual_quote_max_price_delta_bps:.1f}bps"
        )
        raise LiveTradingDisabledError(msg)


def _price_delta_bps(first: float, second: float) -> float:
    midpoint = (abs(first) + abs(second)) / 2.0
    if midpoint <= 0.0:
        return math.inf if first != second else 0.0
    return (abs(first - second) / midpoint) * 10_000.0


def _spread_bps(*, best_bid: float | None, best_ask: float | None) -> float:
    if best_bid is None or best_ask is None:
        return math.inf
    midpoint = (best_bid + best_ask) / 2.0
    if midpoint <= 0.0:
        return math.inf
    return ((best_ask - best_bid) / midpoint) * 10_000.0


def _decision_quantity(decision: TradeDecision) -> float:
    if decision.limit_price <= 0.0:
        msg = "Polymarket live execution requires a positive limit_price"
        raise LiveTradingDisabledError(msg)
    return decision.notional_usdc / decision.limit_price


def _sdk_order_size(
    decision: TradeDecision,
    *,
    estimated_quantity: float,
) -> float:
    if decision.order_type.lower() == "market" and decision.side == "BUY":
        return decision.notional_usdc
    return estimated_quantity


def _approval_payload_matches(
    payload: dict[str, object],
    preview: LiveOrderPreview,
) -> bool:
    if payload.get("approved") is not True:
        return False
    if payload.get("venue") != preview.venue:
        return False
    if payload.get("market_id") != preview.market_id:
        return False
    if payload.get("token_id") != preview.token_id:
        return False
    if payload.get("side") != preview.side:
        return False
    if payload.get("outcome") != preview.outcome:
        return False
    if payload.get("max_slippage_bps") != preview.max_slippage_bps:
        return False
    if not _float_close(payload.get("max_notional_usdc"), preview.max_notional_usdc):
        return False
    if not _float_close(payload.get("limit_price"), preview.limit_price):
        return False
    return True


def _float_close(value: object, expected: float) -> bool:
    # Operators copy/paste preview values into the approval JSON. Strict `==`
    # comparison fails on harmless float representation differences (e.g.
    # 0.1 + 0.2) and trains the operator to massage values until equality
    # holds. `math.isclose` keeps the gate strict (rel/abs tolerances are
    # tight) without introducing such drift.
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return False
    return math.isclose(float(value), expected, rel_tol=1e-9, abs_tol=1e-12)


def _build_sdk_client(sdk: object, credentials: VenueCredentials) -> object:
    api_creds = getattr(sdk, "ApiCreds")(
        api_key=_required_secret(credentials.api_key, "api_key"),
        api_secret=_required_secret(credentials.api_secret, "api_secret"),
        api_passphrase=_required_secret(
            credentials.api_passphrase,
            "api_passphrase",
        ),
    )
    return getattr(sdk, "ClobClient")(
        host=credentials.host,
        chain_id=credentials.chain_id,
        key=_required_secret(credentials.private_key, "private_key"),
        creds=api_creds,
        signature_type=credentials.signature_type,
        funder=credentials.funder_address,
    )


def _post_sdk_order(
    sdk: object,
    client: object,
    order: PolymarketOrderRequest,
) -> object:
    order_type = _sdk_order_type(getattr(sdk, "OrderType"), order)
    options = getattr(sdk, "PartialCreateOrderOptions")()
    if order.order_type.lower() == "market":
        order_args = getattr(sdk, "MarketOrderArgs")(
            token_id=order.token_id,
            amount=order.size,
            side=_sdk_side(getattr(sdk, "Side"), order.side),
            price=order.price,
            order_type=order_type,
        )
        return getattr(client, "create_and_post_market_order")(
            order_args=order_args,
            options=options,
            order_type=order_type,
        )

    order_args = getattr(sdk, "OrderArgs")(
        token_id=order.token_id,
        price=order.price,
        side=_sdk_side(getattr(sdk, "Side"), order.side),
        size=order.size,
    )
    return getattr(client, "create_and_post_order")(
        order_args=order_args,
        options=options,
        order_type=order_type,
    )


def _get_sdk_order_book(client: object, token_id: str) -> object:
    for method_name in ("get_order_book", "get_orderbook", "get_book"):
        method = getattr(client, method_name, None)
        if not callable(method):
            continue
        try:
            return method(token_id)
        except TypeError:
            return method(token_id=token_id)
    msg = "Polymarket SDK client does not expose an order-book fetch method"
    raise LiveTradingDisabledError(msg)


def _get_sdk_open_orders(client: object) -> object:
    for method_name in ("get_orders", "get_open_orders", "getOpenOrders"):
        method = getattr(client, method_name, None)
        if callable(method):
            return method()
    return ()


def _get_sdk_positions(client: object, credentials: VenueCredentials) -> object:
    for method_name in ("get_positions", "get_current_positions", "getPositions"):
        method = getattr(client, method_name, None)
        if not callable(method):
            continue
        try:
            return method(user=credentials.funder_address)
        except TypeError:
            try:
                return method(credentials.funder_address)
            except TypeError:
                return method()
    return ()


def _get_sdk_balances(client: object, sdk: object) -> dict[str, float]:
    for response in _sdk_balance_responses(client, sdk):
        usdc = _usdc_balance_from_response(response)
        if usdc is not None:
            return {"USDC": usdc}
    return {}


def _sdk_balance_responses(client: object, sdk: object) -> list[object]:
    responses: list[object] = []
    balance_allowance = getattr(client, "get_balance_allowance", None)
    if callable(balance_allowance):
        params_cls = getattr(sdk, "BalanceAllowanceParams", None)
        asset_type_cls = getattr(sdk, "AssetType", None)
        asset_type = getattr(asset_type_cls, "COLLATERAL", "COLLATERAL")
        if callable(params_cls):
            try:
                params = params_cls(asset_type=asset_type)
                responses.append(balance_allowance(params=params))
            except TypeError:
                try:
                    params = params_cls(asset_type=asset_type)
                    responses.append(balance_allowance(params))
                except TypeError:
                    pass
        try:
            responses.append(balance_allowance(asset_type=asset_type))
        except TypeError:
            pass
    for method_name in ("get_balance", "get_balances", "getBalance", "getBalances"):
        method = getattr(client, method_name, None)
        if callable(method):
            try:
                responses.append(method())
            except TypeError:
                pass
    return responses


def _usdc_balance_from_response(response: object) -> float | None:
    direct = _coerce_float_or_none(response)
    if direct is not None:
        return direct
    if isinstance(response, Sequence) and not isinstance(response, (str, bytes)):
        for item in response:
            asset = _response_value(item, "asset", "asset_type", "currency", "token")
            if asset is None or str(asset).upper() in {"USDC", "COLLATERAL"}:
                value = _coerce_float_or_none(
                    _response_value(
                        item,
                        "balance",
                        "available",
                        "available_balance",
                        "cash",
                        "usdc",
                    )
                )
                if value is not None:
                    return value
        return None
    return _coerce_float_or_none(
        _response_value(
            response,
            "balance",
            "available",
            "available_balance",
            "cash",
            "usdc",
            "collateral",
        )
    )


def _sdk_order_type(order_type_cls: object, order: PolymarketOrderRequest) -> object:
    # Polymarket SDK enum: GTC (rest), FAK (fill-and-kill = IOC), FOK (all-or-nothing),
    # GTD (good-till-date). PMS TimeInForce maps GTC→GTC, IOC→FAK, FOK→FOK
    # symmetrically across market and limit orders. Without explicit IOC/FOK
    # mapping for limit orders, an IOC limit silently rests in the book as
    # GTC — a real behaviour change.
    if order.time_in_force == "IOC":
        return getattr(order_type_cls, "FAK")
    if order.time_in_force == "FOK":
        return getattr(order_type_cls, "FOK")
    if order.order_type.lower() == "market":
        # Default for market orders without an explicit TIF is FOK
        # (all-or-nothing) — the SDK's own MarketOrderArgs default.
        return getattr(order_type_cls, "FOK")
    return getattr(order_type_cls, "GTC")


def _sdk_side(side_cls: object, side: str) -> object:
    return getattr(side_cls, side)


def _order_result_from_sdk_response(
    order: PolymarketOrderRequest,
    response: object,
) -> PolymarketOrderResult:
    if _response_value(response, "success") is False or _response_value(
        response,
        "errorMsg",
        "error_msg",
    ):
        msg = "Polymarket live order rejected by venue; venue error redacted"
        raise LiveTradingDisabledError(msg)

    status = str(_response_value(response, "status") or OrderStatus.LIVE.value).lower()

    # Parse explicit partial-fill data from the SDK response. Polymarket
    # can return positive `filled_notional` with status != MATCHED (e.g.
    # an IOC limit that filled half its size and cancelled the rest, or
    # a GTC limit with a partial match still resting in the book). Field
    # names vary across SDK versions; we accept any of the documented
    # aliases and fall back to the status-based heuristic when none are
    # present. Pre-fix, only MATCHED produced fill data — every partial
    # fill was silently dropped.
    # Track raw key presence and coerced value separately. A
    # `_coerce_float_or_none` of "nan"/"inf"/null returns None, but the
    # field IS still in the venue response — we must NOT treat that as
    # "no explicit data" (which would trigger the matched-fallback
    # full-fill synthesis). Reject unparseable values up front.
    notional_field_present = _response_field_present(
        response, "filled_notional_usdc", "filledNotional", "filled_amount"
    )
    raw_notional = _response_value(
        response, "filled_notional_usdc", "filledNotional", "filled_amount"
    )
    explicit_filled_notional = _coerce_float_or_none(raw_notional)
    if notional_field_present and explicit_filled_notional is None:
        msg = (
            "Polymarket reported unparseable filled_notional "
            "(non-finite or null); refusing to persist suspect fill"
        )
        raise LiveTradingDisabledError(msg)

    # `fill_count` is the venue-side contract count alias documented at
    # `docs/research/schema-spec.md:284,307` — order-response contract
    # count used for order-state reconciliation. Including it here
    # closes the bypass codex called out in Round 7 finding f14: a
    # response with `fill_count: "nan"` was previously not seen by the
    # raw-presence check and therefore could route through the matched
    # full-fill synthesis path. NOTE: `price` is intentionally NOT
    # aliased to fill_price — in `py_clob_client_v2.OrderArgs`/
    # `MarketOrderArgs`, `price` is the *request* price, not the fill
    # price. Aliasing it would conflate request and execution data.
    quantity_field_present = _response_field_present(
        response, "filled_quantity", "filledQuantity", "filled_size", "filled", "fill_count"
    )
    raw_quantity = _response_value(
        response, "filled_quantity", "filledQuantity", "filled_size", "filled", "fill_count"
    )
    explicit_filled_quantity = _coerce_float_or_none(raw_quantity)
    if quantity_field_present and explicit_filled_quantity is None:
        msg = (
            "Polymarket reported unparseable filled_quantity "
            "(non-finite or null); refusing to persist suspect fill"
        )
        raise LiveTradingDisabledError(msg)

    price_field_present = _response_field_present(
        response, "fill_price", "fillPrice", "average_price", "avg_price"
    )
    raw_price = _response_value(
        response, "fill_price", "fillPrice", "average_price", "avg_price"
    )
    explicit_fill_price = _coerce_float_or_none(raw_price)
    if price_field_present and explicit_fill_price is None:
        msg = (
            "Polymarket reported unparseable fill_price "
            "(non-finite or null); refusing to persist suspect fill"
        )
        raise LiveTradingDisabledError(msg)

    filled_notional_usdc: float
    filled_quantity: float
    fill_price: float | None

    # Validate every non-None explicit fill field BEFORE branching. A
    # malformed venue response must not be able to corrupt accounting
    # via the matched fallback (which previously trusted explicit values
    # without checking) — a status="matched" response with negative
    # filled_quantity or fill_price=1.5 used to slip through.
    _validate_explicit_fill_fields(
        order=order,
        notional=explicit_filled_notional,
        quantity=explicit_filled_quantity,
        price=explicit_fill_price,
    )

    # Use *raw* presence here, not coerced. A field that arrived but
    # failed to coerce was already rejected above; this flag is for
    # legitimately-set but zero-valued fields (e.g. resting limit with
    # filled_notional=0).
    explicit_field_present = (
        notional_field_present or quantity_field_present or price_field_present
    )

    if explicit_filled_notional is not None and explicit_filled_notional > 0.0:
        # Partial-or-full fill with explicit data from the venue.
        # A positive filled_notional MUST imply positive filled_quantity —
        # a fill of $4 worth of zero shares is contradictory and would
        # corrupt portfolio share accounting (`_fill_from_order` would
        # persist a row with shares=0). Resolve in two passes: first
        # determine fill_price (from explicit or fall back to limit),
        # then derive quantity from notional/price when the venue
        # omitted it; reject explicit-zero quantity with positive
        # notional outright.
        filled_notional_usdc = explicit_filled_notional
        if explicit_fill_price is not None:
            fill_price = explicit_fill_price
        elif (
            explicit_filled_quantity is not None
            and explicit_filled_quantity > 0.0
        ):
            implied_price = filled_notional_usdc / explicit_filled_quantity
            if not (
                _PROBABILITY_PRICE_MIN < implied_price <= _PROBABILITY_PRICE_MAX
            ):
                msg = (
                    "Polymarket implied fill_price (notional/quantity) "
                    "outside (0, 1] range; refusing to persist suspect fill"
                )
                raise LiveTradingDisabledError(msg)
            fill_price = implied_price
        else:
            # Neither explicit price nor quantity available — fall back
            # to the limit price (best estimate). Range-valid by the
            # `TradeDecision.__post_init__` invariant `0 < limit_price < 1`.
            fill_price = order.price
        if explicit_filled_quantity is not None:
            if explicit_filled_quantity == 0.0:
                msg = (
                    "Polymarket reported filled_notional > 0 with explicit "
                    "filled_quantity == 0; refusing to persist suspect fill"
                )
                raise LiveTradingDisabledError(msg)
            filled_quantity = explicit_filled_quantity
        elif fill_price > 0.0:
            # Derive quantity from notional/price so share accounting
            # remains consistent across all venue response shapes.
            filled_quantity = filled_notional_usdc / fill_price
        else:
            # Defensive: should be unreachable given the price-resolution
            # logic above, but raise rather than persist a bad fill.
            msg = (
                "Polymarket fill price resolved to zero; cannot derive "
                "filled_quantity for positive notional"
            )
            raise LiveTradingDisabledError(msg)
    elif status == OrderStatus.MATCHED.value and not explicit_field_present:
        # Backwards-compat: status=matched WITH NO EXPLICIT FILL DATA
        # implies full fill at limit price. The full-fill heuristic must
        # only fire when the venue offered no fill fields at all —
        # otherwise a partial response (e.g. status=matched with explicit
        # filled_notional=0, or with explicit_quantity=0 + price set)
        # would silently synthesize a fake $10 / 0-share fill.
        filled_notional_usdc = order.notional_usdc
        filled_quantity = order.estimated_quantity
        fill_price = order.price
    elif status == OrderStatus.MATCHED.value:
        # Status=matched but the explicit fields disagree with a full
        # fill (zero notional, or partial fields without positive
        # notional). The partial-fill branch above only fires for
        # `explicit_filled_notional > 0`, so reaching here means the
        # response is contradictory. Refuse to fabricate a fill.
        msg = (
            "Polymarket reported status=matched with inconsistent "
            "explicit fill fields (zero notional or partial data); "
            "refusing to persist contradictory fill"
        )
        raise LiveTradingDisabledError(msg)
    else:
        # Live / pending / cancelled with no positive fill — pass
        # through any explicit zeros from the venue without synthesis.
        filled_notional_usdc = 0.0
        filled_quantity = (
            explicit_filled_quantity if explicit_filled_quantity is not None else 0.0
        )
        fill_price = explicit_fill_price

    remaining_notional_usdc = max(0.0, order.notional_usdc - filled_notional_usdc)

    order_id = _response_value(response, "orderID", "order_id", "id")
    return PolymarketOrderResult(
        order_id=str(order_id or ""),
        status=status,
        raw_status=status,
        filled_notional_usdc=filled_notional_usdc,
        remaining_notional_usdc=remaining_notional_usdc,
        fill_price=fill_price,
        filled_quantity=filled_quantity,
    )


def _venue_book_from_sdk_response(
    response: object,
    *,
    market_id: str,
    token_id: str,
) -> LiveVenueBook:
    book_ts = _coerce_datetime_or_now(
        _response_value(response, "timestamp", "ts", "created_at", "createdAt")
    )
    raw_hash = _response_value(response, "hash", "book_hash", "bookHash")
    quote_hash = str(raw_hash or f"venue:{token_id}:{book_ts.isoformat()}")
    market_status = _venue_market_status_from_response(response)
    return LiveVenueBook(
        market_id=market_id,
        token_id=token_id,
        bids=tuple(
            _book_levels_from_response_side(
                _response_value(response, "bids", "buy"),
                market_id=market_id,
                side="BUY",
            )
        ),
        asks=tuple(
            _book_levels_from_response_side(
                _response_value(response, "asks", "sell"),
                market_id=market_id,
                side="SELL",
            )
        ),
        book_ts=book_ts,
        quote_hash=quote_hash,
        market_status=market_status,
    )


def _book_levels_from_response_side(
    raw_levels: object,
    *,
    market_id: str,
    side: Literal["BUY", "SELL"],
) -> list[BookLevel]:
    if raw_levels is None:
        return []
    if not isinstance(raw_levels, Sequence) or isinstance(raw_levels, (str, bytes)):
        msg = "Polymarket order book levels must be a sequence"
        raise LiveTradingDisabledError(msg)
    levels: list[BookLevel] = []
    for raw_level in raw_levels:
        price = _coerce_float_or_none(_response_value(raw_level, "price", "p"))
        size = _coerce_float_or_none(_response_value(raw_level, "size", "s"))
        if price is None or size is None:
            msg = "Polymarket order book level has unparseable price or size"
            raise LiveTradingDisabledError(msg)
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


def _venue_market_status_from_response(response: object) -> str:
    if _coerce_bool_or_none(_response_value(response, "closed", "is_closed")) is True:
        return "closed"
    if _coerce_bool_or_none(_response_value(response, "active", "is_active")) is False:
        return "inactive"
    accepting_orders = _coerce_bool_or_none(
        _response_value(response, "accepting_orders", "acceptingOrders")
    )
    if accepting_orders is False:
        return "not_accepting_orders"
    raw_status = _response_value(response, "market_status", "status")
    if raw_status is None:
        return "open"
    normalized = str(raw_status).strip().lower()
    return normalized or "open"


def _coerce_bool_or_none(value: object) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes", "y"}:
            return True
        if normalized in {"false", "0", "no", "n"}:
            return False
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if value == 1:
            return True
        if value == 0:
            return False
    return None


def _coerce_datetime_or_now(value: object) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=UTC)
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        raw = float(value)
        if raw > 10_000_000_000:
            raw = raw / 1000.0
        return datetime.fromtimestamp(raw, tz=UTC)
    if isinstance(value, str) and value.strip():
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return _utc_now()
        return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)
    return _utc_now()


def _order_states_from_open_orders(
    raw_orders: object,
    *,
    credentials: VenueCredentials,
) -> list[OrderState]:
    if raw_orders is None:
        return []
    if not isinstance(raw_orders, Sequence) or isinstance(raw_orders, (str, bytes)):
        return []
    states: list[OrderState] = []
    for raw_order in raw_orders:
        now = _utc_now()
        order_id = str(_response_value(raw_order, "order_id", "id", "orderID") or "")
        if not order_id:
            continue
        market_id = str(
            _response_value(raw_order, "market_id", "condition_id", "market") or ""
        )
        token_id_value = _response_value(raw_order, "token_id", "asset_id", "assetId")
        remaining = _coerce_float_or_none(
            _response_value(raw_order, "remaining_notional_usdc", "remaining", "size")
        )
        price = _coerce_float_or_none(_response_value(raw_order, "price"))
        remaining_notional = remaining or 0.0
        states.append(
            OrderState(
                order_id=order_id,
                decision_id=f"venue-open-{order_id}",
                status=OrderStatus.LIVE.value,
                market_id=market_id or "unknown",
                token_id=None if token_id_value is None else str(token_id_value),
                venue=credentials.venue,
                requested_notional_usdc=remaining_notional,
                filled_notional_usdc=0.0,
                remaining_notional_usdc=remaining_notional,
                fill_price=price,
                submitted_at=now,
                last_updated_at=now,
                raw_status=str(_response_value(raw_order, "status") or "open"),
                strategy_id="venue",
                strategy_version_id="venue",
            )
        )
    return states


def _positions_from_sdk_positions(
    raw_positions: object,
    *,
    credentials: VenueCredentials,
) -> list[Position]:
    if raw_positions is None:
        return []
    if not isinstance(raw_positions, Sequence) or isinstance(raw_positions, (str, bytes)):
        return []
    positions: list[Position] = []
    for raw_position in raw_positions:
        shares = _coerce_float_or_none(
            _response_value(raw_position, "shares", "size", "quantity", "balance")
        )
        if shares is None or shares <= 0.0:
            continue
        market_id = str(
            _response_value(raw_position, "market_id", "condition_id", "market") or ""
        )
        token_id_value = _response_value(raw_position, "token_id", "asset_id", "assetId")
        avg_price = _coerce_float_or_none(
            _response_value(raw_position, "avg_entry_price", "avgPrice", "price")
        )
        current_price = _coerce_float_or_none(
            _response_value(raw_position, "current_price", "curPrice")
        )
        entry_price = avg_price or current_price or 0.0
        positions.append(
            Position(
                market_id=market_id or "unknown",
                token_id=None if token_id_value is None else str(token_id_value),
                venue=credentials.venue,
                side=str(_response_value(raw_position, "side", "outcome") or "BUY"),
                shares_held=shares,
                avg_entry_price=entry_price,
                unrealized_pnl=0.0,
                locked_usdc=shares * entry_price,
            )
        )
    return positions


def _compare_positions(
    db_positions: Sequence[Position],
    venue_positions: Sequence[Position],
    *,
    share_tolerance: float,
    notional_tolerance: float,
) -> list[str]:
    mismatches: list[str] = []
    db_by_key = {_position_key(position): position for position in db_positions}
    venue_by_key = {_position_key(position): position for position in venue_positions}
    for key in sorted(set(db_by_key) | set(venue_by_key)):
        db_position = db_by_key.get(key)
        venue_position = venue_by_key.get(key)
        if db_position is None:
            mismatches.append(f"venue position missing from DB: {key}")
            continue
        if venue_position is None:
            mismatches.append(f"DB position missing from venue: {key}")
            continue
        if abs(db_position.shares_held - venue_position.shares_held) > share_tolerance:
            mismatches.append(
                "position shares mismatch "
                f"{key}: DB={db_position.shares_held:.8f} "
                f"venue={venue_position.shares_held:.8f}"
            )
        if abs(db_position.locked_usdc - venue_position.locked_usdc) > notional_tolerance:
            mismatches.append(
                "position notional mismatch "
                f"{key}: DB={db_position.locked_usdc:.8f} "
                f"venue={venue_position.locked_usdc:.8f}"
            )
    return mismatches


def _venue_usdc_balance(balances: Mapping[str, float]) -> float | None:
    for key, value in balances.items():
        if key.upper() in {"USDC", "COLLATERAL"}:
            return value
    return None


def _position_key(position: Position) -> tuple[str, str | None, str]:
    return position.market_id, position.token_id, position.venue


def _is_sdk_transport_failure(exc: BaseException) -> bool:
    """Detect SDK exceptions that represent a transport-level failure
    (no HTTP response from the venue) — distinct from the venue
    rejecting the order with an HTTP error response.

    `py_clob_client_v2.exceptions.PolyApiException` takes `resp` in
    its constructor but does NOT retain it as an attribute — the
    instance stores `status_code` (extracted from `resp.status_code`,
    or None if `resp` was None) plus `error_msg`. So the right
    transport-failure signal is `status_code is None`. Verified against
    real SDK 1.0.0:
        PolyApiException(resp=None).__dict__ ==
            {'status_code': None, 'error_msg': ...}
        PolyApiException(resp=<httpx_response>).__dict__ ==
            {'status_code': 400, 'error_msg': ...}

    Duck-typed (class name + attribute) so this module does not take a
    hard import dependency on the SDK exception class.
    """
    if type(exc).__name__ != "PolyApiException":
        return False
    status_code = getattr(exc, "status_code", _MISSING_SENTINEL)
    return status_code is None


_MISSING_SENTINEL: Final[object] = object()


def _validate_explicit_fill_fields(
    *,
    order: PolymarketOrderRequest,
    notional: float | None,
    quantity: float | None,
    price: float | None,
) -> None:
    """Reject malformed venue-supplied fill fields before any branch
    consumes them. Runs ahead of the matched/partial/no-fill branches so
    a `status=matched` response with `filled_quantity=-8.0` (for
    example) cannot be persisted via the backwards-compat fallback.
    """
    if notional is not None:
        if notional < 0.0:
            msg = (
                "Polymarket reported negative filled_notional; refusing "
                "to persist suspect fill"
            )
            raise LiveTradingDisabledError(msg)
        if notional > order.notional_usdc + _NOTIONAL_OVERFILL_TOLERANCE:
            msg = (
                "Polymarket reported filled_notional exceeds requested "
                "notional; refusing to persist suspect fill"
            )
            raise LiveTradingDisabledError(msg)
    if quantity is not None and quantity < 0.0:
        msg = (
            "Polymarket reported negative filled_quantity; refusing to "
            "persist suspect fill"
        )
        raise LiveTradingDisabledError(msg)
    if price is not None and not (
        _PROBABILITY_PRICE_MIN < price <= _PROBABILITY_PRICE_MAX
    ):
        msg = (
            "Polymarket reported fill_price outside (0, 1] range; "
            "refusing to persist suspect fill"
        )
        raise LiveTradingDisabledError(msg)
    # Cross-field consistency: when the venue surfaces all three of
    # `filled_notional_usdc`, `filled_quantity`, and `fill_price` with
    # positive values, the identity `notional == quantity * price` must
    # hold within rounding tolerance. Without this check a malformed
    # triple like (notional=4, quantity=100, price=0.5) — where the
    # true notional should be 50 — passes individual range validation
    # and is silently persisted, corrupting share accounting.
    #
    # `math.isclose` with rel_tol=1% and abs_tol=$0.01 accommodates
    # legitimate venue rounding (Polymarket CLOB matches at discrete
    # prices) while catching the kind of order-of-magnitude divergence
    # the example above exhibits.
    if (
        notional is not None
        and notional > 0.0
        and quantity is not None
        and quantity > 0.0
        and price is not None
        and price > 0.0
        and not math.isclose(notional, quantity * price, rel_tol=0.01, abs_tol=0.01)
    ):
        expected = quantity * price
        msg = (
            "Polymarket reported inconsistent fill triple "
            f"(notional={notional}, quantity={quantity}, price={price}; "
            f"expected notional≈{expected:.4f}); refusing to persist "
            "suspect fill"
        )
        raise LiveTradingDisabledError(msg)


def _coerce_float_or_none(value: object) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        f = float(value)
    elif isinstance(value, str):
        try:
            f = float(value)
        except ValueError:
            return None
    else:
        return None
    # Reject NaN / +/- infinity. Persisting either as `filled_notional`
    # / `filled_quantity` / `fill_price` would silently corrupt the
    # portfolio and downstream metrics. A malformed venue response that
    # parses to a non-finite number must be treated as "no fill data".
    if not math.isfinite(f):
        return None
    return f


_PROBABILITY_PRICE_MIN: Final[float] = 0.0
_PROBABILITY_PRICE_MAX: Final[float] = 1.0
_NOTIONAL_OVERFILL_TOLERANCE: Final[float] = 1e-6


def _response_value(response: object, *keys: str) -> object | None:
    if isinstance(response, Mapping):
        mapping = cast(Mapping[str, object], response)
        for key in keys:
            if key in mapping:
                return mapping[key]
        return None
    for key in keys:
        value = getattr(response, key, None)
        if value is not None:
            return cast(object, value)
    return None


def _response_field_present(response: object, *keys: str) -> bool:
    """Distinct from `_response_value`: returns True iff the venue
    actually surfaced one of the keys (regardless of whether the value
    parses). Used to detect malformed-but-present fields, which must be
    rejected before the matched-fallback can synthesize a full fill.
    """
    if isinstance(response, Mapping):
        mapping = cast(Mapping[str, object], response)
        return any(key in mapping for key in keys)
    return any(hasattr(response, key) for key in keys)


def _required_secret(value: str | None, field_name: str) -> str:
    if value is None or value.strip() == "":
        msg = f"Missing Polymarket credential fields: {field_name}"
        raise LiveTradingDisabledError(msg)
    return value


def _redacted_exception_message(
    error: Exception,
    credentials: VenueCredentials,
) -> str:
    message = str(error)
    for secret in (
        credentials.private_key,
        credentials.api_key,
        credentials.api_secret,
        credentials.api_passphrase,
    ):
        if secret:
            message = message.replace(secret, "[REDACTED]")
    return message
