from __future__ import annotations

import asyncio
import importlib
import json
import logging
import math
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Final, Protocol, cast
from uuid import uuid4

from pms.config import PMSSettings, validate_live_mode_ready
from pms.core.enums import OrderStatus
from pms.core.models import (
    LiveTradingDisabledError,
    OrderState,
    Portfolio,
    TradeDecision,
    VenueCredentials,
)


logger = logging.getLogger(__name__)


class OperatorApprovalRequiredError(LiveTradingDisabledError):
    """Raised when the first live order has not been approved by an operator."""


class PolymarketSubmissionUnknownError(RuntimeError):
    """Raised when a Polymarket order submission timed out — the order may
    have reached the venue. Operators must reconcile before retrying.
    """


@dataclass(frozen=True)
class LiveOrderPreview:
    max_notional_usdc: float
    venue: str
    market_id: str
    token_id: str | None
    side: str
    limit_price: float
    max_slippage_bps: int


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


class PolymarketClient(Protocol):
    async def submit_order(
        self,
        order: PolymarketOrderRequest,
        credentials: VenueCredentials,
    ) -> PolymarketOrderResult: ...


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
            # A timeout means the venue may have accepted the order — we
            # simply did not see the response. Surfacing this as
            # `LiveTradingDisabledError` would falsely imply "nothing was
            # sent". Use a venue-specific exception so callers and
            # operators can distinguish "submitted but unknown" from
            # "rejected / never sent".
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
        # `_require_first_order_approval` returns the preview that was just
        # approved by the operator gate; returns None if a previous successful
        # submit already opened the floodgates. The approval is *not* yet
        # marked as committed — that happens only after a successful submit.
        just_approved_preview = await self._require_first_order_approval(decision)
        request = _order_request(decision)
        result = await self.client.submit_order(request, credentials)
        if just_approved_preview is not None:
            # First venue submission succeeded: lock in first-order completion
            # and consume the operator-side approval artefact so it cannot be
            # replayed.
            self._approval_state.approved = True
            try:
                await self.operator_gate.consume(just_approved_preview)
            except Exception as exc:  # noqa: BLE001
                logger.warning("first-order gate consume failed: %s", exc)
        return _order_state_from_result(decision, result)

    async def _require_first_order_approval(
        self,
        decision: TradeDecision,
    ) -> LiveOrderPreview | None:
        if self._first_order_approved():
            return None
        async with self._approval_lock:
            if self._first_order_approved():
                return None
            preview = LiveOrderPreview(
                max_notional_usdc=decision.notional_usdc,
                venue=decision.venue,
                market_id=decision.market_id,
                token_id=decision.token_id,
                side=decision.side,
                limit_price=decision.limit_price,
                max_slippage_bps=decision.max_slippage_bps,
            )
            approved = await self.operator_gate.approve_first_order(preview)
            if not approved:
                msg = (
                    "First Polymarket live order requires operator approval: "
                    f"venue={preview.venue} market={preview.market_id} "
                    f"token={preview.token_id} side={preview.side} "
                    f"max_notional_usdc={preview.max_notional_usdc} "
                    f"limit_price={preview.limit_price} "
                    f"max_slippage_bps={preview.max_slippage_bps}"
                )
                raise OperatorApprovalRequiredError(msg)
            # NOTE: `_approval_state.approved` is intentionally not set here.
            # If the operator-approved preview never reaches the venue (submit
            # failure), the next `execute()` must re-prompt the gate. The
            # caller marks state.approved=True only after `submit_order`
            # returns successfully.
            return preview

    def _first_order_approved(self) -> bool:
        return self._approval_state.approved


def _order_request(decision: TradeDecision) -> PolymarketOrderRequest:
    if decision.token_id is None:
        msg = "Polymarket live execution requires decision.token_id"
        raise LiveTradingDisabledError(msg)
    estimated_quantity = _decision_quantity(decision)
    return PolymarketOrderRequest(
        market_id=decision.market_id,
        token_id=decision.token_id,
        side=decision.side,
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
    )


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
