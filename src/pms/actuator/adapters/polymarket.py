from __future__ import annotations

import asyncio
import importlib
import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol, cast
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


class OperatorApprovalRequiredError(LiveTradingDisabledError):
    """Raised when the first live order has not been approved by an operator."""


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


@dataclass(frozen=True)
class DenyFirstLiveOrderGate:
    async def approve_first_order(self, preview: LiveOrderPreview) -> bool:
        del preview
        return False


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
        except Exception as exc:  # noqa: BLE001
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
    _first_order_approved: bool = field(default=False, init=False, repr=False)

    async def execute(
        self,
        decision: TradeDecision,
        portfolio: Portfolio | None = None,
    ) -> OrderState:
        if not self.settings.live_trading_enabled:
            raise LiveTradingDisabledError("Polymarket live trading is disabled")
        del portfolio
        credentials = validate_live_mode_ready(self.settings)
        await self._require_first_order_approval(decision)
        request = _order_request(decision)
        result = await self.client.submit_order(request, credentials)
        return _order_state_from_result(decision, result)

    async def _require_first_order_approval(self, decision: TradeDecision) -> None:
        if self._first_order_approved:
            return
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
        object.__setattr__(self, "_first_order_approved", True)


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
    expected = {
        "max_notional_usdc": preview.max_notional_usdc,
        "venue": preview.venue,
        "market_id": preview.market_id,
        "token_id": preview.token_id,
        "side": preview.side,
        "limit_price": preview.limit_price,
        "max_slippage_bps": preview.max_slippage_bps,
    }
    return all(payload.get(key) == value for key, value in expected.items())


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
        chain_id=credentials.chain_id or 137,
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
    if order.order_type.lower() == "market":
        if order.time_in_force == "IOC":
            return getattr(order_type_cls, "FAK")
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
    filled_notional_usdc = 0.0
    remaining_notional_usdc = order.notional_usdc
    fill_price: float | None = None
    filled_quantity = 0.0
    if status == OrderStatus.MATCHED.value:
        filled_notional_usdc = order.notional_usdc
        remaining_notional_usdc = 0.0
        fill_price = order.price
        filled_quantity = order.estimated_quantity

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


def _required_secret(value: str | None, field_name: str) -> str:
    if value is None or value.strip() == "":
        msg = f"Missing Polymarket credential fields: {field_name}"
        raise LiveTradingDisabledError(msg)
    return value
