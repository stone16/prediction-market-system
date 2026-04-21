from __future__ import annotations

from dataclasses import fields

import pytest

from pms.core.models import FillRecord, OrderState, TradeDecision


def _trade_decision_kwargs(*, notional_usdc: float, limit_price: float) -> dict[str, object]:
    return {
        "decision_id": "decision-1",
        "market_id": "market-1",
        "token_id": "token-1",
        "venue": "polymarket",
        "side": "BUY",
        "notional_usdc": notional_usdc,
        "order_type": "limit",
        "max_slippage_bps": 25,
        "stop_conditions": ["halt-on-resolution"],
        "prob_estimate": 0.61,
        "expected_edge": 0.11,
        "time_in_force": "GTC",
        "opportunity_id": "opp-1",
        "strategy_id": "strategy-1",
        "strategy_version_id": "strategy-version-1",
        "limit_price": limit_price,
    }


@pytest.mark.parametrize("notional_usdc", [-1.0, 0.0, 0.01, 1.0, 100.0, 10000.0])
@pytest.mark.parametrize("limit_price", [-0.1, 0.0, 0.001, 0.5, 0.95, 0.999, 1.0, 1.1])
def test_trade_decision_notional_and_limit_price_invariants(
    *,
    notional_usdc: float,
    limit_price: float,
) -> None:
    kwargs = _trade_decision_kwargs(
        notional_usdc=notional_usdc,
        limit_price=limit_price,
    )

    if notional_usdc <= 0.0:
        with pytest.raises(ValueError, match="notional_usdc"):
            TradeDecision(**kwargs)
        return

    if limit_price <= 0.0 or limit_price >= 1.0:
        with pytest.raises(ValueError, match="limit_price"):
            TradeDecision(**kwargs)
        return

    decision = TradeDecision(**kwargs)

    assert decision.notional_usdc == pytest.approx(notional_usdc)
    assert decision.limit_price == pytest.approx(limit_price)


def test_trade_decision_fields_drop_ambiguous_size_and_price() -> None:
    field_names = {field.name for field in fields(TradeDecision)}

    assert "notional_usdc" in field_names
    assert "limit_price" in field_names
    assert "size" not in field_names
    assert "price" not in field_names


def test_order_state_fields_switch_to_notional_and_quantity() -> None:
    field_names = {field.name for field in fields(OrderState)}

    assert "requested_notional_usdc" in field_names
    assert "filled_notional_usdc" in field_names
    assert "remaining_notional_usdc" in field_names
    assert "filled_quantity" in field_names
    assert "requested_size" not in field_names
    assert "filled_size" not in field_names
    assert "remaining_size" not in field_names


def test_fill_record_fields_switch_to_notional_and_quantity() -> None:
    field_names = {field.name for field in fields(FillRecord)}

    assert "fill_notional_usdc" in field_names
    assert "fill_quantity" in field_names
    assert "fill_size" not in field_names
    assert "filled_contracts" not in field_names
