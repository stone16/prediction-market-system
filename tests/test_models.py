"""Tests for pms.models — covers instantiation, immutability, and raw field preservation.

Acceptance criteria covered:
- All dataclasses in `pms.models` are importable and instantiable with valid data
- All Protocol classes in `pms.protocols` are importable
- `frozen=True` enforced — mutating a model field raises FrozenInstanceError
- `raw` field preserves arbitrary dict contents
"""

from __future__ import annotations

from dataclasses import FrozenInstanceError
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

# ---------------------------------------------------------------------------
# Imports — must succeed for the package to be considered scaffolded.
# ---------------------------------------------------------------------------

from pms.models import (
    ConnectorFeedback,
    CorrelationPair,
    EvaluationFeedback,
    Market,
    Order,
    OrderBook,
    OrderResult,
    Outcome,
    PerformanceReport,
    PnLReport,
    Position,
    PriceLevel,
    PriceUpdate,
    RiskDecision,
    RiskFeedback,
    StrategyFeedback,
    StrategyMetrics,
)
from pms.protocols import (
    ConnectorProtocol,
    CorrelationDetectorProtocol,
    EmbeddingEngineProtocol,
    ExecutorProtocol,
    FeedbackEngineProtocol,
    MetricsCollectorProtocol,
    RiskManagerProtocol,
    StorageProtocol,
    StrategyProtocol,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_outcome() -> Outcome:
    return Outcome(outcome_id="yes", title="Yes", price=Decimal("0.55"))


def _make_market() -> Market:
    return Market(
        platform="polymarket",
        market_id="m-1",
        title="Will it rain tomorrow?",
        description="Resolves YES if measurable rain falls.",
        outcomes=[_make_outcome(), Outcome(outcome_id="no", title="No", price=Decimal("0.45"))],
        volume=Decimal("12345.67"),
        end_date=datetime(2030, 1, 1, tzinfo=timezone.utc),
        category="weather",
        url="https://example.com/m-1",
        status="open",
        raw={"id": "m-1", "nested": {"k": "v"}, "list": [1, 2, 3]},
    )


def _make_order() -> Order:
    return Order(
        order_id="o-1",
        platform="polymarket",
        market_id="m-1",
        outcome_id="yes",
        side="buy",
        price=Decimal("0.55"),
        size=Decimal("10"),
        order_type="limit",
    )


# ---------------------------------------------------------------------------
# Instantiation tests — every model must accept valid kwargs.
# ---------------------------------------------------------------------------


def test_outcome_instantiation() -> None:
    o = _make_outcome()
    assert o.outcome_id == "yes"
    assert o.title == "Yes"
    assert o.price == Decimal("0.55")


def test_market_instantiation() -> None:
    m = _make_market()
    assert m.platform == "polymarket"
    assert m.market_id == "m-1"
    assert len(m.outcomes) == 2
    assert m.volume == Decimal("12345.67")
    assert m.end_date == datetime(2030, 1, 1, tzinfo=timezone.utc)
    assert m.status == "open"


def test_market_end_date_optional() -> None:
    m = Market(
        platform="kalshi",
        market_id="k-1",
        title="t",
        description="d",
        outcomes=[],
        volume=Decimal("0"),
        end_date=None,
        category="c",
        url="u",
        status="open",
        raw={},
    )
    assert m.end_date is None


def test_price_level_instantiation() -> None:
    pl = PriceLevel(price=Decimal("0.5"), size=Decimal("100"))
    assert pl.price == Decimal("0.5")
    assert pl.size == Decimal("100")


def test_orderbook_instantiation() -> None:
    ob = OrderBook(
        platform="polymarket",
        market_id="m-1",
        bids=[PriceLevel(price=Decimal("0.50"), size=Decimal("10"))],
        asks=[PriceLevel(price=Decimal("0.52"), size=Decimal("20"))],
        timestamp=datetime(2026, 4, 3, tzinfo=timezone.utc),
    )
    assert ob.bids[0].price == Decimal("0.50")
    assert ob.asks[0].size == Decimal("20")


def test_price_update_instantiation() -> None:
    pu = PriceUpdate(
        platform="polymarket",
        market_id="m-1",
        outcome_id="yes",
        bid=Decimal("0.50"),
        ask=Decimal("0.52"),
        last=Decimal("0.51"),
        timestamp=datetime(2026, 4, 3, tzinfo=timezone.utc),
    )
    assert pu.bid == Decimal("0.50")
    assert pu.ask == Decimal("0.52")
    assert pu.last == Decimal("0.51")


def test_order_instantiation() -> None:
    o = _make_order()
    assert o.side == "buy"
    assert o.order_type == "limit"
    assert o.size == Decimal("10")


def test_order_result_instantiation() -> None:
    r = OrderResult(
        order_id="o-1",
        status="filled",
        filled_size=Decimal("10"),
        filled_price=Decimal("0.55"),
        message="ok",
        raw={"venue_id": "abc"},
    )
    assert r.status == "filled"
    assert r.raw["venue_id"] == "abc"


def test_position_instantiation() -> None:
    p = Position(
        platform="polymarket",
        market_id="m-1",
        outcome_id="yes",
        size=Decimal("10"),
        avg_entry_price=Decimal("0.55"),
        unrealized_pnl=Decimal("0.50"),
    )
    assert p.size == Decimal("10")
    assert p.unrealized_pnl == Decimal("0.50")


def test_correlation_pair_instantiation() -> None:
    a = _make_market()
    b = _make_market()
    cp = CorrelationPair(
        market_a=a,
        market_b=b,
        similarity_score=0.92,
        relation_type="overlapping",
        relation_detail="both about weather tomorrow",
        arbitrage_opportunity=Decimal("0.03"),
    )
    assert cp.similarity_score == 0.92
    assert cp.relation_type == "overlapping"
    assert cp.arbitrage_opportunity == Decimal("0.03")


def test_correlation_pair_no_arbitrage() -> None:
    a = _make_market()
    b = _make_market()
    cp = CorrelationPair(
        market_a=a,
        market_b=b,
        similarity_score=0.4,
        relation_type="independent",
        relation_detail="no overlap",
        arbitrage_opportunity=None,
    )
    assert cp.arbitrage_opportunity is None


def test_risk_decision_instantiation() -> None:
    d = RiskDecision(approved=True, reason="within limits", adjusted_size=None)
    assert d.approved is True
    assert d.adjusted_size is None

    d2 = RiskDecision(
        approved=True, reason="size capped to max position", adjusted_size=Decimal("5")
    )
    assert d2.adjusted_size == Decimal("5")


def test_strategy_feedback_instantiation() -> None:
    sf = StrategyFeedback(
        pnl=12.5, win_rate=0.62, avg_slippage=0.003, suggestion="raise_min_spread"
    )
    assert sf.pnl == 12.5
    assert sf.win_rate == 0.62


def test_risk_feedback_instantiation() -> None:
    rf = RiskFeedback(
        max_drawdown_hit=False,
        current_exposure=Decimal("250.00"),
        suggestion="hold",
    )
    assert rf.max_drawdown_hit is False
    assert rf.current_exposure == Decimal("250.00")


def test_connector_feedback_instantiation() -> None:
    cf = ConnectorFeedback(
        data_staleness_ms=120.0,
        api_error_rate=0.01,
        suggestion="ok",
    )
    assert cf.data_staleness_ms == 120.0
    assert cf.api_error_rate == 0.01


def test_evaluation_feedback_instantiation() -> None:
    sf = StrategyFeedback(pnl=1.0, win_rate=0.5, avg_slippage=0.001, suggestion="ok")
    rf = RiskFeedback(
        max_drawdown_hit=False, current_exposure=Decimal("1"), suggestion="ok"
    )
    cf = ConnectorFeedback(data_staleness_ms=10.0, api_error_rate=0.0, suggestion="ok")
    fb = EvaluationFeedback(
        timestamp=datetime(2026, 4, 3, tzinfo=timezone.utc),
        period=timedelta(hours=1),
        strategy_adjustments={"arb": sf},
        risk_adjustments=rf,
        connector_adjustments={"polymarket": cf},
    )
    assert fb.period == timedelta(hours=1)
    assert fb.strategy_adjustments["arb"].pnl == 1.0
    assert fb.connector_adjustments["polymarket"].api_error_rate == 0.0


def test_pnl_report_instantiation() -> None:
    r = PnLReport(
        start=datetime(2026, 4, 1, tzinfo=timezone.utc),
        end=datetime(2026, 4, 3, tzinfo=timezone.utc),
        realized=Decimal("100.00"),
        unrealized=Decimal("12.50"),
        total=Decimal("112.50"),
        num_trades=4,
    )
    assert r.realized == Decimal("100.00")
    assert r.total == Decimal("112.50")
    assert r.num_trades == 4


def test_strategy_metrics_instantiation() -> None:
    sm = StrategyMetrics(
        strategy_name="arb",
        num_orders=10,
        num_fills=7,
        win_rate=0.7,
        avg_slippage=0.002,
        avg_fill_latency_ms=45.0,
        pnl=12.5,
    )
    assert sm.strategy_name == "arb"
    assert sm.num_orders == 10
    assert sm.num_fills == 7
    assert sm.win_rate == 0.7
    assert sm.avg_fill_latency_ms == 45.0


def test_performance_report_instantiation() -> None:
    sm = StrategyMetrics(
        strategy_name="arb",
        num_orders=1,
        num_fills=1,
        win_rate=1.0,
        avg_slippage=0.0,
        avg_fill_latency_ms=0.0,
        pnl=1.0,
    )
    pr = PerformanceReport(
        start=datetime(2026, 4, 1, tzinfo=timezone.utc),
        end=datetime(2026, 4, 2, tzinfo=timezone.utc),
        per_strategy={"arb": sm},
    )
    assert pr.per_strategy["arb"].pnl == 1.0
    assert pr.start == datetime(2026, 4, 1, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# Immutability — every model must reject mutation.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "make,attr,value",
    [
        (lambda: _make_outcome(), "title", "No"),
        (lambda: _make_market(), "title", "Other"),
        (
            lambda: PriceLevel(price=Decimal("0.5"), size=Decimal("1")),
            "size",
            Decimal("2"),
        ),
        (
            lambda: OrderBook(
                platform="p",
                market_id="m",
                bids=[],
                asks=[],
                timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
            ),
            "platform",
            "q",
        ),
        (
            lambda: PriceUpdate(
                platform="p",
                market_id="m",
                outcome_id="y",
                bid=Decimal("0"),
                ask=Decimal("0"),
                last=Decimal("0"),
                timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
            ),
            "bid",
            Decimal("1"),
        ),
        (lambda: _make_order(), "size", Decimal("99")),
        (
            lambda: OrderResult(
                order_id="o",
                status="filled",
                filled_size=Decimal("0"),
                filled_price=Decimal("0"),
                message="",
                raw={},
            ),
            "status",
            "rejected",
        ),
        (
            lambda: Position(
                platform="p",
                market_id="m",
                outcome_id="y",
                size=Decimal("0"),
                avg_entry_price=Decimal("0"),
                unrealized_pnl=Decimal("0"),
            ),
            "size",
            Decimal("1"),
        ),
        (
            lambda: CorrelationPair(
                market_a=_make_market(),
                market_b=_make_market(),
                similarity_score=0.5,
                relation_type="independent",
                relation_detail="",
                arbitrage_opportunity=None,
            ),
            "similarity_score",
            0.9,
        ),
        (
            lambda: RiskDecision(approved=True, reason="ok", adjusted_size=None),
            "approved",
            False,
        ),
        (
            lambda: StrategyFeedback(
                pnl=0.0, win_rate=0.0, avg_slippage=0.0, suggestion=""
            ),
            "pnl",
            1.0,
        ),
        (
            lambda: RiskFeedback(
                max_drawdown_hit=False,
                current_exposure=Decimal("0"),
                suggestion="",
            ),
            "max_drawdown_hit",
            True,
        ),
        (
            lambda: ConnectorFeedback(
                data_staleness_ms=0.0, api_error_rate=0.0, suggestion=""
            ),
            "api_error_rate",
            1.0,
        ),
        (
            lambda: PnLReport(
                start=datetime(2026, 1, 1, tzinfo=timezone.utc),
                end=datetime(2026, 1, 2, tzinfo=timezone.utc),
                realized=Decimal("0"),
                unrealized=Decimal("0"),
                total=Decimal("0"),
                num_trades=0,
            ),
            "realized",
            Decimal("1"),
        ),
        (
            lambda: StrategyMetrics(
                strategy_name="s",
                num_orders=0,
                num_fills=0,
                win_rate=0.0,
                avg_slippage=0.0,
                avg_fill_latency_ms=0.0,
                pnl=0.0,
            ),
            "win_rate",
            1.0,
        ),
        (
            lambda: PerformanceReport(
                start=datetime(2026, 1, 1, tzinfo=timezone.utc),
                end=datetime(2026, 1, 2, tzinfo=timezone.utc),
                per_strategy={},
            ),
            "per_strategy",
            {},
        ),
    ],
)
def test_model_is_frozen(make, attr, value) -> None:  # type: ignore[no-untyped-def]
    instance = make()
    with pytest.raises(FrozenInstanceError):
        setattr(instance, attr, value)


def test_evaluation_feedback_is_frozen() -> None:
    sf = StrategyFeedback(pnl=0.0, win_rate=0.0, avg_slippage=0.0, suggestion="")
    rf = RiskFeedback(
        max_drawdown_hit=False, current_exposure=Decimal("0"), suggestion=""
    )
    cf = ConnectorFeedback(data_staleness_ms=0.0, api_error_rate=0.0, suggestion="")
    fb = EvaluationFeedback(
        timestamp=datetime(2026, 4, 3, tzinfo=timezone.utc),
        period=timedelta(seconds=60),
        strategy_adjustments={"a": sf},
        risk_adjustments=rf,
        connector_adjustments={"p": cf},
    )
    with pytest.raises(FrozenInstanceError):
        fb.period = timedelta(seconds=120)  # type: ignore[misc]


# ---------------------------------------------------------------------------
# `raw` field preservation — must store arbitrary dict contents losslessly.
# ---------------------------------------------------------------------------


def test_market_raw_preserves_arbitrary_dict() -> None:
    raw_payload: dict[str, object] = {
        "string": "value",
        "int": 42,
        "float": 3.14,
        "bool": True,
        "none": None,
        "nested": {"a": [1, 2, {"b": "c"}]},
        "list": [{"k": "v"}, {"k2": "v2"}],
    }
    m = Market(
        platform="polymarket",
        market_id="m-2",
        title="t",
        description="d",
        outcomes=[],
        volume=Decimal("0"),
        end_date=None,
        category="c",
        url="u",
        status="open",
        raw=raw_payload,
    )
    assert m.raw == raw_payload
    # Identity preserved (no defensive copy required by spec). Cast through
    # ``object`` because ``raw`` is intentionally heterogeneous.
    nested = m.raw["nested"]
    assert isinstance(nested, dict)
    a = nested["a"]
    assert isinstance(a, list)
    leaf = a[2]
    assert isinstance(leaf, dict)
    assert leaf["b"] == "c"


def test_order_result_raw_preserves_arbitrary_dict() -> None:
    raw_payload: dict[str, object] = {"venue_id": "x", "fees": {"taker": 0.001}}
    r = OrderResult(
        order_id="o",
        status="partial",
        filled_size=Decimal("5"),
        filled_price=Decimal("0.5"),
        message="partial fill",
        raw=raw_payload,
    )
    assert r.raw == raw_payload


# ---------------------------------------------------------------------------
# Protocol importability — every Protocol must be importable.
# (The mere fact that the imports above succeed proves importability;
# these assertions document the contract explicitly.)
# ---------------------------------------------------------------------------


def test_protocols_are_importable() -> None:
    # Each Protocol must be a class object.
    for proto in (
        ConnectorProtocol,
        EmbeddingEngineProtocol,
        CorrelationDetectorProtocol,
        StrategyProtocol,
        ExecutorProtocol,
        RiskManagerProtocol,
        MetricsCollectorProtocol,
        FeedbackEngineProtocol,
        StorageProtocol,
    ):
        assert isinstance(proto, type)
