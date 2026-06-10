"""Evaluator -> Controller calibration feedback edge (spool side).

The spool pushes every *resolved* EvalRecord to a runner-injected
``calibration_sink`` after the record is durably persisted. Persist-then-push
ordering is load-bearing: restart re-hydration reads the eval store, so a
record must never reach a calibrator without also being on disk.
"""

from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import UTC, datetime
from typing import cast

import pytest

from pms.core.enums import OrderStatus, Side, TimeInForce
from pms.core.models import EvalRecord, FillRecord, TradeDecision
from pms.evaluation.adapters.scoring import Scorer
from pms.evaluation.spool import EvalSpool
from pms.storage.eval_store import EvalStore


def _decision(*, decision_id: str = "d-sink") -> TradeDecision:
    return TradeDecision(
        decision_id=decision_id,
        market_id="m-sink",
        token_id="t-yes",
        venue="polymarket",
        side=Side.BUY.value,
        limit_price=0.4,
        notional_usdc=4.0,
        order_type="limit",
        max_slippage_bps=100,
        stop_conditions=["min_volume:100.00"],
        prob_estimate=0.7,
        expected_edge=0.3,
        time_in_force=TimeInForce.GTC,
        opportunity_id=f"op-{decision_id}",
        strategy_id="default",
        strategy_version_id="default-v1",
        model_id="model-a",
    )


def _fill(
    *,
    decision_id: str = "d-sink",
    resolved_outcome: float | None = 1.0,
) -> FillRecord:
    now = datetime(2026, 6, 10, tzinfo=UTC)
    return FillRecord(
        trade_id=f"trade-{decision_id}",
        order_id=f"order-{decision_id}",
        decision_id=decision_id,
        market_id="m-sink",
        token_id="t-yes",
        venue="polymarket",
        side=Side.BUY.value,
        fill_price=0.42,
        fill_notional_usdc=4.2,
        fill_quantity=10.0,
        executed_at=now,
        filled_at=now,
        status=OrderStatus.MATCHED.value,
        anomaly_flags=[],
        strategy_id="default",
        strategy_version_id="default-v1",
        resolved_outcome=resolved_outcome,
    )


class _OrderRecordingEvalStore:
    def __init__(self, events: list[str]) -> None:
        self._events = events
        self.records: list[EvalRecord] = []

    async def append(self, record: EvalRecord) -> None:
        self._events.append(f"append:{record.decision_id}")
        self.records.append(record)


@pytest.mark.asyncio
async def test_calibration_sink_receives_resolved_record_after_persistence() -> None:
    events: list[str] = []
    store = _OrderRecordingEvalStore(events)
    sunk: list[EvalRecord] = []

    def sink(record: EvalRecord) -> None:
        events.append(f"sink:{record.decision_id}")
        sunk.append(record)

    spool = EvalSpool(
        store=cast(EvalStore, store),
        scorer=Scorer(),
        calibration_sink=sink,
    )
    await spool.start()
    try:
        spool.enqueue(_fill(decision_id="d-1"), _decision(decision_id="d-1"))
        await asyncio.wait_for(spool.join(), timeout=1.0)
    finally:
        await spool.stop()

    assert events == ["append:d-1", "sink:d-1"]
    assert [record.decision_id for record in sunk] == ["d-1"]
    assert sunk[0].model_id == "model-a"
    assert sunk[0].resolved_outcome == 1.0


@pytest.mark.asyncio
async def test_calibration_sink_not_called_for_unresolved_fill() -> None:
    events: list[str] = []
    store = _OrderRecordingEvalStore(events)
    spool = EvalSpool(
        store=cast(EvalStore, store),
        scorer=Scorer(),
        calibration_sink=lambda record: events.append(f"sink:{record.decision_id}"),
    )
    await spool.start()
    try:
        spool.enqueue(
            _fill(decision_id="d-unresolved", resolved_outcome=None),
            _decision(decision_id="d-unresolved"),
        )
        await asyncio.wait_for(spool.join(), timeout=1.0)
    finally:
        await spool.stop()

    assert events == []


@pytest.mark.asyncio
async def test_calibration_sink_fires_for_sweep_style_late_resolved_reenqueue() -> None:
    """Coordination contract with feat/resolution-ingestion: live PAPER/LIVE
    fills are unresolved at enqueue time, and the resolution sweep later
    re-enqueues ``replace(fill, resolved_outcome=...)`` through the same
    ``EvalSpool.enqueue``. The sink must fire for any record passing ``_run``
    with ``resolved_outcome`` set, regardless of which producer enqueued it."""
    events: list[str] = []
    store = _OrderRecordingEvalStore(events)
    sunk: list[EvalRecord] = []

    def sink(record: EvalRecord) -> None:
        events.append(f"sink:{record.decision_id}")
        sunk.append(record)

    spool = EvalSpool(
        store=cast(EvalStore, store),
        scorer=Scorer(),
        calibration_sink=sink,
    )
    unresolved_fill = _fill(decision_id="d-sweep", resolved_outcome=None)
    decision = _decision(decision_id="d-sweep")
    await spool.start()
    try:
        # Fill-time enqueue: market not yet resolved — no eval row, no sink.
        spool.enqueue(unresolved_fill, decision)
        await asyncio.wait_for(spool.join(), timeout=1.0)
        assert events == []

        # Sweep-time re-enqueue: same fill, now carrying the resolution.
        spool.enqueue(replace(unresolved_fill, resolved_outcome=1.0), decision)
        await asyncio.wait_for(spool.join(), timeout=1.0)
    finally:
        await spool.stop()

    assert events == ["append:d-sweep", "sink:d-sweep"]
    assert [record.decision_id for record in sunk] == ["d-sweep"]
    assert sunk[0].resolved_outcome == 1.0


@pytest.mark.asyncio
async def test_spool_survives_raising_calibration_sink() -> None:
    events: list[str] = []
    store = _OrderRecordingEvalStore(events)

    def raising_sink(record: EvalRecord) -> None:
        events.append(f"sink:{record.decision_id}")
        msg = "calibrator exploded"
        raise RuntimeError(msg)

    spool = EvalSpool(
        store=cast(EvalStore, store),
        scorer=Scorer(),
        calibration_sink=raising_sink,
    )
    await spool.start()
    try:
        spool.enqueue(_fill(decision_id="d-boom"), _decision(decision_id="d-boom"))
        spool.enqueue(_fill(decision_id="d-after"), _decision(decision_id="d-after"))
        await asyncio.wait_for(spool.join(), timeout=1.0)
    finally:
        await spool.stop()

    # Eval persistence happened for both records despite the sink raising.
    assert events == [
        "append:d-boom",
        "sink:d-boom",
        "append:d-after",
        "sink:d-after",
    ]
