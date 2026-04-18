from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from typing import Any, cast

import pytest

from pms.core.enums import MarketStatus
from pms.core.models import Market, MarketSignal
from pms.factors.base import FactorDefinition, FactorValueRow
from pms.factors.service import FactorService
from pms.sensor.stream import SensorStream


def _signal(*, market_id: str = "factor-service-market") -> MarketSignal:
    return MarketSignal(
        market_id=market_id,
        token_id="yes-token",
        venue="polymarket",
        title="Will FactorService persist factors?",
        yes_price=0.4,
        volume_24h=1000.0,
        resolves_at=datetime(2026, 4, 30, tzinfo=UTC),
        orderbook={
            "bids": [{"price": 0.39, "size": 100.0}],
            "asks": [{"price": 0.41, "size": 50.0}],
        },
        external_signal={"metaculus_prob": 0.7},
        fetched_at=datetime(2026, 4, 18, tzinfo=UTC),
        market_status=MarketStatus.OPEN.value,
    )


class SequenceSignalStream:
    def __init__(self, signals: list[MarketSignal]) -> None:
        self._signals = list(signals)

    def __aiter__(self) -> AsyncIterator[MarketSignal]:
        return self._iterate()

    async def _iterate(self) -> AsyncIterator[MarketSignal]:
        for signal in self._signals:
            yield signal


class FakeStore:
    def __init__(self) -> None:
        self.markets: dict[str, Market] = {}

    async def read_market(self, market_id: str) -> Market | None:
        return self.markets.get(market_id)

    async def write_market(self, market: Market) -> None:
        self.markets[market.condition_id] = market


class PersistedFactor(FactorDefinition):
    factor_id = "persisted_factor"
    required_inputs = ("yes_price",)

    def compute(
        self,
        signal: MarketSignal,
        outer_ring: object,
    ) -> FactorValueRow | None:
        del outer_ring
        return FactorValueRow(
            factor_id=self.factor_id,
            param="",
            market_id=signal.market_id,
            ts=signal.timestamp,
            value=signal.yes_price,
        )


class MissingFactor(FactorDefinition):
    factor_id = "missing_factor"
    required_inputs = ()

    def compute(
        self,
        signal: MarketSignal,
        outer_ring: object,
    ) -> FactorValueRow | None:
        del signal, outer_ring
        return None


@pytest.mark.asyncio
async def test_factor_service_compute_once_persists_non_none_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    persisted: list[FactorValueRow] = []

    async def fake_persist(pool: object, row: FactorValueRow) -> None:
        del pool
        persisted.append(row)

    monkeypatch.setattr("pms.factors.service.persist_factor_value", fake_persist)
    store = FakeStore()
    service = FactorService(
        pool=cast(Any, object()),
        store=cast(Any, store),
        cadence_s=0.1,
        factors=(PersistedFactor, MissingFactor),
        signal_stream=SequenceSignalStream([]),
    )

    count = await service.compute_once([_signal()])

    assert count == 1
    assert persisted == [
        FactorValueRow(
            factor_id="persisted_factor",
            param="",
            market_id="factor-service-market",
            ts=datetime(2026, 4, 18, tzinfo=UTC),
            value=0.4,
        )
    ]
    assert "factor-service-market" in store.markets


@pytest.mark.asyncio
async def test_factor_service_run_exits_when_signal_stream_finishes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    persisted: list[FactorValueRow] = []

    async def fake_persist(pool: object, row: FactorValueRow) -> None:
        del pool
        persisted.append(row)

    monkeypatch.setattr("pms.factors.service.persist_factor_value", fake_persist)
    service = FactorService(
        pool=cast(Any, object()),
        store=cast(Any, FakeStore()),
        cadence_s=0.01,
        factors=(PersistedFactor,),
        signal_stream=SequenceSignalStream([_signal(market_id="run-exit-market")]),
    )

    await asyncio.wait_for(service.run(), timeout=1.0)

    assert [row.market_id for row in persisted] == ["run-exit-market"]


@pytest.mark.asyncio
async def test_factor_service_skips_repersisting_the_same_factor_timestamp(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    persisted: list[FactorValueRow] = []

    async def fake_persist(pool: object, row: FactorValueRow) -> None:
        del pool
        persisted.append(row)

    monkeypatch.setattr("pms.factors.service.persist_factor_value", fake_persist)
    service = FactorService(
        pool=cast(Any, object()),
        store=cast(Any, FakeStore()),
        cadence_s=0.1,
        factors=(PersistedFactor,),
        signal_stream=SequenceSignalStream([]),
    )
    signal = _signal()

    first = await service.compute_once([signal])
    second = await service.compute_once([signal])

    assert first == 1
    assert second == 0
    assert persisted == [
        FactorValueRow(
            factor_id="persisted_factor",
            param="",
            market_id="factor-service-market",
            ts=datetime(2026, 4, 18, tzinfo=UTC),
            value=0.4,
        )
    ]


@pytest.mark.asyncio
async def test_sensor_stream_subscription_receives_signals_without_consuming_main_queue() -> None:
    class OneShotSensor:
        def __aiter__(self) -> AsyncIterator[MarketSignal]:
            return self._iterate()

        async def _iterate(self) -> AsyncIterator[MarketSignal]:
            yield _signal(market_id="tee-market")

    stream = SensorStream()
    subscription = stream.subscribe()

    await stream.start([OneShotSensor()])

    main_signal = await asyncio.wait_for(stream.queue.get(), timeout=1.0)
    tee_signal = await asyncio.wait_for(anext(subscription), timeout=1.0)

    stream.queue.task_done()
    await asyncio.wait_for(stream.stop(), timeout=5.0)

    assert main_signal == tee_signal
    with pytest.raises(StopAsyncIteration):
        await asyncio.wait_for(anext(subscription), timeout=1.0)


@pytest.mark.asyncio
async def test_sensor_stream_closes_late_subscriptions_after_consumers_finish() -> None:
    class OneShotSensor:
        def __aiter__(self) -> AsyncIterator[MarketSignal]:
            return self._iterate()

        async def _iterate(self) -> AsyncIterator[MarketSignal]:
            yield _signal(market_id="late-subscription-market")

    stream = SensorStream()
    await stream.start([OneShotSensor()])
    signal = await asyncio.wait_for(stream.queue.get(), timeout=1.0)
    stream.queue.task_done()
    await asyncio.gather(*stream.tasks, return_exceptions=True)

    late_subscription = stream.subscribe()

    assert signal.market_id == "late-subscription-market"
    with pytest.raises(StopAsyncIteration):
        await asyncio.wait_for(anext(late_subscription), timeout=1.0)
    await asyncio.wait_for(stream.stop(), timeout=5.0)
