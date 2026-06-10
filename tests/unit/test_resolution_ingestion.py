from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import httpx
import pytest

from pms.config import DatabaseSettings, PMSSettings, RiskSettings
from pms.core.enums import OrderStatus, RunMode, Side, TimeInForce
from pms.core.models import FillRecord, MarketSignal, TradeDecision
from pms.evaluation.resolution import (
    GammaResolutionSource,
    ResolutionSweeper,
    ResolutionSweepResult,
)
from pms.runner import Runner
from pms.storage.fill_store import FillStore
from tests.support.fake_stores import InMemoryEvalStore


FIXTURE_PATH = Path("tests/fixtures/polymarket_7day_synthetic.jsonl")


def _decision(
    *,
    decision_id: str = "d-res-1",
    market_id: str = "0xmarket-1",
    prob: float = 0.7,
    price: float = 0.4,
    strategy_id: str = "default",
    strategy_version_id: str = "default-v1",
) -> TradeDecision:
    return TradeDecision(
        decision_id=decision_id,
        market_id=market_id,
        token_id="t-yes",
        venue="polymarket",
        side=Side.BUY.value,
        limit_price=price,
        notional_usdc=price * 10.0,
        order_type="limit",
        max_slippage_bps=100,
        stop_conditions=["min_volume:100.00"],
        prob_estimate=prob,
        expected_edge=prob - price,
        time_in_force=TimeInForce.GTC,
        opportunity_id=f"op-{decision_id}",
        strategy_id=strategy_id,
        strategy_version_id=strategy_version_id,
        model_id="model-a",
    )


def _fill(
    *,
    fill_id: str = "fill-res-1",
    decision_id: str = "d-res-1",
    market_id: str = "0xmarket-1",
    resolved_outcome: float | None = None,
    strategy_id: str = "default",
    strategy_version_id: str = "default-v1",
) -> FillRecord:
    now = datetime(2026, 6, 1, tzinfo=UTC)
    return FillRecord(
        trade_id=f"trade-{fill_id}",
        fill_id=fill_id,
        order_id=f"order-{fill_id}",
        decision_id=decision_id,
        market_id=market_id,
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
        strategy_id=strategy_id,
        strategy_version_id=strategy_version_id,
        resolved_outcome=resolved_outcome,
    )


class FakeResolutionFillStore:
    def __init__(self, fills: Sequence[FillRecord]) -> None:
        self.fills: dict[str, FillRecord] = {
            fill.fill_id or fill.trade_id: fill for fill in fills
        }
        self.resolve_calls: list[tuple[str, float]] = []

    async def read_unresolved_fills(self) -> list[FillRecord]:
        return [
            fill for fill in self.fills.values() if fill.resolved_outcome is None
        ]

    async def resolve_fill(self, fill_id: str, *, resolved_outcome: float) -> bool:
        self.resolve_calls.append((fill_id, resolved_outcome))
        fill = self.fills.get(fill_id)
        if fill is None or fill.resolved_outcome is not None:
            return False
        self.fills[fill_id] = replace(fill, resolved_outcome=resolved_outcome)
        return True


class AlreadyResolvedElsewhereFillStore(FakeResolutionFillStore):
    """Simulates a concurrent writer winning the resolved-outcome update."""

    async def resolve_fill(self, fill_id: str, *, resolved_outcome: float) -> bool:
        self.resolve_calls.append((fill_id, resolved_outcome))
        return False


@dataclass(frozen=True)
class _StoredDecision:
    decision: TradeDecision
    decision_evidence: Mapping[str, Any]


class FakeDecisionReader:
    def __init__(self, rows: Mapping[str, _StoredDecision]) -> None:
        self._rows = dict(rows)

    async def get_decision(self, decision_id: str) -> _StoredDecision | None:
        return self._rows.get(decision_id)


class FakeResolutionSource:
    def __init__(self, resolutions: Mapping[str, float]) -> None:
        self.resolutions = dict(resolutions)
        self.calls: list[list[str]] = []

    async def fetch_resolutions(
        self,
        condition_ids: Sequence[str],
    ) -> Mapping[str, float]:
        self.calls.append(list(condition_ids))
        return dict(self.resolutions)


def _sweeper(
    *,
    fill_store: FakeResolutionFillStore,
    eval_store: InMemoryEvalStore,
    source: FakeResolutionSource,
    decisions: Mapping[str, _StoredDecision],
) -> ResolutionSweeper:
    return ResolutionSweeper(
        fill_store=fill_store,
        decision_reader=FakeDecisionReader(decisions),
        eval_store=cast(Any, eval_store),
        resolution_source=source,
    )


@pytest.mark.asyncio
async def test_sweep_scores_resolved_fill_and_updates_fill() -> None:
    fill_store = FakeResolutionFillStore([_fill()])
    eval_store = InMemoryEvalStore()
    source = FakeResolutionSource({"0xmarket-1": 1.0})
    sweeper = _sweeper(
        fill_store=fill_store,
        eval_store=eval_store,
        source=source,
        decisions={
            "d-res-1": _StoredDecision(
                decision=_decision(),
                decision_evidence={
                    "mid_quote_baseline_prob_estimate": 0.45,
                    "last_trade_baseline_prob_estimate": 0.43,
                },
            )
        },
    )

    result = await sweeper.sweep_once()

    assert result == ResolutionSweepResult(
        unresolved_fills=1,
        fills_resolved=1,
        eval_records_appended=1,
    )
    assert fill_store.resolve_calls == [("fill-res-1", 1.0)]
    assert fill_store.fills["fill-res-1"].resolved_outcome == 1.0
    records = await eval_store.all()
    assert len(records) == 1
    record = records[0]
    assert record.decision_id == "d-res-1"
    assert record.market_id == "0xmarket-1"
    assert record.resolved_outcome == 1.0
    assert record.brier_score == pytest.approx((0.7 - 1.0) ** 2)
    # Baseline-Brier inputs are recovered from persisted decision evidence,
    # matching the live EvalSpool scoring path.
    assert record.baseline_prob_estimates == {
        "market_implied": 0.4,
        "mid_quote": 0.45,
        "last_trade": 0.43,
    }
    assert record.baseline_brier_scores["mid_quote"] == pytest.approx(
        (0.45 - 1.0) ** 2
    )
    assert record.citations == ["trade-fill-res-1"]


@pytest.mark.asyncio
async def test_sweep_leaves_fills_for_unresolved_markets_untouched() -> None:
    fill_store = FakeResolutionFillStore([_fill()])
    eval_store = InMemoryEvalStore()
    source = FakeResolutionSource({})
    sweeper = _sweeper(
        fill_store=fill_store,
        eval_store=eval_store,
        source=source,
        decisions={
            "d-res-1": _StoredDecision(
                decision=_decision(),
                decision_evidence={},
            )
        },
    )

    result = await sweeper.sweep_once()

    assert result == ResolutionSweepResult(
        unresolved_fills=1,
        fills_resolved=0,
        eval_records_appended=0,
    )
    assert fill_store.resolve_calls == []
    assert fill_store.fills["fill-res-1"].resolved_outcome is None
    assert await eval_store.all() == []
    assert source.calls == [["0xmarket-1"]]


@pytest.mark.asyncio
async def test_second_sweep_does_not_duplicate_eval_records() -> None:
    fill_store = FakeResolutionFillStore([_fill()])
    eval_store = InMemoryEvalStore()
    source = FakeResolutionSource({"0xmarket-1": 0.0})
    sweeper = _sweeper(
        fill_store=fill_store,
        eval_store=eval_store,
        source=source,
        decisions={
            "d-res-1": _StoredDecision(
                decision=_decision(),
                decision_evidence={},
            )
        },
    )

    first = await sweeper.sweep_once()
    second = await sweeper.sweep_once()

    assert first.eval_records_appended == 1
    assert second == ResolutionSweepResult(
        unresolved_fills=0,
        fills_resolved=0,
        eval_records_appended=0,
    )
    assert len(await eval_store.all()) == 1
    # The second sweep finds no unresolved fills, so Gamma is not re-polled.
    assert source.calls == [["0xmarket-1"]]


@pytest.mark.asyncio
async def test_sweep_skips_scoring_when_resolve_update_loses_race() -> None:
    fill_store = AlreadyResolvedElsewhereFillStore([_fill()])
    eval_store = InMemoryEvalStore()
    sweeper = _sweeper(
        fill_store=fill_store,
        eval_store=eval_store,
        source=FakeResolutionSource({"0xmarket-1": 1.0}),
        decisions={
            "d-res-1": _StoredDecision(
                decision=_decision(),
                decision_evidence={},
            )
        },
    )

    result = await sweeper.sweep_once()

    assert result == ResolutionSweepResult(
        unresolved_fills=1,
        fills_resolved=0,
        eval_records_appended=0,
    )
    assert await eval_store.all() == []


@pytest.mark.asyncio
async def test_sweep_resolves_fill_without_recoverable_decision_but_skips_eval() -> None:
    fill_store = FakeResolutionFillStore([_fill()])
    eval_store = InMemoryEvalStore()
    sweeper = _sweeper(
        fill_store=fill_store,
        eval_store=eval_store,
        source=FakeResolutionSource({"0xmarket-1": 1.0}),
        decisions={},
    )

    result = await sweeper.sweep_once()

    assert result == ResolutionSweepResult(
        unresolved_fills=1,
        fills_resolved=1,
        eval_records_appended=0,
    )
    assert fill_store.fills["fill-res-1"].resolved_outcome == 1.0
    assert await eval_store.all() == []


@pytest.mark.asyncio
async def test_sweep_continues_after_scoring_failure_for_one_fill() -> None:
    fill_store = FakeResolutionFillStore(
        [
            _fill(fill_id="fill-bad", decision_id="d-bad", market_id="0xmarket-1"),
            _fill(fill_id="fill-good", decision_id="d-good", market_id="0xmarket-2"),
        ]
    )
    eval_store = InMemoryEvalStore()
    sweeper = _sweeper(
        fill_store=fill_store,
        eval_store=eval_store,
        source=FakeResolutionSource({"0xmarket-1": 1.0, "0xmarket-2": 1.0}),
        decisions={
            # Strategy identity mismatch makes Scorer.score raise for this fill.
            "d-bad": _StoredDecision(
                decision=_decision(
                    decision_id="d-bad",
                    strategy_id="other-strategy",
                ),
                decision_evidence={},
            ),
            "d-good": _StoredDecision(
                decision=_decision(
                    decision_id="d-good",
                    market_id="0xmarket-2",
                ),
                decision_evidence={},
            ),
        },
    )

    result = await sweeper.sweep_once()

    records = await eval_store.all()
    assert [record.decision_id for record in records] == ["d-good"]
    assert result.unresolved_fills == 2
    assert result.eval_records_appended == 1


def _gamma_market_row(
    condition_id: str,
    *,
    closed: bool,
    outcomes: Sequence[str] = ("Yes", "No"),
    prices: Sequence[str] = ("1", "0"),
) -> dict[str, Any]:
    return {
        "conditionId": condition_id,
        "closed": closed,
        "outcomes": json.dumps(list(outcomes)),
        "outcomePrices": json.dumps(list(prices)),
    }


def _gamma_client(handler: Any) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="https://gamma-api.test",
    )


@pytest.mark.asyncio
async def test_gamma_resolution_source_parses_settled_closed_markets() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            json=[
                _gamma_market_row("0xresolved-yes", closed=True, prices=("1", "0")),
                _gamma_market_row(
                    "0xresolved-no-reversed",
                    closed=True,
                    outcomes=("No", "Yes"),
                    prices=("1", "0"),
                ),
                # Closed but not settled: near-resolution trade prices must
                # not be admitted as Brier ground truth.
                _gamma_market_row(
                    "0xclosed-unsettled",
                    closed=True,
                    prices=("0.97", "0.03"),
                ),
                _gamma_market_row("0xstill-open", closed=False, prices=("1", "0")),
                {
                    "conditionId": "0xnon-binary",
                    "closed": True,
                    "outcomes": json.dumps(["A", "B", "C"]),
                    "outcomePrices": json.dumps(["1", "0", "0"]),
                },
            ],
        )

    async with _gamma_client(handler) as client:
        source = GammaResolutionSource(http_client=client)
        resolutions = await source.fetch_resolutions(
            [
                "0xresolved-yes",
                "0xresolved-no-reversed",
                "0xclosed-unsettled",
                "0xstill-open",
                "0xnon-binary",
            ]
        )

    assert resolutions == {
        "0xresolved-yes": 1.0,
        "0xresolved-no-reversed": 0.0,
    }
    assert len(requests) == 1
    assert requests[0].url.params.get_list("condition_ids") == [
        "0xresolved-yes",
        "0xresolved-no-reversed",
        "0xclosed-unsettled",
        "0xstill-open",
        "0xnon-binary",
    ]


@pytest.mark.asyncio
async def test_gamma_resolution_source_batches_condition_ids() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        rows = [
            _gamma_market_row(condition_id, closed=True)
            for condition_id in request.url.params.get_list("condition_ids")
        ]
        return httpx.Response(200, json=rows)

    async with _gamma_client(handler) as client:
        source = GammaResolutionSource(http_client=client, batch_size=2)
        resolutions = await source.fetch_resolutions(["0xa", "0xb", "0xc"])

    assert resolutions == {"0xa": 1.0, "0xb": 1.0, "0xc": 1.0}
    assert [request.url.params.get_list("condition_ids") for request in requests] == [
        ["0xa", "0xb"],
        ["0xc"],
    ]


class _RecordingConnection:
    def __init__(self) -> None:
        self.execute_calls: list[tuple[str, tuple[object, ...]]] = []
        self.fetch_calls: list[tuple[str, tuple[object, ...]]] = []
        self.fetchrow_calls: list[tuple[str, tuple[object, ...]]] = []
        self.fetch_rows: list[object] = []
        self.fetchrow_result: object | None = None

    async def execute(self, query: str, *args: object) -> str:
        self.execute_calls.append((query, args))
        return "OK"

    async def fetch(self, query: str, *args: object) -> list[object]:
        self.fetch_calls.append((query, args))
        return list(self.fetch_rows)

    async def fetchrow(self, query: str, *args: object) -> object | None:
        self.fetchrow_calls.append((query, args))
        return self.fetchrow_result


class _AcquireContext:
    def __init__(self, connection: _RecordingConnection) -> None:
        self._connection = connection

    async def __aenter__(self) -> _RecordingConnection:
        return self._connection

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        del exc_type, exc, tb


class _RecordingPool:
    def __init__(self, connection: _RecordingConnection) -> None:
        self._connection = connection

    def acquire(self) -> _AcquireContext:
        return _AcquireContext(self._connection)


def _unresolved_fill_row() -> dict[str, object]:
    return {
        "fill_id": "fill-res-1",
        "order_id": "order-fill-res-1",
        "market_id": "0xmarket-1",
        "ts": datetime(2026, 6, 1, tzinfo=UTC),
        "fill_notional_usdc": 4.2,
        "fill_quantity": 10.0,
        "strategy_id": "default",
        "strategy_version_id": "default-v1",
        "payload": {
            "trade_id": "trade-fill-res-1",
            "decision_id": "d-res-1",
            "token_id": "t-yes",
            "venue": "polymarket",
            "side": "BUY",
            "fill_price": 0.42,
            "executed_at": "2026-06-01T00:00:00+00:00",
            "status": "MATCHED",
            "anomaly_flags": [],
            "resolved_outcome": None,
        },
    }


@pytest.mark.asyncio
async def test_fill_store_read_unresolved_fills_filters_on_null_outcome() -> None:
    connection = _RecordingConnection()
    connection.fetch_rows = [_unresolved_fill_row()]
    store = FillStore(pool=cast(Any, _RecordingPool(connection)))

    fills = await store.read_unresolved_fills()

    assert len(fills) == 1
    assert fills[0].fill_id == "fill-res-1"
    assert fills[0].decision_id == "d-res-1"
    assert fills[0].resolved_outcome is None
    (query, args) = connection.fetch_calls[0]
    assert "payload->>'resolved_outcome' IS NULL" in query
    assert args == ()


@pytest.mark.asyncio
async def test_fill_store_resolve_fill_updates_payload_once() -> None:
    connection = _RecordingConnection()
    connection.fetchrow_result = {"fill_id": "fill-res-1"}
    store = FillStore(pool=cast(Any, _RecordingPool(connection)))

    updated = await store.resolve_fill("fill-res-1", resolved_outcome=1.0)

    assert updated is True
    (query, args) = connection.fetchrow_calls[0]
    assert "UPDATE fill_payloads" in query
    assert "jsonb_set" in query
    # The NULL guard is the idempotency contract: a second writer (or a
    # second sweep racing the first) must not re-resolve the same fill.
    assert "payload->>'resolved_outcome' IS NULL" in query
    assert args == ("fill-res-1", 1.0)


@pytest.mark.asyncio
async def test_fill_store_resolve_fill_returns_false_when_already_resolved() -> None:
    connection = _RecordingConnection()
    connection.fetchrow_result = None
    store = FillStore(pool=cast(Any, _RecordingPool(connection)))

    updated = await store.resolve_fill("fill-res-1", resolved_outcome=1.0)

    assert updated is False


class HoldingSensor:
    def __aiter__(self) -> AsyncIterator[MarketSignal]:
        return self._iterate()

    async def _iterate(self) -> AsyncIterator[MarketSignal]:
        while True:
            await asyncio.sleep(60.0)
            yield cast(MarketSignal, None)


def _paper_settings(**overrides: Any) -> PMSSettings:
    return PMSSettings(
        mode=RunMode.PAPER,
        auto_migrate_default_v2=False,
        risk=RiskSettings(
            max_position_per_market=1_000.0,
            max_total_exposure=10_000.0,
        ),
        **overrides,
    )


async def _wait_for(predicate: Any, *, timeout: float = 2.0) -> None:
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(0.01)


@pytest.mark.asyncio
async def test_paper_runner_starts_resolution_sweep_task_and_stop_clears_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = Runner(
        config=_paper_settings(),
        historical_data_path=FIXTURE_PATH,
        sensors=[HoldingSensor()],
    )
    sweep_calls = 0

    async def fake_sweep_once() -> ResolutionSweepResult:
        nonlocal sweep_calls
        sweep_calls += 1
        return ResolutionSweepResult(
            unresolved_fills=0,
            fills_resolved=0,
            eval_records_appended=0,
        )

    monkeypatch.setattr(runner, "_resolution_sweep_once", fake_sweep_once)

    await runner.start()
    try:
        assert runner._resolution_sweep_task is not None
        await _wait_for(lambda: sweep_calls >= 1)
        await _wait_for(lambda: runner.resolution_sweeps_total >= 1)
    finally:
        await runner.stop()

    assert runner._resolution_sweep_task is None


@pytest.mark.asyncio
async def test_backtest_runner_does_not_start_resolution_sweep_task() -> None:
    runner = Runner(
        config=PMSSettings(
            mode=RunMode.BACKTEST,
            auto_migrate_default_v2=False,
            database=DatabaseSettings(dsn="postgresql://localhost/pms_test_runner"),
        ),
        historical_data_path=FIXTURE_PATH,
        sensors=[HoldingSensor()],
    )

    await runner.start()
    try:
        assert runner.pg_pool is not None
        assert runner._resolution_sweep_task is None
    finally:
        await runner.stop()


@pytest.mark.asyncio
async def test_resolution_sweep_loop_survives_transient_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = Runner(
        config=_paper_settings(resolution_poll_interval_s=0.01),
        historical_data_path=FIXTURE_PATH,
        sensors=[HoldingSensor()],
    )
    calls = 0

    async def flaky_sweep_once() -> ResolutionSweepResult:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("gamma transient boom")
        return ResolutionSweepResult(
            unresolved_fills=2,
            fills_resolved=1,
            eval_records_appended=1,
        )

    monkeypatch.setattr(runner, "_resolution_sweep_once", flaky_sweep_once)

    loop_task = asyncio.create_task(runner._resolution_sweep_loop())
    await _wait_for(lambda: calls >= 2)
    runner._stop_event.set()
    await asyncio.wait_for(loop_task, timeout=2.0)

    assert calls >= 2
    assert runner.resolution_sweeps_total >= 1
    assert runner.resolution_fills_resolved_total >= 1


def test_resolution_poll_interval_defaults_to_five_minutes() -> None:
    assert PMSSettings(mode=RunMode.BACKTEST).resolution_poll_interval_s == 300.0
