# Trading Correctness and Live-Arb Readiness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make paper/backtest/controller/risk semantics match token-level executable trading before adding any arbitrage strategy or increasing LIVE scope.

**Architecture:** PR1 fixes correctness in the existing single-order path with tests first. PR2 introduces a shared token-level executable quote abstraction and Polymarket metadata cache. PR3 adds basket-shaped arbitrage decisions behind a separate multi-leg execution contract. PR4 is a live-readiness checklist after Polymarket CLOB V2 cutover validation.

**Tech Stack:** Python 3.13, pytest, mypy strict, FastAPI runner, Polymarket CLOB V2 SDK/path, PostgreSQL-backed market data snapshots.

---

## Scope Split

Do not ship all of this in one PR. The immediate PR should be only the correctness patch:

1. Paper and backtest use token-level orderbooks for YES and NO.
2. Controller records traded-outcome probability and edge.
3. Router gates stale/non-open/wide-spread signals when those fields are present.
4. Risk `max_open_positions` allows adding to an existing `(market_id, token_id)` position.

Then ship quote-provider and strategy work as separate PRs. The quote-provider work changes cross-layer contracts and should not be bundled with a bug fix that must land quickly.

## Source Anchors

- Official Polymarket CLOB V2 migration docs currently state go-live is April 28, 2026 around 11:00 UTC, with about one hour of downtime, V2 SDK requirement, open-order wipe, pUSD collateral, and no V1 backward compatibility after migration.
- Official Polymarket orderbook docs fetch books by token ID and return `market`, `asset_id`, `bids`, `asks`, `min_order_size`, `tick_size`, `neg_risk`, and `hash`.
- Official Polymarket order docs require tick-size-conforming prices and `negRisk` order options for neg-risk markets, and distinguish FOK/FAK immediate execution from GTC/GTD resting orders.

## Current Code Map

Modify in PR1:

- `src/pms/actuator/adapters/paper.py` - token-level orderbook lookup and VWAP fill.
- `src/pms/research/execution.py` - remove NO-side price complement from backtest simulator fill math.
- `src/pms/controller/pipeline.py` - compute `decision_edge` and `decision_probability` for the traded outcome.
- `src/pms/controller/router.py` - use existing controller freshness/spread config when fields are present.
- `src/pms/actuator/risk.py` - distinguish new positions from existing-position adds.
- `src/pms/runner.py` - cache paper books by `token_id` as the primary key while keeping market-id fallback for old tests.

Tests in PR1:

- `tests/unit/test_paper_actuator_cp12.py`
- `tests/unit/test_actuator_cp06.py`
- `tests/unit/test_backtest_execution_simulator_cp12.py`
- `tests/unit/test_controller_order_intent_cp03.py`
- `tests/unit/test_controller_router.py` (new)
- `tests/unit/test_risk.py`
- Targeted integration smoke: `tests/integration/test_pipeline_end_to_end.py`

Modify in PR2:

- `src/pms/actuator/quotes.py` (new)
- `src/pms/core/interfaces.py`
- `src/pms/actuator/adapters/paper.py`
- `src/pms/actuator/adapters/backtest.py`
- `src/pms/actuator/adapters/polymarket.py`
- `src/pms/storage/market_data_store.py`
- `src/pms/runner.py`

Modify in PR3:

- `src/pms/core/models.py`
- `src/pms/strategies/binary_complement.py` (new)
- `src/pms/strategies/subset_pricing.py` (new)
- `src/pms/actuator/executor.py`
- `src/pms/actuator/adapters/polymarket.py`
- `src/pms/storage/order_store.py`
- `schema.sql`
- `alembic/versions/0015_order_baskets.py` (new; revision id stays short for the existing `alembic_version.version_num` limit)

## PR1: Correctness Patch

### Task 1: PaperActuator Token-Level VWAP Fill

**Files:**
- Modify: `src/pms/actuator/adapters/paper.py`
- Test: `tests/unit/test_paper_actuator_cp12.py`
- Test: `tests/unit/test_actuator_cp06.py`

- [ ] **Step 1: Write failing token-level NO test**

Add this test to `tests/unit/test_paper_actuator_cp12.py`:

```python
@pytest.mark.asyncio
async def test_paper_buy_no_uses_no_token_asks_not_yes_complement() -> None:
    decision = _decision(limit_price=0.45)
    object.__setattr__(decision, "token_id", "token-no")
    object.__setattr__(decision, "outcome", "NO")
    actuator = PaperActuator(
        orderbooks={
            "token-yes": {
                "bids": [{"price": 0.62, "size": 1_000.0}],
                "asks": [{"price": 0.64, "size": 1_000.0}],
            },
            "token-no": {
                "bids": [{"price": 0.40, "size": 1_000.0}],
                "asks": [{"price": 0.44, "size": 1_000.0}],
            },
        }
    )

    state = await actuator.execute(decision, _portfolio())

    assert state.fill_price == pytest.approx(0.44)
    assert state.filled_quantity == pytest.approx(100.0 / 0.44)
```

- [ ] **Step 2: Write failing limit-price and VWAP tests**

Add these tests to `tests/unit/test_paper_actuator_cp12.py`:

```python
@pytest.mark.asyncio
async def test_paper_buy_respects_limit_price() -> None:
    actuator = PaperActuator(
        orderbooks={
            "token-yes": {
                "bids": [{"price": 0.39, "size": 100.0}],
                "asks": [{"price": 0.46, "size": 100.0}],
            }
        }
    )

    with pytest.raises(InsufficientLiquidityError, match="executable depth"):
        await actuator.execute(_decision(limit_price=0.45), _portfolio())


@pytest.mark.asyncio
async def test_paper_fill_consumes_multiple_levels_vwap() -> None:
    actuator = PaperActuator(
        orderbooks={
            "token-yes": {
                "bids": [{"price": 0.39, "size": 100.0}],
                "asks": [
                    {"price": 0.40, "size": 10.0},
                    {"price": 0.50, "size": 20.0},
                ],
            }
        }
    )

    state = await actuator.execute(
        _decision(notional_usdc=10.0, limit_price=0.55),
        _portfolio(),
    )

    assert state.fill_price == pytest.approx(10.0 / 22.0)
    assert state.filled_quantity == pytest.approx(22.0)
```

- [ ] **Step 3: Run failing tests**

Run:

```bash
uv run pytest tests/unit/test_paper_actuator_cp12.py::test_paper_buy_no_uses_no_token_asks_not_yes_complement tests/unit/test_paper_actuator_cp12.py::test_paper_buy_respects_limit_price tests/unit/test_paper_actuator_cp12.py::test_paper_fill_consumes_multiple_levels_vwap -q
```

Expected before implementation: at least one failure because `PaperActuator` currently looks up by market ID and complements NO prices.

- [ ] **Step 4: Implement token-level lookup and VWAP**

In `src/pms/actuator/adapters/paper.py`, replace `_best_fill_price` with `_orderbook_for_decision` and `_vwap_fill`, then pass `filled_quantity` into `_matched_order_state`:

```python
def _orderbook_for_decision(
    orderbooks: Mapping[str, dict[str, Any]],
    decision: TradeDecision,
) -> dict[str, Any]:
    if decision.token_id is not None and decision.token_id in orderbooks:
        return orderbooks[decision.token_id]
    if (
        decision.outcome == "YES" or decision.token_id is None
    ) and decision.market_id in orderbooks:
        return orderbooks[decision.market_id]
    raise InsufficientLiquidityError(
        f"missing paper orderbook for token={decision.token_id} market={decision.market_id}"
    )


def _vwap_fill(orderbook: dict[str, Any], decision: TradeDecision) -> tuple[float, float]:
    requested_notional_usdc = _decision_notional_usdc(decision)
    is_buy = decision.action == Side.BUY.value or decision.side == Side.BUY.value
    side_key = "asks" if is_buy else "bids"
    raw_levels = orderbook.get(side_key)
    if not isinstance(raw_levels, list) or not raw_levels:
        raise InsufficientLiquidityError(f"{side_key} depth is empty")

    levels: list[tuple[float, float]] = []
    for raw in raw_levels:
        if not isinstance(raw, dict):
            raise InsufficientLiquidityError(f"{side_key} depth is invalid")
        price = float(cast(str | int | float, raw["price"]))
        size = float(cast(str | int | float, raw.get("size", 0.0)))
        if price <= 0.0 or size <= 0.0:
            continue
        levels.append((price, size))

    levels.sort(key=lambda item: item[0], reverse=not is_buy)
    remaining = requested_notional_usdc
    filled_notional = 0.0
    filled_quantity = 0.0
    for price, size in levels:
        if is_buy and price > decision.limit_price:
            break
        if not is_buy and price < decision.limit_price:
            break
        take_notional = min(remaining, price * size)
        filled_notional += take_notional
        filled_quantity += take_notional / price
        remaining -= take_notional
        if remaining <= 1e-9:
            break

    if remaining > 1e-9 or filled_quantity <= 0.0:
        raise InsufficientLiquidityError(
            f"{side_key} executable depth is insufficient at limit={decision.limit_price}"
        )
    return filled_notional / filled_quantity, filled_quantity
```

Update `execute`:

```python
orderbook = _orderbook_for_decision(self.orderbooks, decision)
fill_price, filled_quantity = _vwap_fill(orderbook, decision)
return _matched_order_state(decision, fill_price, filled_quantity, "paper")
```

The market-id fallback is intentionally restricted to legacy YES paths. A NO decision with a missing NO token book must raise `InsufficientLiquidityError`; otherwise paper can silently recreate the same YES-complement bug through the fallback path.

Update `_matched_order_state`:

```python
def _matched_order_state(
    decision: TradeDecision,
    fill_price: float,
    filled_quantity: float,
    order_id_prefix: str,
) -> OrderState:
    now = datetime.now(tz=UTC)
    filled_notional_usdc = _decision_notional_usdc(decision)
    return OrderState(
        order_id=f"{order_id_prefix}-{uuid4().hex}",
        decision_id=decision.decision_id,
        status=OrderStatus.MATCHED.value,
        market_id=decision.market_id,
        token_id=decision.token_id,
        venue=decision.venue,
        requested_notional_usdc=filled_notional_usdc,
        filled_notional_usdc=filled_notional_usdc,
        remaining_notional_usdc=0.0,
        fill_price=fill_price,
        submitted_at=now,
        last_updated_at=now,
        raw_status="matched",
        strategy_id=decision.strategy_id,
        strategy_version_id=decision.strategy_version_id,
        filled_quantity=filled_quantity,
        action=decision.action,
        outcome=decision.outcome,
        time_in_force=decision.time_in_force.value,
        intent_key=decision.intent_key,
    )
```

- [ ] **Step 5: Replace wrong legacy test**

In `tests/unit/test_actuator_cp06.py`, rename `test_paper_actuator_derives_no_fill_price_from_yes_bid` to `test_paper_actuator_fills_no_from_no_token_ask` and change its fixture so the NO decision has `token_id="no-token"` and the actuator has `"no-token": {"asks": [{"price": 0.38, "size": 100.0}]}`. The assertion remains `fill_price == 0.38`, but it must come from NO asks, not from `1 - YES bid`.

- [ ] **Step 6: Run task tests**

Run:

```bash
uv run pytest tests/unit/test_paper_actuator_cp12.py tests/unit/test_actuator_cp06.py -q
```

Expected after implementation: all selected tests pass.

- [ ] **Step 7: Commit**

```bash
git add src/pms/actuator/adapters/paper.py tests/unit/test_paper_actuator_cp12.py tests/unit/test_actuator_cp06.py
git commit -m "fix(actuator): fill paper orders from token-level depth"
```

### Task 2: Backtest Simulator Uses Token Book Prices Directly

**Files:**
- Modify: `src/pms/research/execution.py`
- Test: `tests/unit/test_backtest_execution_simulator_cp12.py`

- [ ] **Step 1: Write failing NO backtest test**

Add a test that creates a `MarketSignal` whose orderbook is already the NO token book, then executes a BUY NO decision at a limit above the NO ask. Assert fill price equals the NO ask, not `1 - raw_price`.

```python
@pytest.mark.asyncio
async def test_backtest_buy_no_uses_no_token_ask_without_complement() -> None:
    signal = _signal(
        asks=[{"price": 0.42, "size": 100.0}],
        bids=[{"price": 0.40, "size": 100.0}],
    )
    decision = _decision(limit_price=0.43)
    object.__setattr__(decision, "token_id", signal.token_id)
    object.__setattr__(decision, "outcome", "NO")

    state = await BacktestExecutionSimulator().execute(
        signal=signal,
        decision=decision,
        portfolio=None,
        execution_model=ExecutionModel.polymarket_paper(),
    )

    assert state.fill_price == pytest.approx(0.42)
```

- [ ] **Step 2: Run failing test**

```bash
uv run pytest tests/unit/test_backtest_execution_simulator_cp12.py::test_backtest_buy_no_uses_no_token_ask_without_complement -q
```

Expected before implementation: failure showing complement or wrong side selection.

- [ ] **Step 3: Implement side selection**

In `src/pms/research/execution.py`, replace `_side_key` and `_effective_level_price`:

```python
def _side_key(decision: TradeDecision) -> str:
    action = _action(decision)
    return "asks" if action == Side.BUY.value else "bids"


def _effective_level_price(decision: TradeDecision, raw_level: dict[str, Any]) -> float:
    del decision
    return float(cast(str | int | float, raw_level["price"]))
```

- [ ] **Step 4: Run simulator tests**

```bash
uv run pytest tests/unit/test_backtest_execution_simulator_cp12.py tests/unit/test_backtest_actuator_cp12.py tests/unit/test_backtest_actuator_delegation.py -q
```

Expected after implementation: pass.

- [ ] **Step 5: Commit**

```bash
git add src/pms/research/execution.py tests/unit/test_backtest_execution_simulator_cp12.py
git commit -m "fix(backtest): price fills from token orderbooks"
```

### Task 3: Controller BUY NO Records Traded-Outcome Edge

**Files:**
- Modify: `src/pms/controller/pipeline.py`
- Test: `tests/unit/test_controller_order_intent_cp03.py`
- Test: `tests/unit/test_controller_runtime_selection_contract_cp01.py`

- [ ] **Step 1: Extend existing negative-edge test**

In `test_negative_edge_maps_to_buy_no_with_resolved_no_token`, add:

```python
assert opportunity.expected_edge == pytest.approx(0.35)
assert decision.expected_edge == pytest.approx(0.35)
assert decision.prob_estimate == pytest.approx(0.70)
assert opportunity.composition_trace["yes_probability"] == pytest.approx(0.30)
assert opportunity.composition_trace["yes_reference_price"] == pytest.approx(0.65)
assert opportunity.composition_trace["traded_outcome"] == "NO"
assert opportunity.composition_trace["traded_probability"] == pytest.approx(0.70)
assert opportunity.composition_trace["traded_price"] == pytest.approx(0.35)
assert opportunity.composition_trace["traded_edge"] == pytest.approx(0.35)
```

- [ ] **Step 2: Run failing test**

```bash
uv run pytest tests/unit/test_controller_order_intent_cp03.py::test_negative_edge_maps_to_buy_no_with_resolved_no_token -q
```

Expected before implementation: `decision.expected_edge` and `opportunity.expected_edge` are negative or trace keys are missing.

- [ ] **Step 3: Implement traded-outcome semantics**

In `src/pms/controller/pipeline.py`, replace the `expected_edge` block with explicit YES and traded-outcome values:

```python
yes_probability = prob_estimate
yes_reference_price = signal.yes_price
yes_edge = yes_probability - yes_reference_price
decision_token_id = signal.token_id
decision_outcome: Literal["YES", "NO"] = "YES"
decision_probability = yes_probability
decision_price = yes_reference_price
decision_edge = yes_edge
opportunity_side: Literal["yes", "no"] = "yes"

if yes_edge < 0.0:
    outcome_tokens = await self.outcome_token_resolver.resolve(
        market_id=signal.market_id,
        signal_token_id=signal.token_id,
    )
    if outcome_tokens.no_token_id is None:
        self.last_diagnostic = ControllerDiagnostic(
            code="missing_no_token",
            message="Skipping bearish decision because no NO token could be resolved.",
            market_id=signal.market_id,
            strategy_id=self.strategy_id,
            strategy_version_id=self.strategy_version_id,
            token_id=signal.token_id,
            severity="warning",
            metadata={
                "signal_token_id": signal.token_id,
                "yes_token_id": outcome_tokens.yes_token_id,
                "outcome": "NO",
            },
        )
        logger.info(
            "controller diagnostic %s for %s",
            self.last_diagnostic.code,
            signal.market_id,
            extra={"controller_diagnostic": self.last_diagnostic},
        )
        return None
    decision_token_id = outcome_tokens.no_token_id
    decision_outcome = "NO"
    opportunity_side = "no"
    decision_probability = 1.0 - yes_probability
    decision_price = max(1e-6, min(1.0 - 1e-6, 1.0 - yes_reference_price))
    decision_edge = decision_probability - decision_price

if decision_edge <= 0.0:
    return None
```

When constructing `Opportunity`, set `expected_edge=decision_edge` and merge trace keys:

```python
composition_trace={
    **composition_trace,
    "yes_probability": yes_probability,
    "yes_reference_price": yes_reference_price,
    "yes_edge": yes_edge,
    "traded_outcome": decision_outcome,
    "traded_probability": decision_probability,
    "traded_price": decision_price,
    "traded_edge": decision_edge,
},
```

When constructing `TradeDecision`, set:

```python
prob_estimate=decision_probability,
expected_edge=decision_edge,
```

- [ ] **Step 4: Run controller tests**

```bash
uv run pytest tests/unit/test_controller_order_intent_cp03.py tests/unit/test_controller_runtime_selection_contract_cp01.py tests/unit/test_controller_cp05.py -q
```

Expected after implementation: pass. If old tests expected YES-probability for a NO decision, update those tests to assert traded-outcome probability.

- [ ] **Step 5: Commit**

```bash
git add src/pms/controller/pipeline.py tests/unit/test_controller_order_intent_cp03.py tests/unit/test_controller_runtime_selection_contract_cp01.py tests/unit/test_controller_cp05.py
git commit -m "fix(controller): record traded-outcome edge"
```

### Task 4: Router Uses Existing Freshness/Spread/Status Gates

**Files:**
- Modify: `src/pms/controller/router.py`
- Test: `tests/unit/test_controller_router.py` (new)

- [ ] **Step 1: Add router tests**

Create `tests/unit/test_controller_router.py`:

```python
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from pms.config import ControllerSettings
from pms.controller.router import Router
from pms.core.models import MarketSignal


def _signal(**overrides: Any) -> MarketSignal:
    now = datetime(2026, 4, 27, 12, 0, tzinfo=UTC)
    data: dict[str, Any] = {
        "market_id": "router-market",
        "token_id": "router-token",
        "venue": "polymarket",
        "title": "Router market",
        "yes_price": 0.5,
        "volume_24h": 100.0,
        "resolves_at": now + timedelta(days=1),
        "orderbook": {"bids": [], "asks": []},
        "external_signal": {},
        "fetched_at": now,
        "market_status": "open",
    }
    data.update(overrides)
    return MarketSignal(**data)


def test_router_rejects_resolved_market() -> None:
    now = datetime(2026, 4, 27, 12, 0, tzinfo=UTC)
    assert not Router().gate(_signal(fetched_at=now, resolves_at=now))


def test_router_rejects_wide_spread_when_available() -> None:
    router = Router(ControllerSettings(max_spread_bps=100.0))
    assert not router.gate(_signal(external_signal={"spread_bps": 101.0}))


def test_router_rejects_stale_book_when_available() -> None:
    router = Router(ControllerSettings(max_book_age_ms=1_000.0))
    assert not router.gate(_signal(external_signal={"book_age_ms": 1_001.0}))


def test_router_rejects_non_open_status_from_signal_or_external_signal() -> None:
    router = Router()
    assert not router.gate(_signal(market_status="closed"))
    assert not router.gate(_signal(external_signal={"market_status": "halted"}))


def test_router_allows_signal_when_optional_quote_fields_are_absent() -> None:
    assert Router().gate(_signal())
```

- [ ] **Step 2: Run failing router tests**

```bash
uv run pytest tests/unit/test_controller_router.py -q
```

Expected before implementation: several failures.

- [ ] **Step 3: Implement router gates**

Update `src/pms/controller/router.py`:

```python
from math import inf
from typing import Any


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return inf
```

Then update `gate`:

```python
if signal.resolves_at is not None and signal.resolves_at <= signal.timestamp:
    return False
status = str(signal.external_signal.get("market_status", signal.market_status)).lower()
if status not in {"open", "active"}:
    return False
spread_bps = _optional_float(signal.external_signal.get("spread_bps"))
if spread_bps is not None and spread_bps > self.controller.max_spread_bps:
    return False
book_age_ms = _optional_float(signal.external_signal.get("book_age_ms"))
if book_age_ms is not None and book_age_ms > self.controller.max_book_age_ms:
    return False
```

- [ ] **Step 4: Run router and controller tests**

```bash
uv run pytest tests/unit/test_controller_router.py tests/unit/test_controller_cp01.py tests/unit/test_controller_cp05.py -q
```

Expected after implementation: pass.

- [ ] **Step 5: Commit**

```bash
git add src/pms/controller/router.py tests/unit/test_controller_router.py
git commit -m "fix(controller): gate stale and non-open signals"
```

### Task 5: Risk Allows Adding to Existing Token Position

**Files:**
- Modify: `src/pms/actuator/risk.py`
- Test: `tests/unit/test_risk.py`

- [ ] **Step 1: Add failing risk test**

Add to `tests/unit/test_risk.py`:

```python
def test_risk_manager_allows_existing_position_add_at_open_position_cap() -> None:
    positions = _open_positions(4)
    positions.append(
        Position(
            market_id="market-risk",
            token_id="token-risk",
            venue="polymarket",
            side="BUY",
            shares_held=1.0,
            avg_entry_price=0.5,
            unrealized_pnl=0.0,
            locked_usdc=1.0,
        )
    )
    portfolio = _portfolio(
        free_usdc=995.0,
        locked_usdc=5.0,
        open_positions=positions,
    )

    result = RiskManager(_risk(max_open_positions=5)).check(
        _decision(notional_usdc=10.0),
        portfolio,
    )

    assert result == RiskDecision(approved=True, reason="approved")
```

- [ ] **Step 2: Run failing test**

```bash
uv run pytest tests/unit/test_risk.py::test_risk_manager_allows_existing_position_add_at_open_position_cap -q
```

Expected before implementation: rejects with `max_open_positions`.

- [ ] **Step 3: Implement helper**

Add to `src/pms/actuator/risk.py`:

```python
def _has_open_position(portfolio: Portfolio, decision: TradeDecision) -> bool:
    return any(
        position.market_id == decision.market_id
        and position.token_id == decision.token_id
        for position in portfolio.open_positions
    )
```

Update the cap:

```python
if (
    self.risk.max_open_positions is not None
    and not _has_open_position(portfolio, decision)
    and len(portfolio.open_positions) >= self.risk.max_open_positions
):
    return RiskDecision(False, "max_open_positions")
```

- [ ] **Step 4: Run risk tests**

```bash
uv run pytest tests/unit/test_risk.py -q
```

Expected after implementation: pass.

- [ ] **Step 5: Commit**

```bash
git add src/pms/actuator/risk.py tests/unit/test_risk.py
git commit -m "fix(risk): allow adds to existing open positions"
```

### Task 6: Runner Paper Book Cache Is Token-Primary

**Files:**
- Modify: `src/pms/runner.py`
- Test: `tests/integration/test_pipeline_end_to_end.py`

- [ ] **Step 1: Add integration test**

Add a test that emits a YES-token signal, produces a BUY NO decision through the resolver, and verifies the paper actuator can fill only when a NO token book is present under the NO token key. The test should fail if `_paper_orderbooks` only stores by `market_id`.

Use this assertion:

```python
assert runner._paper_orderbooks["no-token"]["asks"][0]["price"] == pytest.approx(0.44)
```

- [ ] **Step 2: Implement token-primary cache**

In `_controller_loop` and `_actuator_loop`, change:

```python
self._paper_orderbooks[signal.market_id] = signal.orderbook
```

to:

```python
if signal.token_id is not None:
    self._paper_orderbooks[signal.token_id] = signal.orderbook
self._paper_orderbooks[signal.market_id] = signal.orderbook
```

Market-id fallback remains only for old tests and non-tokenized fixtures.

- [ ] **Step 3: Run integration smoke**

```bash
uv run pytest tests/integration/test_pipeline_end_to_end.py -q
```

Expected after implementation: pass.

- [ ] **Step 4: Commit**

```bash
git add src/pms/runner.py tests/integration/test_pipeline_end_to_end.py
git commit -m "fix(runner): cache paper books by token id"
```

### Task 7: PR1 Verification

**Files:**
- No code changes unless gates fail.

- [ ] **Step 1: Run targeted tests**

```bash
uv run pytest tests/unit/test_paper_actuator_cp12.py tests/unit/test_actuator_cp06.py tests/unit/test_backtest_execution_simulator_cp12.py tests/unit/test_backtest_actuator_cp12.py tests/unit/test_controller_order_intent_cp03.py tests/unit/test_controller_router.py tests/unit/test_risk.py tests/integration/test_pipeline_end_to_end.py -q
```

Expected: all pass.

- [ ] **Step 2: Run canonical gates**

```bash
uv sync
uv run pytest -q
uv run mypy src/ tests/ --strict
uv run lint-imports
```

Expected: pytest baseline remains at or above documented passing/skipped count, mypy strict clean, import-linter clean.

- [ ] **Step 3: Commit any gate-only repairs**

If the full suite exposes stale expectations, update tests to match the new runtime semantics. Do not weaken token-level or fail-closed behavior.

```bash
git add src/pms/actuator/adapters/paper.py src/pms/research/execution.py src/pms/controller/pipeline.py src/pms/controller/router.py src/pms/actuator/risk.py src/pms/runner.py tests/unit/test_paper_actuator_cp12.py tests/unit/test_actuator_cp06.py tests/unit/test_backtest_execution_simulator_cp12.py tests/unit/test_controller_order_intent_cp03.py tests/unit/test_controller_runtime_selection_contract_cp01.py tests/unit/test_controller_cp05.py tests/unit/test_controller_router.py tests/unit/test_risk.py tests/integration/test_pipeline_end_to_end.py
git commit -m "fix(tests): align trading correctness expectations"
```

## PR2: Token-Level Executable Quote Provider

### Task 8: Introduce Shared Quote Math

**Files:**
- Create: `src/pms/actuator/quotes.py`
- Modify: `src/pms/core/interfaces.py`
- Test: `tests/unit/test_executable_quote.py` (new)

- [ ] **Step 1: Add tests for quote math**

Create tests for BUY walking asks, SELL walking bids, limit rejection, spread bps, VWAP price, executable notional, and book hash propagation. Use raw levels where price/size are strings to match CLOB payloads.

- [ ] **Step 2: Implement frozen quote value objects**

Create:

```python
@dataclass(frozen=True)
class ExecutableQuote:
    market_id: str
    token_id: str
    side: str
    best_price: float
    vwap_price: float
    executable_notional_usdc: float
    spread_bps: float
    book_age_ms: float
    book_hash: str
    tick_size: float
    min_order_size: float
    fee_bps: float
```

Create a pure function:

```python
def executable_quote_from_book(
    *,
    market_id: str,
    token_id: str,
    side: str,
    limit_price: float,
    notional_usdc: float,
    bids: Sequence[Mapping[str, object]],
    asks: Sequence[Mapping[str, object]],
    book_ts: datetime,
    now: datetime,
    book_hash: str,
    tick_size: float,
    min_order_size: float,
    fee_bps: float,
) -> ExecutableQuote:
    if notional_usdc <= 0.0:
        raise InsufficientLiquidityError("quote notional must be positive")
    is_buy = side == "BUY"
    executable_levels = asks if is_buy else bids
    sorted_levels = sorted(
        (
            (
                float(cast(str | int | float, raw["price"])),
                float(cast(str | int | float, raw.get("size", 0.0))),
            )
            for raw in executable_levels
        ),
        key=lambda item: item[0],
        reverse=not is_buy,
    )
    best_bid = max(
        (float(cast(str | int | float, raw["price"])) for raw in bids),
        default=None,
    )
    best_ask = min(
        (float(cast(str | int | float, raw["price"])) for raw in asks),
        default=None,
    )
    remaining = notional_usdc
    filled_notional = 0.0
    filled_quantity = 0.0
    executable_notional = 0.0
    best_price: float | None = None
    for price, size in sorted_levels:
        if price <= 0.0 or size <= 0.0:
            continue
        if is_buy and price > limit_price:
            break
        if not is_buy and price < limit_price:
            break
        if best_price is None:
            best_price = price
        level_notional = price * size
        executable_notional += level_notional
        if remaining > 1e-9:
            take_notional = min(remaining, level_notional)
            filled_notional += take_notional
            filled_quantity += take_notional / price
            remaining -= take_notional
    if filled_quantity <= 0.0:
        raise InsufficientLiquidityError("no executable depth at limit")
    vwap_price = filled_notional / filled_quantity
    return ExecutableQuote(
        market_id=market_id,
        token_id=token_id,
        side=side,
        best_price=best_price if best_price is not None else limit_price,
        vwap_price=vwap_price,
        executable_notional_usdc=executable_notional,
        spread_bps=_spread_bps(best_bid=best_bid, best_ask=best_ask),
        book_age_ms=max(0.0, (now - book_ts).total_seconds() * 1000.0),
        book_hash=book_hash,
        tick_size=tick_size,
        min_order_size=min_order_size,
        fee_bps=fee_bps,
    )
```

The function must select asks for BUY and bids for SELL, never complement NO prices.

- [ ] **Step 3: Adapt existing live quote provider**

Keep `LivePreSubmitQuote` for compatibility, but build it from `ExecutableQuote` so paper/backtest/live share the same fill math.

- [ ] **Step 4: Move real slippage checking out of RiskManager**

Delete the `decision.max_slippage_bps > risk.slippage_threshold_bps` rejection from `RiskManager` after quote math enforces actual slippage using `vwap_price`, `best_price`, and the decision limit. Replace `tests/unit/test_risk.py::test_risk_manager_rejects_slippage_above_threshold` with `tests/unit/test_executable_quote.py::test_quote_rejects_vwap_slippage_above_threshold`.

## PR2: Polymarket Metadata Cache

### Task 9: Persist and Use Market Execution Metadata

**Files:**
- Modify: `src/pms/core/models.py`
- Create: `src/pms/storage/clob_market_metadata_store.py`
- Modify: `schema.sql`
- Create: `alembic/versions/0014_clob_metadata.py`
- Modify: `src/pms/sensor/adapters/market_data.py`
- Modify: `src/pms/storage/market_data_store.py`
- Modify: `src/pms/actuator/adapters/polymarket.py`
- Test: storage and adapter unit/integration tests.

- [ ] **Step 1: Add metadata dataclass**

```python
@dataclass(frozen=True)
class ClobMarketMetadata:
    condition_id: str
    yes_token_id: str
    no_token_id: str
    min_order_size: float
    min_tick_size: float
    maker_fee_bps: float
    taker_fee_bps: float
    neg_risk: bool
    refreshed_at: datetime
```

- [ ] **Step 2: Enforce before live order construction**

Before creating/posting a live order:

1. Round limit price to tick in the conservative direction.
2. Reject if notional is below `min_order_size`.
3. Reject if token ID does not belong to condition ID.
4. Pass `negRisk` from metadata.
5. Include fee bps in edge calculation and `pre_submit_quote`.

- [ ] **Step 3: Add V2 cutover checklist command**

Add an operator script or command that verifies SDK version, CLOB host behavior, metadata fields, pUSD balance/allowance, order create/post shape, open orders, positions, and account reconciliation. The command must be read-only unless explicitly invoked with a live-submit flag.

## PR3: Real Arbitrage Strategies

### Task 10: Add Basket Decision Entities

**Files:**
- Modify: `src/pms/core/models.py`
- Modify: `schema.sql`
- Create: `alembic/versions/0015_order_baskets.py`
- Modify: `src/pms/storage/order_store.py`
- Test: `tests/unit/test_order_basket_models.py` (new)

- [ ] **Step 1: Add frozen entities**

```python
@dataclass(frozen=True)
class OrderLeg:
    market_id: str
    token_id: str
    outcome: Outcome
    side: BookSide
    limit_price: float
    notional_usdc: float


@dataclass(frozen=True)
class OrderBasket:
    basket_id: str
    strategy_id: str
    strategy_version_id: str
    legs: tuple[OrderLeg, ...]
    expected_edge_usdc: float
    max_loss_usdc: float
    book_hashes: tuple[str, ...]
```

- [ ] **Step 2: Reject invalid baskets**

Validation rules:

1. At least two legs.
2. Every leg has positive notional and `0 < limit_price < 1`.
3. `book_hashes` length matches `legs`.
4. Basket strategy tag is non-empty.

### Task 11: Binary Complement Arbitrage Strategy

**Files:**
- Create strategy module.
- Test: `tests/unit/test_binary_complement_arb_strategy.py` (new)

- [ ] **Step 1: Add quote input**

```python
@dataclass(frozen=True)
class BinaryComplementQuote:
    market_id: str
    yes_token_id: str
    no_token_id: str
    yes_ask: float
    no_ask: float
    yes_ask_size: float
    no_ask_size: float
    tick_size: float
    min_order_size: float
    fee_bps: float
    book_hash: str
```

- [ ] **Step 2: Implement decision rule**

The strategy emits a two-leg BUY basket only when:

```python
edge_per_share = 1.0 - yes_ask - no_ask - fee_cost - slippage_buffer
edge_per_share > min_edge
```

Size uses the minimum executable notional across YES ask depth, NO ask depth, and risk cap. Do not use `1 - yes_price`.

- [ ] **Step 3: Require all-or-nothing execution**

The actuator must use FOK or an equivalent basket guard. If one leg cannot be guaranteed, reject the basket before submitting either leg.

### Task 12: Subset Pricing Violation Strategy

**Files:**
- Create strategy module.
- Test: `tests/unit/test_subset_pricing_violation_strategy.py` (new)

- [ ] **Step 1: Model mutually exclusive outcome sets**

Represent exhaustive outcome groups and implication edges explicitly. Do not infer logical relationships from market titles.

- [ ] **Step 2: Implement checks**

For exhaustive mutually exclusive sets:

```python
sum(executable_ask_i) + fees + slippage_buffer < 1.0
```

For implication:

```python
P(A) <= P(B) if A implies B
```

Use executable asks/bids from token-level quote provider only.

## PR4: Post-Cutover Live Checklist

### Task 13: Live Readiness Run

**Files:**
- Modify: `docs/operations/live-polymarket-runbook.md`
- Optionally create: `claudedocs/YYYY-MM-DD-live-v2-verification.md`

- [ ] **Step 1: Re-verify official docs after cutover**

Open official Polymarket docs/status after April 28, 2026 11:00 UTC. Confirm whether go-live completed, whether the production URL is V2, and whether the Python SDK version in `uv.lock` is still accepted.

- [ ] **Step 2: Run repository gates**

```bash
uv sync --extra live
uv run pytest -q
PMS_RUN_INTEGRATION=1 uv run pytest -m integration
uv run mypy src/ tests/ --strict
uv run lint-imports
```

- [ ] **Step 3: Run dashboard gates**

```bash
cd dashboard
npm run lint
npm test -- --run
npm run build
```

- [ ] **Step 4: Run fail-closed live control-plane smoke**

Use a clean smoke DB, not the PAPER soak DB:

```bash
export DATABASE_URL=postgresql://localhost/pms_live_smoke_stometa
export PMS_MODE=LIVE
export PMS_AUTO_START=0
uv run pms-api
```

Verify `/status` and `/run/start` fail closed without complete live credentials.

- [ ] **Step 5: Run read-only venue checks**

Verify:

1. pUSD balance and allowance.
2. YES/NO token metadata for one allowlisted market.
3. Token-level orderbook fetch for both tokens.
4. Tick size and neg-risk metadata.
5. Open orders and positions reconciliation.
6. Cancel-all path in a non-production or explicitly approved tiny live environment.

- [ ] **Step 6: First live order remains manual**

Only after paper soak and read-only checks pass:

1. IOC/FOK only.
2. Single-market allowlist.
3. Tiny notional.
4. Manual first-order approval.
5. Dashboard points to live backend and cannot use mock fallback.
6. Unknown-submission recovery and cancel-all tested.

## Final Verification for Each PR

Every PR must end with:

```bash
git status --short --branch
uv sync
uv run pytest -q
uv run mypy src/ tests/ --strict
uv run lint-imports
```

For PR2 and PR4 live-related work also run:

```bash
uv sync --extra live
```

Do not claim live readiness from local unit tests alone. Include the exact command output in the PR or handoff note when the user asks for evidence.
