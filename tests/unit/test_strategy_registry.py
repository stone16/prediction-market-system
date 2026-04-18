from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta, timezone
import json
from typing import Any

import pytest

from pms.storage.strategy_registry import (
    PostgresStrategyRegistry,
    _ensure_utc,
    _json_float,
    _json_object,
    _json_optional_int,
    _json_pairs,
    _json_string,
    _json_string_list,
    _load_json_object,
    _strategy_from_config_json,
)
from pms.strategies.aggregate import Strategy
from pms.strategies.projections import (
    EvalSpec,
    FactorCompositionStep,
    ForecasterSpec,
    MarketSelectionSpec,
    RiskParams,
    StrategyConfig,
)
from pms.strategies.versioning import (
    compute_strategy_version_id,
    serialize_strategy_config_json,
)


@dataclass
class FakeTransaction:
    entered: int = 0
    exited: int = 0

    async def __aenter__(self) -> None:
        self.entered += 1
        return None

    async def __aexit__(self, *_: object) -> None:
        self.exited += 1
        return None


@dataclass
class FakeConnection:
    fetchval_results: list[object] = field(default_factory=list)
    fetchrow_results: list[dict[str, object] | None] = field(default_factory=list)
    fetch_results: list[list[dict[str, object]]] = field(default_factory=list)
    execute_calls: list[tuple[str, tuple[object, ...]]] = field(default_factory=list)
    fetchval_calls: list[tuple[str, tuple[object, ...]]] = field(default_factory=list)
    fetchrow_calls: list[tuple[str, tuple[object, ...]]] = field(default_factory=list)
    fetch_calls: list[tuple[str, tuple[object, ...]]] = field(default_factory=list)
    transaction_manager: FakeTransaction = field(default_factory=FakeTransaction)

    async def execute(self, query: str, *args: object) -> str:
        self.execute_calls.append((query, args))
        return "EXECUTE"

    async def fetchval(self, query: str, *args: object) -> object:
        self.fetchval_calls.append((query, args))
        if not self.fetchval_results:
            msg = "fetchval called without a configured result"
            raise AssertionError(msg)
        return self.fetchval_results.pop(0)

    async def fetchrow(self, query: str, *args: object) -> dict[str, object] | None:
        self.fetchrow_calls.append((query, args))
        if not self.fetchrow_results:
            return None
        return self.fetchrow_results.pop(0)

    async def fetch(self, query: str, *args: object) -> list[dict[str, object]]:
        self.fetch_calls.append((query, args))
        if not self.fetch_results:
            return []
        return self.fetch_results.pop(0)

    def transaction(self) -> FakeTransaction:
        return self.transaction_manager


class FakeAcquire:
    def __init__(self, connection: FakeConnection) -> None:
        self._connection = connection

    async def __aenter__(self) -> FakeConnection:
        return self._connection

    async def __aexit__(self, *_: object) -> None:
        return None


class FakePool:
    def __init__(self, connection: FakeConnection) -> None:
        self._connection = connection

    def acquire(self) -> FakeAcquire:
        return FakeAcquire(self._connection)


def _strategy(
    strategy_id: str = "default",
    *,
    owner: str = "system",
    drawdown_pct: float = 2.5,
) -> Strategy:
    return Strategy(
        config=StrategyConfig(
            strategy_id=strategy_id,
            factor_composition=(
                FactorCompositionStep(
                    factor_id="factor-a",
                    role="weighted",
                    param="",
                    weight=0.6,
                    threshold=None,
                ),
                FactorCompositionStep(
                    factor_id="factor-b",
                    role="weighted",
                    param="",
                    weight=0.4,
                    threshold=None,
                ),
            ),
            metadata=(("owner", owner), ("tier", "default")),
        ),
        risk=RiskParams(
            max_position_notional_usdc=100.0,
            max_daily_drawdown_pct=drawdown_pct,
            min_order_size_usdc=1.0,
        ),
        eval_spec=EvalSpec(metrics=("brier", "pnl", "fill_rate")),
        forecaster=ForecasterSpec(
            forecasters=(
                ("rules", (("threshold", "0.55"),)),
                ("stats", (("window", "15m"),)),
            )
        ),
        market_selection=MarketSelectionSpec(
            venue="polymarket",
            resolution_time_max_horizon_days=7,
            volume_min_usdc=500.0,
        ),
    )


@pytest.mark.asyncio
async def test_create_strategy_serializes_metadata_json() -> None:
    connection = FakeConnection()
    registry = PostgresStrategyRegistry(FakePool(connection))

    await registry.create_strategy("default", metadata={"tier": "default", "owner": "system"})

    assert connection.execute_calls == [
        (
            """
        INSERT INTO strategies (strategy_id, metadata_json)
        VALUES ($1, $2::jsonb)
        ON CONFLICT (strategy_id) DO NOTHING
        """,
            ("default", '{"owner":"system","tier":"default"}'),
        )
    ]


@pytest.mark.asyncio
async def test_create_version_inserts_and_activates_strategy() -> None:
    strategy = _strategy()
    created_at = datetime(2026, 4, 17, 12, 30, tzinfo=timezone(timedelta(hours=-5)))
    connection = FakeConnection(fetchval_results=[created_at])
    registry = PostgresStrategyRegistry(FakePool(connection))

    version = await registry.create_version(strategy)

    assert version.strategy_id == "default"
    assert version.strategy_version_id == compute_strategy_version_id(*strategy.snapshot())
    assert version.created_at == created_at.astimezone(UTC)
    assert connection.transaction_manager.entered == 1
    assert connection.transaction_manager.exited == 1
    assert len(connection.execute_calls) == 2
    assert connection.fetchval_calls[0][1] == (
        version.strategy_version_id,
        "default",
        serialize_strategy_config_json(*strategy.snapshot()),
    )
    assert connection.execute_calls[-1][1] == ("default", version.strategy_version_id)


@pytest.mark.asyncio
async def test_create_version_reuses_existing_timestamp_when_insert_is_idempotent() -> None:
    strategy = _strategy()
    existing_created_at = datetime(2026, 4, 17, 8, 0, tzinfo=timezone(timedelta(hours=2)))
    connection = FakeConnection(fetchval_results=[None, existing_created_at])
    registry = PostgresStrategyRegistry(FakePool(connection))

    version = await registry.create_version(strategy)

    assert len(connection.fetchval_calls) == 2
    assert connection.fetchval_calls[1][1] == ("default", version.strategy_version_id)
    assert version.created_at == existing_created_at.astimezone(UTC)


@pytest.mark.asyncio
async def test_create_version_raises_type_error_when_created_at_is_not_a_datetime() -> None:
    connection = FakeConnection(fetchval_results=[None, "not-a-timestamp"])
    registry = PostgresStrategyRegistry(FakePool(connection))

    with pytest.raises(TypeError, match="did not return a timestamp"):
        await registry.create_version(_strategy())


@pytest.mark.asyncio
async def test_get_by_id_round_trips_json_string_and_handles_missing_rows() -> None:
    strategy = _strategy()
    connection = FakeConnection(
        fetchrow_results=[
            {"config_json": serialize_strategy_config_json(*strategy.snapshot())},
            {"config_json": None},
            None,
        ]
    )
    registry = PostgresStrategyRegistry(FakePool(connection))

    assert await registry.get_by_id("default") == strategy
    assert await registry.get_by_id("inactive") is None
    assert await registry.get_by_id("missing") is None


@pytest.mark.asyncio
async def test_list_strategies_and_versions_return_utc_projection_rows() -> None:
    created_at = datetime(2026, 4, 17, 15, 0, tzinfo=timezone(timedelta(hours=9)))
    connection = FakeConnection(
        fetch_results=[
            [
                {
                    "strategy_id": "default",
                    "active_version_id": "default-v1",
                    "created_at": created_at,
                }
            ],
            [
                {
                    "strategy_id": "default",
                    "strategy_version_id": "default-v1",
                    "created_at": created_at,
                }
            ],
        ]
    )
    registry = PostgresStrategyRegistry(FakePool(connection))

    strategies = await registry.list_strategies()
    versions = await registry.list_versions("default")

    assert strategies[0].strategy_id == "default"
    assert strategies[0].active_version_id == "default-v1"
    assert strategies[0].created_at == created_at.astimezone(UTC)
    assert versions[0].strategy_version_id == "default-v1"
    assert versions[0].created_at == created_at.astimezone(UTC)


def test_ensure_utc_rejects_non_datetime_values() -> None:
    with pytest.raises(TypeError, match="expected datetime"):
        _ensure_utc("2026-04-17")


def test_load_json_object_accepts_dict_or_json_string() -> None:
    payload = {"config": {"strategy_id": "default"}}

    assert _load_json_object(payload) == payload
    assert _load_json_object(json.dumps(payload)) == payload


def test_load_json_object_rejects_non_object_payload() -> None:
    with pytest.raises(TypeError, match="JSON object"):
        _load_json_object(["not", "an", "object"])


def test_json_object_rejects_non_dict() -> None:
    with pytest.raises(TypeError, match="risk must decode to a JSON object"):
        _json_object([], "risk")


def test_json_pairs_rejects_non_array_and_bad_pair_shape() -> None:
    with pytest.raises(TypeError, match="JSON array of pairs"):
        _json_pairs("bad")
    with pytest.raises(TypeError, match="JSON array pair"):
        _json_pairs([["ok", 1], ["missing-value"]])


def test_json_string_and_string_list_validate_types() -> None:
    assert _json_string("default", "config.strategy_id") == "default"
    assert _json_string_list(["brier", "pnl"], "eval_spec.metrics") == ["brier", "pnl"]

    with pytest.raises(TypeError, match="config.strategy_id must decode to a string"):
        _json_string(7, "config.strategy_id")
    with pytest.raises(TypeError, match="eval_spec.metrics must decode to a JSON array"):
        _json_string_list("brier", "eval_spec.metrics")
    with pytest.raises(TypeError, match="eval_spec.metrics must decode to a string"):
        _json_string_list(["brier", 1], "eval_spec.metrics")


def test_json_optional_int_and_float_validate_types() -> None:
    assert _json_optional_int(None, "market_selection.resolution_time_max_horizon_days") is None
    assert _json_optional_int(7, "market_selection.resolution_time_max_horizon_days") == 7
    assert _json_float("1.25", "risk.min_order_size_usdc") == pytest.approx(1.25)

    with pytest.raises(TypeError, match="must decode to an int or null"):
        _json_optional_int(True, "market_selection.resolution_time_max_horizon_days")
    with pytest.raises(TypeError, match="float-compatible"):
        _json_float(True, "risk.min_order_size_usdc")
    with pytest.raises(TypeError, match="float-compatible"):
        _json_float(object(), "risk.min_order_size_usdc")


def test_strategy_from_config_json_validates_nested_fields() -> None:
    strategy = _strategy()
    payload = json.loads(serialize_strategy_config_json(*strategy.snapshot()))
    payload["config"]["factor_composition"] = [{"factor_id": "factor-a"}]

    with pytest.raises(TypeError, match=r"config\.factor_composition\.step\.role"):
        _strategy_from_config_json(payload)

    payload = json.loads(serialize_strategy_config_json(*strategy.snapshot()))
    payload["market_selection"]["resolution_time_max_horizon_days"] = True

    with pytest.raises(
        TypeError,
        match="market_selection.resolution_time_max_horizon_days must decode to an int or null",
    ):
        _strategy_from_config_json(payload)

    payload = json.loads(serialize_strategy_config_json(*strategy.snapshot()))
    payload["risk"]["max_daily_drawdown_pct"] = True

    with pytest.raises(
        TypeError,
        match="risk.max_daily_drawdown_pct must decode to a float-compatible value",
    ):
        _strategy_from_config_json(payload)
