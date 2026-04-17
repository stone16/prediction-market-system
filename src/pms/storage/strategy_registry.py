from __future__ import annotations

from datetime import UTC, datetime
import json
from typing import Any, cast

import asyncpg

from pms.strategies.aggregate import Strategy
from pms.strategies.projections import (
    EvalSpec,
    ForecasterSpec,
    MarketSelectionSpec,
    RiskParams,
    StrategyConfig,
    StrategyRow,
    StrategyVersion,
)
from pms.strategies.versioning import (
    compute_strategy_version_id,
    serialize_strategy_config_json,
)


class PostgresStrategyRegistry:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def create_strategy(
        self,
        strategy_id: str,
        metadata: dict[str, str] | None = None,
    ) -> None:
        query = """
        INSERT INTO strategies (strategy_id, metadata_json)
        VALUES ($1, $2::jsonb)
        ON CONFLICT (strategy_id) DO NOTHING
        """
        metadata_json = json.dumps(
            metadata or {},
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        )
        async with self._pool.acquire() as connection:
            await connection.execute(query, strategy_id, metadata_json)

    async def create_version(self, strategy: Strategy) -> StrategyVersion:
        """Register a strategy snapshot and make that version active.

        Re-registering an existing version is idempotent for the
        `strategy_versions` row but still re-points
        `strategies.active_version_id` to the requested version. S2
        treats `create_version(...)` as the explicit register-and-activate
        entrypoint until a separate activation API exists.
        """
        strategy_id = strategy.config.strategy_id
        strategy_version_id = compute_strategy_version_id(*strategy.snapshot())
        config_json = serialize_strategy_config_json(*strategy.snapshot())
        metadata_json = json.dumps(
            dict(strategy.config.metadata),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        )
        insert_strategy_query = """
        INSERT INTO strategies (strategy_id, metadata_json)
        VALUES ($1, $2::jsonb)
        ON CONFLICT (strategy_id) DO NOTHING
        """
        insert_version_query = """
        INSERT INTO strategy_versions (
            strategy_version_id,
            strategy_id,
            config_json,
            created_at
        ) VALUES ($1, $2, $3::jsonb, clock_timestamp())
        ON CONFLICT (strategy_version_id) DO NOTHING
        RETURNING created_at
        """
        select_version_query = """
        SELECT created_at
        FROM strategy_versions
        WHERE strategy_id = $1 AND strategy_version_id = $2
        """
        update_strategy_query = """
        UPDATE strategies
        SET active_version_id = $2
        WHERE strategy_id = $1
        """
        async with self._pool.acquire() as connection:
            async with connection.transaction():
                await connection.execute(
                    insert_strategy_query,
                    strategy_id,
                    metadata_json,
                )
                created_at = await connection.fetchval(
                    insert_version_query,
                    strategy_version_id,
                    strategy_id,
                    config_json,
                )
                if created_at is None:
                    created_at = await connection.fetchval(
                        select_version_query,
                        strategy_id,
                        strategy_version_id,
                    )
                if not isinstance(created_at, datetime):
                    msg = "strategy_versions.created_at did not return a timestamp"
                    raise TypeError(msg)
                await connection.execute(
                    update_strategy_query,
                    strategy_id,
                    strategy_version_id,
                )
        return StrategyVersion(
            strategy_id=strategy_id,
            strategy_version_id=strategy_version_id,
            created_at=_ensure_utc(created_at),
        )

    async def get_by_id(self, strategy_id: str) -> Strategy | None:
        query = """
        SELECT versions.config_json
        FROM strategies
        LEFT JOIN strategy_versions AS versions
            ON versions.strategy_version_id = strategies.active_version_id
        WHERE strategies.strategy_id = $1
        """
        async with self._pool.acquire() as connection:
            row = await connection.fetchrow(query, strategy_id)
        if row is None or row["config_json"] is None:
            return None
        return _strategy_from_config_json(row["config_json"])

    async def list_strategies(self) -> list[StrategyRow]:
        query = """
        SELECT strategy_id, active_version_id, created_at
        FROM strategies
        ORDER BY strategy_id ASC
        """
        async with self._pool.acquire() as connection:
            rows = await connection.fetch(query)
        return [
            StrategyRow(
                strategy_id=row["strategy_id"],
                active_version_id=row["active_version_id"],
                created_at=_ensure_utc(row["created_at"]),
            )
            for row in rows
        ]

    async def list_versions(self, strategy_id: str) -> list[StrategyVersion]:
        query = """
        SELECT strategy_id, strategy_version_id, created_at
        FROM strategy_versions
        WHERE strategy_id = $1
        ORDER BY created_at ASC, strategy_version_id ASC
        """
        async with self._pool.acquire() as connection:
            rows = await connection.fetch(query, strategy_id)
        return [
            StrategyVersion(
                strategy_id=row["strategy_id"],
                strategy_version_id=row["strategy_version_id"],
                created_at=_ensure_utc(row["created_at"]),
            )
            for row in rows
        ]


def _ensure_utc(value: object) -> datetime:
    if not isinstance(value, datetime):
        msg = f"expected datetime, got {type(value).__name__}"
        raise TypeError(msg)
    return value.astimezone(UTC)


def _strategy_from_config_json(raw_value: object) -> Strategy:
    payload = _load_json_object(raw_value)
    config_payload = _json_object(payload["config"], "config")
    risk_payload = _json_object(payload["risk"], "risk")
    eval_spec_payload = _json_object(payload["eval_spec"], "eval_spec")
    forecaster_payload = _json_object(payload["forecaster"], "forecaster")
    market_selection_payload = _json_object(
        payload["market_selection"],
        "market_selection",
    )
    return Strategy(
        config=StrategyConfig(
            strategy_id=_json_string(config_payload["strategy_id"], "config.strategy_id"),
            factor_composition=tuple(
                (
                    _json_string(item[0], "config.factor_composition.key"),
                    _json_float(item[1], "config.factor_composition"),
                )
                for item in _json_pairs(config_payload["factor_composition"])
            ),
            metadata=tuple(
                (
                    _json_string(item[0], "config.metadata.key"),
                    _json_string(item[1], "config.metadata.value"),
                )
                for item in _json_pairs(config_payload["metadata"])
            ),
        ),
        risk=RiskParams(
            max_position_notional_usdc=_json_float(
                risk_payload["max_position_notional_usdc"],
                "risk.max_position_notional_usdc",
            ),
            max_daily_drawdown_pct=_json_float(
                risk_payload["max_daily_drawdown_pct"],
                "risk.max_daily_drawdown_pct",
            ),
            min_order_size_usdc=_json_float(
                risk_payload["min_order_size_usdc"],
                "risk.min_order_size_usdc",
            ),
        ),
        eval_spec=EvalSpec(
            metrics=tuple(_json_string_list(eval_spec_payload["metrics"], "eval_spec.metrics"))
        ),
        forecaster=ForecasterSpec(
            forecasters=tuple(
                (
                    _json_string(item[0], "forecaster.forecasters.name"),
                    tuple(
                        (
                            _json_string(param[0], "forecaster.forecasters.param.key"),
                            _json_string(param[1], "forecaster.forecasters.param.value"),
                        )
                        for param in _json_pairs(item[1])
                    ),
                )
                for item in _json_pairs(forecaster_payload["forecasters"])
            )
        ),
        market_selection=MarketSelectionSpec(
            venue=_json_string(market_selection_payload["venue"], "market_selection.venue"),
            resolution_time_max_horizon_days=_json_optional_int(
                market_selection_payload["resolution_time_max_horizon_days"],
                "market_selection.resolution_time_max_horizon_days",
            ),
            volume_min_usdc=_json_float(
                market_selection_payload["volume_min_usdc"],
                "market_selection.volume_min_usdc",
            ),
        ),
    )


def _load_json_object(raw_value: object) -> dict[str, object]:
    if isinstance(raw_value, str):
        loaded = json.loads(raw_value)
    else:
        loaded = raw_value
    if not isinstance(loaded, dict):
        msg = "strategy config payload must be a JSON object"
        raise TypeError(msg)
    return cast(dict[str, object], loaded)


def _json_object(value: object, field_name: str) -> dict[str, object]:
    if not isinstance(value, dict):
        msg = f"{field_name} must decode to a JSON object"
        raise TypeError(msg)
    return cast(dict[str, object], value)


def _json_pairs(value: object) -> list[list[object]]:
    if not isinstance(value, list):
        msg = "expected JSON array of pairs"
        raise TypeError(msg)
    pairs = cast(list[object], value)
    decoded_pairs: list[list[object]] = []
    for pair in pairs:
        if not isinstance(pair, list) or len(pair) != 2:
            msg = "expected JSON array pair"
            raise TypeError(msg)
        decoded_pairs.append(cast(list[object], pair))
    return decoded_pairs


def _json_string(value: object, field_name: str) -> str:
    if not isinstance(value, str):
        msg = f"{field_name} must decode to a string"
        raise TypeError(msg)
    return value


def _json_string_list(value: object, field_name: str) -> list[str]:
    if not isinstance(value, list):
        msg = f"{field_name} must decode to a JSON array"
        raise TypeError(msg)
    decoded: list[str] = []
    for item in value:
        decoded.append(_json_string(item, field_name))
    return decoded


def _json_optional_int(value: object, field_name: str) -> int | None:
    if value is None:
        return None
    if not isinstance(value, int) or isinstance(value, bool):
        msg = f"{field_name} must decode to an int or null"
        raise TypeError(msg)
    return value


def _json_float(value: object, field_name: str) -> float:
    if not isinstance(value, (int, float, str)):
        msg = f"{field_name} must decode to a float-compatible value"
        raise TypeError(msg)
    return float(value)
