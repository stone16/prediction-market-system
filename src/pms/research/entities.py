"""Research-side entities for backtest execution and reporting."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
import json
from typing import Any, Literal, TypeAlias, cast


EvaluationRankingMetric = Literal["brier", "sharpe", "pnl_cum"]
PortfolioTargetSide = Literal["buy_yes", "buy_no"]
PortfolioTargetKey: TypeAlias = tuple[str, str, PortfolioTargetSide, datetime]


@dataclass(frozen=True, slots=True)
class PortfolioTarget:
    strategy_id: str
    strategy_version_id: str
    targets: Mapping[PortfolioTargetKey, float]

    def __post_init__(self) -> None:
        if not self.strategy_id:
            msg = "PortfolioTarget.strategy_id must be non-empty"
            raise ValueError(msg)
        if not self.strategy_version_id:
            msg = "PortfolioTarget.strategy_version_id must be non-empty"
            raise ValueError(msg)
        for market_id, token_id, side, timestamp in self.targets:
            if not market_id:
                msg = "PortfolioTarget targets require non-empty market_id"
                raise ValueError(msg)
            if not token_id:
                msg = "PortfolioTarget targets require non-empty token_id"
                raise ValueError(msg)
            if side not in ("buy_yes", "buy_no"):
                msg = f"Unsupported PortfolioTarget side {side!r}"
                raise ValueError(msg)
            if timestamp.tzinfo is None or timestamp.utcoffset() is None:
                msg = "PortfolioTarget timestamps must be timezone-aware"
                raise ValueError(msg)

    def to_json_value(self) -> list[dict[str, object]]:
        rows = [
            {
                "market_id": market_id,
                "token_id": token_id,
                "side": side,
                "timestamp": timestamp.isoformat(),
                "target_size_usdc": float(size),
            }
            for (market_id, token_id, side, timestamp), size in sorted(
                self.targets.items(),
                key=lambda item: (
                    item[0][0],
                    item[0][1],
                    item[0][2],
                    item[0][3].isoformat(),
                ),
            )
        ]
        return rows

    @classmethod
    def from_json_value(
        cls,
        *,
        strategy_id: str,
        strategy_version_id: str,
        payload: object,
    ) -> "PortfolioTarget":
        if not isinstance(payload, list):
            msg = "PortfolioTarget payload must decode to a JSON array"
            raise TypeError(msg)
        targets: dict[PortfolioTargetKey, float] = {}
        for raw_item in payload:
            if not isinstance(raw_item, dict):
                msg = "PortfolioTarget entries must decode to JSON objects"
                raise TypeError(msg)
            market_id = _required_str(raw_item, "market_id")
            token_id = _required_str(raw_item, "token_id")
            side = cast(PortfolioTargetSide, _required_str(raw_item, "side"))
            timestamp = datetime.fromisoformat(_required_str(raw_item, "timestamp"))
            if timestamp.tzinfo is None or timestamp.utcoffset() is None:
                msg = "PortfolioTarget payload timestamps must be timezone-aware"
                raise ValueError(msg)
            target_size = _required_float(raw_item, "target_size_usdc")
            targets[(market_id, token_id, side, timestamp)] = target_size
        return cls(
            strategy_id=strategy_id,
            strategy_version_id=strategy_version_id,
            targets=targets,
        )


@dataclass(frozen=True, slots=True)
class RankedStrategy:
    strategy_id: str
    strategy_version_id: str
    metric_value: float
    rank: int


@dataclass(frozen=True, slots=True)
class EvaluationReport:
    report_id: str
    run_id: str
    ranking_metric: EvaluationRankingMetric
    ranked_strategies: tuple[RankedStrategy, ...]
    benchmark_rows: tuple[Mapping[str, object], ...]
    attribution_commentary: str
    warnings: tuple[str, ...]
    next_action: str
    generated_at: datetime


def serialize_portfolio_target_json(target: PortfolioTarget) -> str:
    return json.dumps(
        target.to_json_value(),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    )


def deserialize_portfolio_target_json(
    *,
    strategy_id: str,
    strategy_version_id: str,
    raw_value: object,
) -> PortfolioTarget:
    decoded = json.loads(raw_value) if isinstance(raw_value, str) else raw_value
    return PortfolioTarget.from_json_value(
        strategy_id=strategy_id,
        strategy_version_id=strategy_version_id,
        payload=decoded,
    )


def _required_str(payload: Mapping[str, object], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value:
        msg = f"PortfolioTarget payload field {key!r} must be a non-empty string"
        raise TypeError(msg)
    return value


def _required_float(payload: Mapping[str, object], key: str) -> float:
    value = payload.get(key)
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        msg = f"PortfolioTarget payload field {key!r} must be numeric"
        raise TypeError(msg)
    return float(value)


__all__ = [
    "EvaluationRankingMetric",
    "EvaluationReport",
    "PortfolioTarget",
    "PortfolioTargetKey",
    "PortfolioTargetSide",
    "RankedStrategy",
    "deserialize_portfolio_target_json",
    "serialize_portfolio_target_json",
]
