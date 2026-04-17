"""Strategy aggregate that owns immutable projection state."""

from __future__ import annotations

from typing import TypeVar

from .projections import (
    EvalSpec,
    ForecasterSpec,
    MarketSelectionSpec,
    RiskParams,
    StrategyConfig,
)


ProjectionT = TypeVar("ProjectionT")


class Strategy:
    __slots__ = (
        "_config",
        "_risk",
        "_eval_spec",
        "_forecaster",
        "_market_selection",
    )

    def __init__(
        self,
        *,
        config: StrategyConfig,
        risk: RiskParams,
        eval_spec: EvalSpec,
        forecaster: ForecasterSpec,
        market_selection: MarketSelectionSpec,
    ) -> None:
        self._config = self._require_projection("config", config)
        self._risk = self._require_projection("risk", risk)
        self._eval_spec = self._require_projection("eval_spec", eval_spec)
        self._forecaster = self._require_projection("forecaster", forecaster)
        self._market_selection = self._require_projection(
            "market_selection",
            market_selection,
        )

    @staticmethod
    def _require_projection(
        field_name: str,
        value: ProjectionT | None,
    ) -> ProjectionT:
        if value is None:
            raise TypeError(f"{field_name} must not be None")
        return value

    @property
    def config(self) -> StrategyConfig:
        return self._config

    @property
    def risk(self) -> RiskParams:
        return self._risk

    @property
    def eval_spec(self) -> EvalSpec:
        return self._eval_spec

    @property
    def forecaster(self) -> ForecasterSpec:
        return self._forecaster

    @property
    def market_selection(self) -> MarketSelectionSpec:
        return self._market_selection

    def snapshot(
        self,
    ) -> tuple[
        StrategyConfig,
        RiskParams,
        EvalSpec,
        ForecasterSpec,
        MarketSelectionSpec,
    ]:
        return (
            self._config,
            self._risk,
            self._eval_spec,
            self._forecaster,
            self._market_selection,
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Strategy):
            return NotImplemented
        return self.snapshot() == other.snapshot()

    def __hash__(self) -> int:
        return hash(self.snapshot())
