from __future__ import annotations

from collections.abc import Iterable

from pms.strategies.base import StrategyModule


StrategyModuleKey = tuple[str, str]


def _key(strategy_id: str, strategy_version_id: str) -> StrategyModuleKey:
    if not strategy_id:
        raise ValueError("strategy_id must be non-empty")
    if not strategy_version_id:
        raise ValueError("strategy_version_id must be non-empty")
    return (strategy_id, strategy_version_id)


class StrategyModuleRegistry:
    def __init__(self, modules: Iterable[StrategyModule] = ()) -> None:
        self._modules: dict[StrategyModuleKey, StrategyModule] = {}
        for module in modules:
            self.register(module)

    def register(self, module: StrategyModule) -> None:
        module_key = _key(module.strategy_id, module.strategy_version_id)
        if module_key in self._modules:
            raise ValueError(
                "strategy module already registered: "
                f"{module.strategy_id}@{module.strategy_version_id}"
            )
        self._modules[module_key] = module

    def get(self, strategy_id: str, strategy_version_id: str) -> StrategyModule | None:
        return self._modules.get(_key(strategy_id, strategy_version_id))

    def require(self, strategy_id: str, strategy_version_id: str) -> StrategyModule:
        module = self.get(strategy_id, strategy_version_id)
        if module is None:
            raise KeyError(f"strategy module not registered: {strategy_id}@{strategy_version_id}")
        return module

    def list_modules(self) -> tuple[StrategyModule, ...]:
        return tuple(self._modules.values())
