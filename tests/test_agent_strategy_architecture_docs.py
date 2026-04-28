from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _read_repo_file(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def test_readme_documents_agent_strategy_execution_boundary() -> None:
    readme = _read_repo_file("README.md")

    assert (
        "Agent strategy modules may propose, judge, and explain market actions"
        in readme
    )
    assert "cannot submit orders" in readme
    assert "cannot override risk" in readme
    assert "cannot override reconciliation" in readme
    assert (
        "`TradeIntent | BasketIntent` -> `ExecutionPlan` -> `RiskDecision` -> "
        "`OrderState` -> reconciliation -> evaluator"
    ) in readme
    assert (
        "Sensor, Controller, Actuator, and Evaluator run as concurrent asyncio "
        "tasks with bidirectional feedback edges"
    ) in readme
    assert "Predict-Raven is an external reference pattern" in readme
    assert "not an architecture PMS copies wholesale" in readme


def test_architecture_invariants_define_execution_planner_boundary() -> None:
    invariants = _read_repo_file("agent_docs/architecture-invariants.md")

    assert "ExecutionPlanner is an executability gate" in invariants
    assert "quote, depth, freshness, min-size, tick-size, and slippage" in invariants
    assert "does not select strategies" in invariants
    assert "does not override risk policy" in invariants
    assert "does not submit orders to venues" in invariants
