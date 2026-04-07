"""HarnessRunner — orchestrates survival gate and functional test execution.

The runner is intentionally decoupled from any concrete candidate
implementation: callers pass a `test_fn` callable that knows how to
execute the actual check against the candidate. This keeps the harness
trivially testable (the unit tests use a `MockCandidate`) and lets the
CP03 CLI plug in subprocess-based test runners later without touching
this module.

Both methods catch exceptions raised by the user-provided callables and
record them on the per-item result so a single broken test cannot crash
the whole run.
"""

from __future__ import annotations

import time
from typing import Awaitable, Callable

from .schema import (
    Benchmark,
    Candidate,
    FunctionalCategoryResult,
    FunctionalResult,
    FunctionalTest,
    FunctionalTestResult,
    SurvivalGateItem,
    SurvivalItemResult,
    SurvivalResult,
)

SurvivalTestFn = Callable[[SurvivalGateItem], Awaitable[bool]]
FunctionalTestFn = Callable[[FunctionalTest], Awaitable[FunctionalTestResult]]


class HarnessRunner:
    """Execute survival-gate and functional tests for a candidate.

    The runner is stateless — instances exist mainly so callers can swap
    in alternate implementations (e.g. a sandboxed subprocess runner) via
    the protocol satisfied by this class.
    """

    async def run_survival_gate(
        self,
        candidate: Candidate,
        benchmark: Benchmark,
        test_fn: SurvivalTestFn,
    ) -> SurvivalResult:
        """Run every survival-gate item; aggregate pass/fail.

        A single ``False`` or exception flips ``all_passed`` to ``False``.
        Each item records elapsed wall-time in milliseconds for later
        diagnostics.
        """
        items: list[SurvivalItemResult] = []
        all_passed = True
        for gate_item in benchmark.survival_gate:
            start = time.perf_counter()
            passed = False
            error: str | None = None
            try:
                passed = await test_fn(gate_item)
            except Exception as exc:  # noqa: BLE001 — runner must not crash
                passed = False
                error = f"{type(exc).__name__}: {exc}"
            elapsed_ms = (time.perf_counter() - start) * 1000.0
            items.append(
                SurvivalItemResult(
                    item_id=gate_item.id,
                    passed=passed,
                    elapsed_ms=elapsed_ms,
                    error=error,
                )
            )
            if not passed:
                all_passed = False
        return SurvivalResult(
            candidate_name=candidate.name,
            items=items,
            all_passed=all_passed,
        )

    async def run_functional_tests(
        self,
        candidate: Candidate,
        benchmark: Benchmark,
        test_fn: FunctionalTestFn,
    ) -> FunctionalResult:
        """Run every functional test, average within categories, weight overall.

        ``category_score`` is the unweighted mean of test scores in that
        category. ``overall_score`` is ``sum(category.weight * category_score)``.
        Empty categories score 0.0 (defensive — schema validation already
        rejects benchmarks where weights don't sum to 1.0).
        """
        category_results: list[FunctionalCategoryResult] = []
        overall = 0.0
        for category in benchmark.functional_tests:
            test_results: list[FunctionalTestResult] = []
            for test in category.tests:
                test_results.append(await self._run_one_functional(test, test_fn))
            if test_results:
                category_score = sum(r.score for r in test_results) / len(test_results)
            else:
                category_score = 0.0
            category_results.append(
                FunctionalCategoryResult(
                    category=category.name,
                    weight=category.weight,
                    tests=test_results,
                    category_score=category_score,
                )
            )
            overall += category.weight * category_score
        return FunctionalResult(
            candidate_name=candidate.name,
            categories=category_results,
            overall_score=overall,
        )

    async def _run_one_functional(
        self,
        test: FunctionalTest,
        test_fn: FunctionalTestFn,
    ) -> FunctionalTestResult:
        try:
            return await test_fn(test)
        except Exception as exc:  # noqa: BLE001 — runner must not crash
            return FunctionalTestResult(
                test_id=test.id,
                metric=test.metric,
                value=0.0,
                score=0.0,
                error=f"{type(exc).__name__}: {exc}",
            )
