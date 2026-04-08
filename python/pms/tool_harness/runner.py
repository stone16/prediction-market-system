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

CP03 adds :meth:`HarnessRunner.evaluate_module` which runs every candidate
through the survival gate + functional tests and ranks the survivors. It
wraps the caller-provided per-candidate test functions into the existing
single-candidate ``run_*`` methods via small closures so the original
signatures remain unchanged.
"""

from __future__ import annotations

import asyncio
import time
from typing import Awaitable, Callable

from .reports import CandidateResult, ModuleReport, utc_now
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

#: Per-candidate survival test callable used by :meth:`evaluate_module`.
ModuleSurvivalTestFn = Callable[[Candidate, SurvivalGateItem], Awaitable[bool]]
#: Per-candidate functional test callable used by :meth:`evaluate_module`.
ModuleFunctionalTestFn = Callable[
    [Candidate, FunctionalTest], Awaitable[FunctionalTestResult]
]


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
                # review-loop fix f7: enforce per-item ``timeout_seconds``
                # so a hung test can never stall the harness. Without this
                # wrapper the runner awaited the user-provided coroutine
                # directly and the timeout field on ``SurvivalGateItem``
                # was effectively documentation only.
                passed = await asyncio.wait_for(
                    test_fn(gate_item),
                    timeout=gate_item.timeout_seconds,
                )
            except asyncio.TimeoutError:
                passed = False
                error = f"timeout after {gate_item.timeout_seconds}s"
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

    async def evaluate_module(
        self,
        candidates: list[Candidate],
        benchmark: Benchmark,
        survival_test_fn: ModuleSurvivalTestFn,
        functional_test_fn: ModuleFunctionalTestFn,
    ) -> ModuleReport:
        """Evaluate every candidate and rank the survivors.

        For each candidate:

        1. Run the survival gate via :meth:`run_survival_gate`. If any
           item fails, emit a :class:`CandidateResult` with
           ``verdict="eliminated"`` and ``functional=None`` — functional
           tests are intentionally **skipped** so the report never mixes
           "ran and scored 0" with "never ran".
        2. If survival passes, run :meth:`run_functional_tests` and emit
           a ``verdict="evaluated"`` result.

        After every candidate has been processed, evaluated results are
        sorted by ``overall_score`` descending (stable sort preserves
        input order for ties) and rank 1..N is assigned in order.
        Eliminated candidates are appended in their original order with
        ``rank=None`` so the report still documents why they failed.

        The per-candidate ``test_fn`` closures reuse the existing
        single-candidate runner methods unchanged — we bind the current
        candidate into a tiny lambda so ``run_survival_gate`` /
        ``run_functional_tests`` keep their CP02 signatures.
        """
        evaluated_results: list[CandidateResult] = []
        eliminated_results: list[CandidateResult] = []

        for candidate in candidates:
            async def _survival(
                item: SurvivalGateItem, _c: Candidate = candidate
            ) -> bool:
                return await survival_test_fn(_c, item)

            survival = await self.run_survival_gate(candidate, benchmark, _survival)

            if not survival.all_passed:
                eliminated_results.append(
                    CandidateResult(
                        candidate=candidate,
                        survival=survival,
                        functional=None,
                        rank=None,
                        verdict="eliminated",
                    )
                )
                continue

            async def _functional(
                test: FunctionalTest, _c: Candidate = candidate
            ) -> FunctionalTestResult:
                return await functional_test_fn(_c, test)

            functional = await self.run_functional_tests(
                candidate, benchmark, _functional
            )
            evaluated_results.append(
                CandidateResult(
                    candidate=candidate,
                    survival=survival,
                    functional=functional,
                    rank=None,  # Assigned below after sorting.
                    verdict="evaluated",
                )
            )

        # Stable descending sort by overall_score. mypy: functional is not None.
        evaluated_results.sort(
            key=lambda cr: (
                cr.functional.overall_score if cr.functional is not None else 0.0
            ),
            reverse=True,
        )
        ranked: list[CandidateResult] = []
        for idx, cr in enumerate(evaluated_results, start=1):
            ranked.append(
                CandidateResult(
                    candidate=cr.candidate,
                    survival=cr.survival,
                    functional=cr.functional,
                    rank=idx,
                    verdict="evaluated",
                )
            )

        all_candidates = ranked + eliminated_results

        top_candidate: str | None = None
        top_score = 0.0
        if ranked and ranked[0].functional is not None:
            top_candidate = ranked[0].candidate.name
            top_score = ranked[0].functional.overall_score

        return ModuleReport(
            module=benchmark.module,
            benchmark_version=benchmark.version,
            evaluated_at=utc_now(),
            candidates=all_candidates,
            top_candidate=top_candidate,
            top_score=top_score,
        )
