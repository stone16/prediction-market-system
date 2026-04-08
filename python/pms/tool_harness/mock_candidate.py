"""MockCandidate — a fake tool used by the harness's own self-tests.

The mock unconditionally passes every survival item and returns
deterministic, non-zero scores for every functional test, regardless of
metric type. This is enough to exercise the runner end-to-end without
requiring any external process or network access.

The mock is *not* a general-purpose stub: it implements only the two
async methods that `HarnessRunner` expects (``survival_check`` and
``functional_check``), matching the signatures of the runner's
``test_fn`` parameters.
"""

from __future__ import annotations

from dataclasses import dataclass

from .schema import FunctionalTest, FunctionalTestResult, SurvivalGateItem


@dataclass(frozen=True)
class MockCandidate:
    """A trivial in-process candidate.

    Survival checks always succeed; functional checks return a fixed
    score of 0.8 with a metric-appropriate `value` field. The 0.8 is
    arbitrary but non-trivial — it lets us assert "non-zero overall
    score" without locking the harness into a specific number.
    """

    name: str

    async def survival_check(self, item: SurvivalGateItem) -> bool:
        # Mock survives any check the benchmark throws at it.
        return True

    async def functional_check(self, test: FunctionalTest) -> FunctionalTestResult:
        score = 0.8
        value: float | bool | str
        if test.metric == "boolean":
            value = True
        elif test.metric == "ms":
            # If lower is better, "value" is the latency in ms.
            value = 200.0
        elif test.metric in ("count", "days"):
            value = 100.0
        elif test.metric in ("percentage", "ratio"):
            value = 0.8
        else:
            value = "ok"
        return FunctionalTestResult(
            test_id=test.id,
            metric=test.metric,
            value=value,
            score=score,
            error=None,
        )
