"""Frozen dataclasses describing benchmark/candidate config and result types.

The shapes mirror the YAML schema documented in `spec.md` (CP02 section).
All result and config types are immutable so the harness can pass them
freely between coroutines without worrying about mutation.

`BenchmarkValidationError` is the single error type raised by the loader
on schema violations. It always carries a `field_path` (dotted YAML path,
list indices in brackets) and a short `expected` description so callers can
surface a precise error message in the CLI.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


class BenchmarkValidationError(ValueError):
    """Raised when a benchmark or candidate YAML fails schema validation.

    Attributes:
        field_path: Dotted path to the offending field, e.g.
            ``"functional_tests.data_coverage.weight"`` or
            ``"survival_gate[0].id"``. Top-level errors use ``"<root>"``.
        expected: Short human-readable description of what was expected,
            e.g. ``"float"``, ``"non-empty str"``, ``"list[str]"``.
    """

    def __init__(self, field_path: str, expected: str, message: str = "") -> None:
        self.field_path = field_path
        self.expected = expected
        super().__init__(f"{field_path}: expected {expected}. {message}".strip())


# ---------------------------------------------------------------------------
# Benchmark schema
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SurvivalGateItem:
    """A single survival-gate check (must pass to proceed to functional tests)."""

    id: str
    test: str
    timeout_seconds: int


@dataclass(frozen=True)
class FunctionalTest:
    """A single scored functional test."""

    id: str
    test: str
    metric: str
    baseline: float | None
    lower_is_better: bool


@dataclass(frozen=True)
class FunctionalCategory:
    """A weighted group of functional tests (e.g. ``data_coverage``)."""

    name: str
    weight: float
    tests: list[FunctionalTest]


@dataclass(frozen=True)
class Benchmark:
    """Top-level benchmark spec for a module (parsed from YAML)."""

    module: str
    version: int
    survival_gate: list[SurvivalGateItem]
    functional_tests: list[FunctionalCategory]


# ---------------------------------------------------------------------------
# Candidate schema
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Candidate:
    """A tool candidate to evaluate against a benchmark."""

    name: str
    repo: str
    language: str
    install: str
    platforms: list[str]
    module: str
    notes: str
    config: dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Survival gate result types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SurvivalItemResult:
    """Outcome of a single survival-gate item."""

    item_id: str
    passed: bool
    elapsed_ms: float
    error: str | None


@dataclass(frozen=True)
class SurvivalResult:
    """Aggregate survival-gate result for a candidate."""

    candidate_name: str
    items: list[SurvivalItemResult]
    all_passed: bool


# ---------------------------------------------------------------------------
# Functional test result types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class FunctionalTestResult:
    """Outcome of a single functional test (already scored 0.0–1.0)."""

    test_id: str
    metric: str
    value: float | bool | str
    score: float
    error: str | None


@dataclass(frozen=True)
class FunctionalCategoryResult:
    """Aggregated functional results for one category."""

    category: str
    weight: float
    tests: list[FunctionalTestResult]
    category_score: float


@dataclass(frozen=True)
class FunctionalResult:
    """Aggregate functional-test result for a candidate."""

    candidate_name: str
    categories: list[FunctionalCategoryResult]
    overall_score: float
