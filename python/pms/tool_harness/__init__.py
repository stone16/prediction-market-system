"""Tool evaluation harness — benchmark/candidate schema, loader, and runner.

CP02 introduces a self-contained harness used to evaluate third-party tool
candidates against a per-module benchmark. The harness has three pieces:

* `schema` — frozen dataclasses describing benchmark/candidate YAML and the
  shape of survival/functional results, plus :class:`BenchmarkValidationError`.
* `loader` — pure functions that parse YAML and validate it against the
  schema, raising precise errors with field paths.
* `runner` — :class:`HarnessRunner` orchestrates execution of survival gate
  and functional tests against any candidate-supplied callable.
* `mock_candidate` — a tiny in-process candidate used by the harness's own
  test suite (and as a smoke test in later checkpoints).
"""

from .aggregate import (
    EvalResults,
    ModuleAggregate,
    build_eval_results,
    build_module_aggregate,
    eval_results_to_dict,
    write_eval_results_yaml,
)
from .loader import load_benchmark, load_candidate
from .mock_candidate import MockCandidate
from .reports import CandidateResult, ModuleReport, ReportGenerator
from .runner import HarnessRunner
from .subprocess_runner import (
    SubprocessRunnerFactory,
    SubprocessSession,
    UnsupportedCandidateError,
    make_subprocess_test_fn,
)
from .schema import (
    Benchmark,
    BenchmarkValidationError,
    Candidate,
    FunctionalCategory,
    FunctionalCategoryResult,
    FunctionalResult,
    FunctionalTest,
    FunctionalTestResult,
    SurvivalGateItem,
    SurvivalItemResult,
    SurvivalResult,
)

__all__ = [
    "Benchmark",
    "BenchmarkValidationError",
    "Candidate",
    "CandidateResult",
    "EvalResults",
    "FunctionalCategory",
    "FunctionalCategoryResult",
    "FunctionalResult",
    "FunctionalTest",
    "FunctionalTestResult",
    "HarnessRunner",
    "MockCandidate",
    "ModuleAggregate",
    "ModuleReport",
    "ReportGenerator",
    "SubprocessRunnerFactory",
    "SubprocessSession",
    "SurvivalGateItem",
    "SurvivalItemResult",
    "SurvivalResult",
    "UnsupportedCandidateError",
    "build_eval_results",
    "build_module_aggregate",
    "eval_results_to_dict",
    "load_benchmark",
    "load_candidate",
    "make_subprocess_test_fn",
    "write_eval_results_yaml",
]
