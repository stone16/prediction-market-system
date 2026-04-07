"""Tests for pms.tool_harness — schema, loader, runner, and mock candidate.

Acceptance criteria covered:
- benchmarks/data_connector.yaml exists with valid survival_gate and functional_tests
- candidates/mock_connector.yaml exists as a test candidate
- HarnessRunner.run_survival_gate() returns SurvivalResult with pass/fail per item
- HarnessRunner.run_functional_tests() returns weighted scored results
- Mock candidate passes survival gate and produces non-zero scores
- Invalid YAML raises BenchmarkValidationError with field path and expected type
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from pms.tool_harness import (
    Benchmark,
    BenchmarkValidationError,
    Candidate,
    FunctionalCategory,
    FunctionalCategoryResult,
    FunctionalResult,
    FunctionalTest,
    FunctionalTestResult,
    HarnessRunner,
    MockCandidate,
    SurvivalGateItem,
    SurvivalItemResult,
    SurvivalResult,
    load_benchmark,
    load_candidate,
)


# ---------------------------------------------------------------------------
# Repository roots — sample YAMLs live at the repo root.
# ---------------------------------------------------------------------------

REPO_ROOT = Path(__file__).resolve().parents[1]
BENCHMARK_PATH = REPO_ROOT / "benchmarks" / "data_connector.yaml"
CANDIDATE_PATH = REPO_ROOT / "candidates" / "mock_connector.yaml"


# ---------------------------------------------------------------------------
# Sample YAML files exist
# ---------------------------------------------------------------------------


def test_data_connector_yaml_exists() -> None:
    assert BENCHMARK_PATH.exists(), f"missing benchmark: {BENCHMARK_PATH}"


def test_mock_connector_yaml_exists() -> None:
    assert CANDIDATE_PATH.exists(), f"missing candidate: {CANDIDATE_PATH}"


# ---------------------------------------------------------------------------
# load_benchmark — happy path
# ---------------------------------------------------------------------------


def test_load_benchmark_happy_path() -> None:
    bm = load_benchmark(BENCHMARK_PATH)
    assert isinstance(bm, Benchmark)
    assert bm.module == "data_connector"
    assert bm.version >= 1
    # Survival gate covers install / connect / fetch_one (per spec).
    assert len(bm.survival_gate) == 3
    ids = {item.id for item in bm.survival_gate}
    assert {"install", "connect", "fetch_one"}.issubset(ids)
    for item in bm.survival_gate:
        assert isinstance(item, SurvivalGateItem)
        assert item.timeout_seconds > 0
    # Four categories, each weight 0.25, summing to 1.0.
    assert len(bm.functional_tests) == 4
    cats = {c.name for c in bm.functional_tests}
    assert {"data_coverage", "performance", "integrability", "code_quality"} == cats
    total = sum(c.weight for c in bm.functional_tests)
    assert abs(total - 1.0) < 1e-9
    for cat in bm.functional_tests:
        assert isinstance(cat, FunctionalCategory)
        assert 2 <= len(cat.tests) <= 3
        for t in cat.tests:
            assert isinstance(t, FunctionalTest)
            assert t.metric  # non-empty


def test_load_candidate_happy_path() -> None:
    cand = load_candidate(CANDIDATE_PATH)
    assert isinstance(cand, Candidate)
    assert cand.name == "mock_connector"
    assert cand.module == "data_connector"
    assert cand.language == "python"
    assert "polymarket" in cand.platforms
    assert "kalshi" in cand.platforms
    assert isinstance(cand.config, dict)
    assert cand.config.get("mock_markets") == 150


# ---------------------------------------------------------------------------
# load_benchmark — error cases (BenchmarkValidationError with field path + expected)
# ---------------------------------------------------------------------------


def _write(tmp_path: Path, name: str, content: str) -> Path:
    p = tmp_path / name
    p.write_text(content)
    return p


def test_benchmark_missing_module(tmp_path: Path) -> None:
    p = _write(
        tmp_path,
        "bm.yaml",
        """
version: 1
survival_gate: []
functional_tests: {}
""",
    )
    with pytest.raises(BenchmarkValidationError) as ei:
        load_benchmark(p)
    assert ei.value.field_path == "module"
    assert "str" in ei.value.expected


def test_benchmark_module_wrong_type(tmp_path: Path) -> None:
    p = _write(
        tmp_path,
        "bm.yaml",
        """
module: 123
version: 1
survival_gate: []
functional_tests: {}
""",
    )
    with pytest.raises(BenchmarkValidationError) as ei:
        load_benchmark(p)
    assert ei.value.field_path == "module"
    assert "str" in ei.value.expected


def test_benchmark_version_wrong_type(tmp_path: Path) -> None:
    p = _write(
        tmp_path,
        "bm.yaml",
        """
module: data_connector
version: "one"
survival_gate: []
functional_tests: {}
""",
    )
    with pytest.raises(BenchmarkValidationError) as ei:
        load_benchmark(p)
    assert ei.value.field_path == "version"
    assert "int" in ei.value.expected


def test_benchmark_survival_item_missing_id(tmp_path: Path) -> None:
    p = _write(
        tmp_path,
        "bm.yaml",
        """
module: data_connector
version: 1
survival_gate:
  - test: "install the thing"
    timeout_seconds: 30
functional_tests: {}
""",
    )
    with pytest.raises(BenchmarkValidationError) as ei:
        load_benchmark(p)
    assert ei.value.field_path == "survival_gate[0].id"
    assert "str" in ei.value.expected


def test_benchmark_category_weight_wrong_type(tmp_path: Path) -> None:
    p = _write(
        tmp_path,
        "bm.yaml",
        """
module: data_connector
version: 1
survival_gate: []
functional_tests:
  data_coverage:
    weight: "heavy"
    tests:
      - id: t1
        test: "covers stuff"
        metric: count
        baseline: 10
        lower_is_better: false
""",
    )
    with pytest.raises(BenchmarkValidationError) as ei:
        load_benchmark(p)
    assert ei.value.field_path == "functional_tests.data_coverage.weight"
    assert "float" in ei.value.expected


def test_benchmark_category_weights_must_sum_to_one(tmp_path: Path) -> None:
    p = _write(
        tmp_path,
        "bm.yaml",
        """
module: data_connector
version: 1
survival_gate: []
functional_tests:
  data_coverage:
    weight: 0.3
    tests:
      - id: t1
        test: "covers stuff"
        metric: count
        baseline: 10
        lower_is_better: false
  performance:
    weight: 0.3
    tests:
      - id: t2
        test: "fast"
        metric: ms
        baseline: 100
        lower_is_better: true
""",
    )
    with pytest.raises(BenchmarkValidationError) as ei:
        load_benchmark(p)
    assert ei.value.field_path == "functional_tests"
    assert "sum" in ei.value.expected.lower() or "1.0" in ei.value.expected


def test_benchmark_test_metric_missing(tmp_path: Path) -> None:
    p = _write(
        tmp_path,
        "bm.yaml",
        """
module: data_connector
version: 1
survival_gate: []
functional_tests:
  data_coverage:
    weight: 1.0
    tests:
      - id: t1
        test: "no metric"
        baseline: 10
        lower_is_better: false
""",
    )
    with pytest.raises(BenchmarkValidationError) as ei:
        load_benchmark(p)
    assert ei.value.field_path == "functional_tests.data_coverage.tests[0].metric"
    assert "str" in ei.value.expected


def test_benchmark_baseline_null_allowed(tmp_path: Path) -> None:
    p = _write(
        tmp_path,
        "bm.yaml",
        """
module: data_connector
version: 1
survival_gate: []
functional_tests:
  data_coverage:
    weight: 1.0
    tests:
      - id: t1
        test: "informational only"
        metric: count
        baseline: null
        lower_is_better: false
""",
    )
    bm = load_benchmark(p)
    assert bm.functional_tests[0].tests[0].baseline is None


def test_benchmark_lower_is_better_defaults_false(tmp_path: Path) -> None:
    p = _write(
        tmp_path,
        "bm.yaml",
        """
module: data_connector
version: 1
survival_gate: []
functional_tests:
  data_coverage:
    weight: 1.0
    tests:
      - id: t1
        test: "default lower_is_better"
        metric: count
        baseline: 10
""",
    )
    bm = load_benchmark(p)
    assert bm.functional_tests[0].tests[0].lower_is_better is False


def test_load_benchmark_top_level_must_be_mapping(tmp_path: Path) -> None:
    p = _write(tmp_path, "bm.yaml", "- just\n- a\n- list\n")
    with pytest.raises(BenchmarkValidationError) as ei:
        load_benchmark(p)
    assert ei.value.field_path == "<root>"
    assert "mapping" in ei.value.expected.lower()


# ---------------------------------------------------------------------------
# load_candidate — error cases
# ---------------------------------------------------------------------------


def test_candidate_missing_name(tmp_path: Path) -> None:
    p = _write(
        tmp_path,
        "cand.yaml",
        """
repo: https://example.com
language: python
install: "echo"
platforms: [polymarket]
module: data_connector
notes: ""
config: {}
""",
    )
    with pytest.raises(BenchmarkValidationError) as ei:
        load_candidate(p)
    assert ei.value.field_path == "name"
    assert "str" in ei.value.expected


def test_candidate_platforms_must_be_list_of_str(tmp_path: Path) -> None:
    p = _write(
        tmp_path,
        "cand.yaml",
        """
name: c
repo: https://example.com
language: python
install: "echo"
platforms: "polymarket"
module: data_connector
notes: ""
config: {}
""",
    )
    with pytest.raises(BenchmarkValidationError) as ei:
        load_candidate(p)
    assert ei.value.field_path == "platforms"
    assert "list" in ei.value.expected


def test_candidate_config_default_empty(tmp_path: Path) -> None:
    p = _write(
        tmp_path,
        "cand.yaml",
        """
name: c
repo: https://example.com
language: python
install: "echo"
platforms: [polymarket]
module: data_connector
notes: ""
""",
    )
    cand = load_candidate(p)
    assert cand.config == {}


# ---------------------------------------------------------------------------
# HarnessRunner.run_survival_gate
# ---------------------------------------------------------------------------


async def test_run_survival_gate_all_pass() -> None:
    bm = load_benchmark(BENCHMARK_PATH)
    cand = load_candidate(CANDIDATE_PATH)
    mock = MockCandidate(name=cand.name)
    runner = HarnessRunner()
    result = await runner.run_survival_gate(cand, bm, mock.survival_check)
    assert isinstance(result, SurvivalResult)
    assert result.candidate_name == cand.name
    assert len(result.items) == len(bm.survival_gate)
    assert result.all_passed is True
    for item in result.items:
        assert isinstance(item, SurvivalItemResult)
        assert item.passed is True
        assert item.elapsed_ms >= 0
        assert item.error is None


async def test_run_survival_gate_records_failure() -> None:
    bm = load_benchmark(BENCHMARK_PATH)
    cand = load_candidate(CANDIDATE_PATH)
    runner = HarnessRunner()

    async def always_false(item: SurvivalGateItem) -> bool:
        return False

    result = await runner.run_survival_gate(cand, bm, always_false)
    assert result.all_passed is False
    assert all(i.passed is False for i in result.items)


async def test_run_survival_gate_handles_exceptions() -> None:
    bm = load_benchmark(BENCHMARK_PATH)
    cand = load_candidate(CANDIDATE_PATH)
    runner = HarnessRunner()

    async def boom(item: SurvivalGateItem) -> bool:
        raise RuntimeError("kaboom")

    result = await runner.run_survival_gate(cand, bm, boom)
    assert result.all_passed is False
    for item in result.items:
        assert item.passed is False
        assert item.error is not None
        assert "kaboom" in item.error


# ---------------------------------------------------------------------------
# HarnessRunner.run_functional_tests
# ---------------------------------------------------------------------------


async def test_run_functional_tests_weighted_score() -> None:
    bm = load_benchmark(BENCHMARK_PATH)
    cand = load_candidate(CANDIDATE_PATH)
    mock = MockCandidate(name=cand.name)
    runner = HarnessRunner()
    result = await runner.run_functional_tests(cand, bm, mock.functional_check)
    assert isinstance(result, FunctionalResult)
    assert result.candidate_name == cand.name
    assert len(result.categories) == len(bm.functional_tests)
    # Mock returns deterministic non-zero scores per spec.
    assert result.overall_score > 0.0
    assert result.overall_score <= 1.0
    # Per-category scores are averages of test scores; overall is weighted sum.
    expected_overall = 0.0
    for cat_result in result.categories:
        assert isinstance(cat_result, FunctionalCategoryResult)
        assert 0.0 <= cat_result.category_score <= 1.0
        avg = sum(t.score for t in cat_result.tests) / len(cat_result.tests)
        assert abs(cat_result.category_score - avg) < 1e-9
        expected_overall += cat_result.weight * cat_result.category_score
    assert abs(result.overall_score - expected_overall) < 1e-9
    # Every test result has the right shape.
    for cat_result in result.categories:
        for tr in cat_result.tests:
            assert isinstance(tr, FunctionalTestResult)
            assert tr.test_id
            assert tr.metric
            assert 0.0 <= tr.score <= 1.0


async def test_run_functional_tests_records_test_exceptions() -> None:
    bm = load_benchmark(BENCHMARK_PATH)
    cand = load_candidate(CANDIDATE_PATH)
    runner = HarnessRunner()

    async def boom(test: FunctionalTest) -> FunctionalTestResult:
        raise ValueError("nope")

    result = await runner.run_functional_tests(cand, bm, boom)
    # All tests recorded as score 0.0 with error captured.
    for cat_result in result.categories:
        for tr in cat_result.tests:
            assert tr.score == 0.0
            assert tr.error is not None
            assert "nope" in tr.error
    assert result.overall_score == 0.0


# ---------------------------------------------------------------------------
# Mock candidate end-to-end
# ---------------------------------------------------------------------------


async def test_mock_candidate_passes_survival_and_scores_nonzero() -> None:
    bm = load_benchmark(BENCHMARK_PATH)
    cand = load_candidate(CANDIDATE_PATH)
    mock = MockCandidate(name=cand.name)
    runner = HarnessRunner()

    survival = await runner.run_survival_gate(cand, bm, mock.survival_check)
    assert survival.all_passed is True

    functional = await runner.run_functional_tests(cand, bm, mock.functional_check)
    assert functional.overall_score > 0.0


# ---------------------------------------------------------------------------
# BenchmarkValidationError shape
# ---------------------------------------------------------------------------


def test_benchmark_validation_error_str_includes_field_and_type() -> None:
    err = BenchmarkValidationError("functional_tests.data_coverage.weight", "float")
    s = str(err)
    assert "functional_tests.data_coverage.weight" in s
    assert "float" in s
    assert err.field_path == "functional_tests.data_coverage.weight"
    assert err.expected == "float"


def test_benchmark_validation_error_is_value_error() -> None:
    err = BenchmarkValidationError("x", "int")
    assert isinstance(err, ValueError)
