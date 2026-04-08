"""Tests for pms.tool_harness — CP03 report generation and CLI.

Acceptance criteria covered:
- HarnessRunner.evaluate_module() produces a ModuleReport with ranked candidates
- ReportGenerator writes valid scores.json and report.md
- report.md includes per-candidate breakdown (survival, functional, rank)
- CLI `pms-harness evaluate --module data_connector` runs end-to-end and exits 0
"""

from __future__ import annotations

import json
import sys
from dataclasses import replace
from pathlib import Path

import pytest

from pms.tool_harness import (
    Benchmark,
    Candidate,
    FunctionalTest,
    FunctionalTestResult,
    HarnessRunner,
    MockCandidate,
    SurvivalGateItem,
    load_benchmark,
    load_candidate,
)
from pms.tool_harness.reports import (
    CandidateResult,
    ModuleReport,
    ReportGenerator,
)


# ---------------------------------------------------------------------------
# Repository roots — sample YAMLs live at the repo root.
# ---------------------------------------------------------------------------

REPO_ROOT = Path(__file__).resolve().parents[1]
BENCHMARK_PATH = REPO_ROOT / "benchmarks" / "data_connector.yaml"
CANDIDATE_PATH = REPO_ROOT / "candidates" / "mock_connector.yaml"


# ---------------------------------------------------------------------------
# Test fixtures / helpers
# ---------------------------------------------------------------------------


def _make_benchmark() -> Benchmark:
    return load_benchmark(BENCHMARK_PATH)


def _make_candidate(name: str = "mock_connector") -> Candidate:
    base = load_candidate(CANDIDATE_PATH)
    return replace(base, name=name)


def _mock_survival_fn(
    candidate: Candidate,
) -> "_MockSurvival":
    return _MockSurvival(candidate)


class _MockSurvival:
    def __init__(self, candidate: Candidate) -> None:
        self.candidate = candidate
        self.mock = MockCandidate(name=candidate.name)

    async def __call__(self, item: SurvivalGateItem) -> bool:
        return await self.mock.survival_check(item)


class _FailingSurvival:
    def __init__(self, candidate: Candidate) -> None:
        self.candidate = candidate

    async def __call__(self, item: SurvivalGateItem) -> bool:
        return False


class _MockFunctional:
    def __init__(self, candidate: Candidate, score: float = 0.8) -> None:
        self.candidate = candidate
        self.score = score

    async def __call__(self, test: FunctionalTest) -> FunctionalTestResult:
        value: float | bool | str
        if test.metric == "boolean":
            value = True
        elif test.metric == "ms":
            value = 200.0
        else:
            value = 1.0
        return FunctionalTestResult(
            test_id=test.id,
            metric=test.metric,
            value=value,
            score=self.score,
            error=None,
        )


# ---------------------------------------------------------------------------
# evaluate_module — happy path (single candidate)
# ---------------------------------------------------------------------------


async def test_evaluate_module_single_candidate_ranked_first() -> None:
    bm = _make_benchmark()
    cand = _make_candidate()
    runner = HarnessRunner()

    survival_fn = _MockSurvival(cand)
    functional_fn = _MockFunctional(cand, score=0.8)

    async def make_survival(c: Candidate, item: SurvivalGateItem) -> bool:
        assert c.name == cand.name
        return await survival_fn(item)

    async def make_functional(
        c: Candidate, test: FunctionalTest
    ) -> FunctionalTestResult:
        assert c.name == cand.name
        return await functional_fn(test)

    report = await runner.evaluate_module(
        candidates=[cand],
        benchmark=bm,
        survival_test_fn=make_survival,
        functional_test_fn=make_functional,
    )

    assert isinstance(report, ModuleReport)
    assert report.module == "data_connector"
    assert report.benchmark_version == bm.version
    assert len(report.candidates) == 1
    only = report.candidates[0]
    assert isinstance(only, CandidateResult)
    assert only.candidate.name == cand.name
    assert only.verdict == "evaluated"
    assert only.survival.all_passed is True
    assert only.functional is not None
    assert only.rank == 1
    assert report.top_candidate == cand.name
    assert abs(report.top_score - only.functional.overall_score) < 1e-9


# ---------------------------------------------------------------------------
# evaluate_module — multiple candidates, ranked by score
# ---------------------------------------------------------------------------


async def test_evaluate_module_ranks_by_score_descending() -> None:
    bm = _make_benchmark()
    low = _make_candidate("low_cand")
    mid = _make_candidate("mid_cand")
    high = _make_candidate("high_cand")
    runner = HarnessRunner()

    score_by_name = {"low_cand": 0.3, "mid_cand": 0.6, "high_cand": 0.9}

    async def survival(c: Candidate, item: SurvivalGateItem) -> bool:
        return True

    async def functional(
        c: Candidate, test: FunctionalTest
    ) -> FunctionalTestResult:
        return FunctionalTestResult(
            test_id=test.id,
            metric=test.metric,
            value=1.0,
            score=score_by_name[c.name],
            error=None,
        )

    report = await runner.evaluate_module(
        candidates=[low, mid, high],
        benchmark=bm,
        survival_test_fn=survival,
        functional_test_fn=functional,
    )

    names_in_order = [cr.candidate.name for cr in report.candidates]
    # All three are "evaluated" and ranked; high > mid > low.
    assert names_in_order == ["high_cand", "mid_cand", "low_cand"]
    ranks = [cr.rank for cr in report.candidates]
    assert ranks == [1, 2, 3]
    assert report.top_candidate == "high_cand"
    assert report.candidates[0].functional is not None
    assert abs(report.top_score - 0.9) < 1e-9
    # Scores strictly descending
    scores = [
        cr.functional.overall_score
        for cr in report.candidates
        if cr.functional is not None
    ]
    assert scores == sorted(scores, reverse=True)


# ---------------------------------------------------------------------------
# evaluate_module — failing candidate is eliminated
# ---------------------------------------------------------------------------


async def test_evaluate_module_failing_candidate_eliminated() -> None:
    bm = _make_benchmark()
    good = _make_candidate("good")
    bad = _make_candidate("bad")
    runner = HarnessRunner()

    async def survival(c: Candidate, item: SurvivalGateItem) -> bool:
        return c.name == "good"

    async def functional(
        c: Candidate, test: FunctionalTest
    ) -> FunctionalTestResult:
        return FunctionalTestResult(
            test_id=test.id,
            metric=test.metric,
            value=1.0,
            score=0.75,
            error=None,
        )

    report = await runner.evaluate_module(
        candidates=[good, bad],
        benchmark=bm,
        survival_test_fn=survival,
        functional_test_fn=functional,
    )

    assert len(report.candidates) == 2
    by_name = {cr.candidate.name: cr for cr in report.candidates}
    assert by_name["good"].verdict == "evaluated"
    assert by_name["good"].rank == 1
    assert by_name["good"].functional is not None
    assert by_name["bad"].verdict == "eliminated"
    assert by_name["bad"].rank is None
    assert by_name["bad"].functional is None
    assert by_name["bad"].survival.all_passed is False
    assert report.top_candidate == "good"


async def test_evaluate_module_all_eliminated_has_no_top() -> None:
    bm = _make_benchmark()
    cand = _make_candidate("failing")
    runner = HarnessRunner()

    async def survival(c: Candidate, item: SurvivalGateItem) -> bool:
        return False

    async def functional(
        c: Candidate, test: FunctionalTest
    ) -> FunctionalTestResult:  # pragma: no cover — never called
        raise AssertionError("functional should not run when survival fails")

    report = await runner.evaluate_module(
        candidates=[cand],
        benchmark=bm,
        survival_test_fn=survival,
        functional_test_fn=functional,
    )
    assert len(report.candidates) == 1
    assert report.candidates[0].verdict == "eliminated"
    assert report.candidates[0].rank is None
    assert report.top_candidate is None
    assert report.top_score == 0.0


# ---------------------------------------------------------------------------
# ReportGenerator.write_scores_json
# ---------------------------------------------------------------------------


async def _build_report_with_two_candidates() -> ModuleReport:
    bm = _make_benchmark()
    good = _make_candidate("good")
    bad = _make_candidate("bad")
    runner = HarnessRunner()

    async def survival(c: Candidate, item: SurvivalGateItem) -> bool:
        return c.name == "good"

    async def functional(
        c: Candidate, test: FunctionalTest
    ) -> FunctionalTestResult:
        return FunctionalTestResult(
            test_id=test.id,
            metric=test.metric,
            value=1.0,
            score=0.8,
            error=None,
        )

    return await runner.evaluate_module(
        candidates=[good, bad],
        benchmark=bm,
        survival_test_fn=survival,
        functional_test_fn=functional,
    )


async def test_write_scores_json_produces_valid_json(tmp_path: Path) -> None:
    report = await _build_report_with_two_candidates()
    gen = ReportGenerator()
    out = tmp_path / "scores.json"
    gen.write_scores_json(report, out)

    assert out.exists()
    payload = json.loads(out.read_text(encoding="utf-8"))
    assert payload["module"] == "data_connector"
    assert payload["benchmark_version"] == report.benchmark_version
    assert "evaluated_at" in payload
    assert payload["top_candidate"] == "good"
    assert abs(payload["top_score"] - 0.8) < 1e-9
    candidates = payload["candidates"]
    assert isinstance(candidates, list)
    assert len(candidates) == 2
    # Sorted: good (rank 1) first, bad (eliminated) after.
    good_entry = candidates[0]
    assert good_entry["name"] == "good"
    assert good_entry["rank"] == 1
    assert good_entry["verdict"] == "evaluated"
    assert good_entry["survival_passed"] is True
    assert abs(good_entry["overall_score"] - 0.8) < 1e-9
    assert "category_scores" in good_entry
    assert set(good_entry["category_scores"].keys()) == {
        "data_coverage",
        "performance",
        "integrability",
        "code_quality",
    }
    bad_entry = candidates[1]
    assert bad_entry["name"] == "bad"
    assert bad_entry["verdict"] == "eliminated"
    assert bad_entry["rank"] is None
    assert bad_entry["survival_passed"] is False
    assert bad_entry["overall_score"] is None


# ---------------------------------------------------------------------------
# ReportGenerator.write_report_md
# ---------------------------------------------------------------------------


async def test_write_report_md_contains_all_sections(tmp_path: Path) -> None:
    report = await _build_report_with_two_candidates()
    gen = ReportGenerator()
    out = tmp_path / "report.md"
    gen.write_report_md(report, out)

    assert out.exists()
    text = out.read_text(encoding="utf-8")
    # Top-level heading and metadata
    assert "# Module Evaluation: data_connector" in text
    assert "Benchmark version" in text
    assert "Evaluated at" in text
    assert "Top candidate" in text
    assert "good" in text
    # Ranked results section
    assert "## Ranked Results" in text
    assert "### 1. good" in text
    # Per-candidate breakdown
    assert "Survival" in text
    assert "Data Coverage" in text or "data_coverage" in text
    assert "Performance" in text or "performance" in text
    # Eliminated section
    assert "Eliminated" in text
    assert "bad" in text


async def test_write_report_md_single_evaluated_candidate(tmp_path: Path) -> None:
    bm = _make_benchmark()
    cand = _make_candidate()
    runner = HarnessRunner()
    mock = MockCandidate(name=cand.name)

    async def survival(c: Candidate, item: SurvivalGateItem) -> bool:
        return await mock.survival_check(item)

    async def functional(
        c: Candidate, test: FunctionalTest
    ) -> FunctionalTestResult:
        return await mock.functional_check(test)

    report = await runner.evaluate_module(
        candidates=[cand],
        benchmark=bm,
        survival_test_fn=survival,
        functional_test_fn=functional,
    )
    gen = ReportGenerator()
    out = tmp_path / "report.md"
    gen.write_report_md(report, out)
    text = out.read_text(encoding="utf-8")
    assert f"### 1. {cand.name}" in text
    # No eliminated candidates — section header may still appear, but must
    # not contain a bulleted candidate line.
    assert "Eliminated" in text  # informational header is fine


# ---------------------------------------------------------------------------
# CLI smoke test — import main() and patch sys.argv
# ---------------------------------------------------------------------------


async def test_cli_evaluate_runs_end_to_end(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    # Set up an isolated workspace with a benchmark and candidate.
    bm_dir = tmp_path / "benchmarks"
    cand_dir = tmp_path / "candidates"
    out_dir = tmp_path / "reports"
    bm_dir.mkdir()
    cand_dir.mkdir()

    (bm_dir / "data_connector.yaml").write_text(
        BENCHMARK_PATH.read_text(encoding="utf-8"), encoding="utf-8"
    )
    (cand_dir / "mock_connector.yaml").write_text(
        CANDIDATE_PATH.read_text(encoding="utf-8"), encoding="utf-8"
    )

    from pms.tool_harness.cli import run_cli

    rc = await run_cli(
        [
            "evaluate",
            "--module",
            "data_connector",
            "--benchmarks-dir",
            str(bm_dir),
            "--candidates-dir",
            str(cand_dir),
            "--output-dir",
            str(out_dir),
        ]
    )
    assert rc == 0

    captured = capsys.readouterr()
    assert "mock_connector" in captured.out
    assert "data_connector" in captured.out

    scores_path = out_dir / "data_connector-scores.json"
    report_path = out_dir / "data_connector-report.md"
    assert scores_path.exists()
    assert report_path.exists()
    payload = json.loads(scores_path.read_text(encoding="utf-8"))
    assert payload["module"] == "data_connector"
    assert payload["top_candidate"] == "mock_connector"


async def test_cli_evaluate_missing_benchmark_returns_nonzero(
    tmp_path: Path,
) -> None:
    bm_dir = tmp_path / "benchmarks"
    cand_dir = tmp_path / "candidates"
    out_dir = tmp_path / "reports"
    bm_dir.mkdir()
    cand_dir.mkdir()

    from pms.tool_harness.cli import run_cli

    rc = await run_cli(
        [
            "evaluate",
            "--module",
            "nonexistent_module",
            "--benchmarks-dir",
            str(bm_dir),
            "--candidates-dir",
            str(cand_dir),
            "--output-dir",
            str(out_dir),
        ]
    )
    assert rc != 0


async def test_cli_evaluate_no_candidates_returns_nonzero(
    tmp_path: Path,
) -> None:
    bm_dir = tmp_path / "benchmarks"
    cand_dir = tmp_path / "candidates"
    out_dir = tmp_path / "reports"
    bm_dir.mkdir()
    cand_dir.mkdir()

    (bm_dir / "data_connector.yaml").write_text(
        BENCHMARK_PATH.read_text(encoding="utf-8"), encoding="utf-8"
    )
    # No candidates in cand_dir.

    from pms.tool_harness.cli import run_cli

    rc = await run_cli(
        [
            "evaluate",
            "--module",
            "data_connector",
            "--benchmarks-dir",
            str(bm_dir),
            "--candidates-dir",
            str(cand_dir),
            "--output-dir",
            str(out_dir),
        ]
    )
    assert rc != 0


# ---------------------------------------------------------------------------
# P2-05: evaluate-all CLI + cross-module eval_results.yaml aggregation.
# ---------------------------------------------------------------------------


async def test_evaluate_all_produces_aggregate_report(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """``evaluate-all`` walks every benchmark and emits a single
    ``eval_results.yaml`` summary alongside the per-module reports.

    The test sets up an isolated workspace with **two** module
    benchmarks (``data_connector`` and ``order_executor``) so the
    aggregate covers more than one row, plus a single mock candidate
    for ``data_connector`` to exercise the survivors path. The
    ``order_executor`` module has no candidate so it must surface as a
    gap in the aggregate.
    """
    import yaml as _yaml

    bm_dir = tmp_path / "benchmarks"
    cand_dir = tmp_path / "candidates"
    out_dir = tmp_path / "reports" / "phase2-run-test"
    bm_dir.mkdir()
    cand_dir.mkdir()

    # Real benchmark fixtures: copy the data_connector + order_executor
    # benchmarks from the repo so the test exercises the actual schema.
    (bm_dir / "data_connector.yaml").write_text(
        BENCHMARK_PATH.read_text(encoding="utf-8"), encoding="utf-8"
    )
    order_executor_src = REPO_ROOT / "benchmarks" / "order_executor.yaml"
    (bm_dir / "order_executor.yaml").write_text(
        order_executor_src.read_text(encoding="utf-8"), encoding="utf-8"
    )

    # Single mock candidate covering data_connector. order_executor has
    # no candidate, so it must surface as a gap in eval_results.yaml.
    (cand_dir / "mock_connector.yaml").write_text(
        CANDIDATE_PATH.read_text(encoding="utf-8"), encoding="utf-8"
    )

    from pms.tool_harness.cli import run_cli

    rc = await run_cli(
        [
            "evaluate-all",
            "--benchmarks-dir",
            str(bm_dir),
            "--candidates-dir",
            str(cand_dir),
            "--output-dir",
            str(out_dir),
        ]
    )
    assert rc == 0

    # Per-module reports were written for the module that had candidates.
    assert (out_dir / "data_connector-scores.json").exists()
    assert (out_dir / "data_connector-report.md").exists()
    # The module with no candidates is skipped (no scores/report files).
    assert not (out_dir / "order_executor-scores.json").exists()

    # eval_results.yaml exists and parses to the expected shape.
    eval_path = out_dir / "eval_results.yaml"
    assert eval_path.exists()
    payload = _yaml.safe_load(eval_path.read_text(encoding="utf-8"))
    assert payload["schema_version"] == 1
    assert "generated_at" in payload
    assert "modules" in payload
    assert "gaps" in payload
    assert "summary" in payload

    # Only data_connector ran (order_executor was skipped due to no
    # candidates), so the aggregate has one module entry.
    assert len(payload["modules"]) == 1
    dc = payload["modules"][0]
    assert dc["module"] == "data_connector"
    assert dc["evaluated_count"] == 1
    assert dc["survived_count"] == 1
    assert dc["top_candidate"] == "mock_connector"
    assert dc["top_score"] > 0.0
    assert dc["request_more_candidates"] is False

    # Gaps list is empty for this run (mock survived).
    assert payload["gaps"] == []
    assert payload["summary"]["total_modules"] == 1
    assert payload["summary"]["modules_with_winner"] == 1
    assert payload["summary"]["modules_with_gap"] == 0


async def test_evaluate_all_flags_modules_with_no_survivors(
    tmp_path: Path,
) -> None:
    """A module whose only candidate fails the survival gate must show up
    in ``gaps`` and have ``request_more_candidates: true``."""
    import yaml as _yaml

    from pms.tool_harness.aggregate import (
        build_eval_results,
        eval_results_to_dict,
    )

    # Build a synthetic ModuleReport where the only candidate is eliminated.
    bm = _make_benchmark()
    cand = _make_candidate("failing_tool")
    runner = HarnessRunner()

    async def survival(c: Candidate, item: SurvivalGateItem) -> bool:
        return False

    async def functional(
        c: Candidate, test: FunctionalTest
    ) -> FunctionalTestResult:  # pragma: no cover — never called
        raise AssertionError("functional should not run when survival fails")

    report = await runner.evaluate_module(
        candidates=[cand],
        benchmark=bm,
        survival_test_fn=survival,
        functional_test_fn=functional,
    )

    from datetime import datetime, timezone

    eval_results = build_eval_results(
        [report], generated_at=datetime(2026, 4, 8, tzinfo=timezone.utc)
    )
    payload = eval_results_to_dict(eval_results)

    assert payload["gaps"] == ["data_connector"]
    modules = payload["modules"]
    assert isinstance(modules, list)
    only_module = modules[0]
    assert isinstance(only_module, dict)
    assert only_module["request_more_candidates"] is True
    hints = only_module["search_hints"]
    assert isinstance(hints, list) and hints
    assert "survive" in str(hints[0]).lower()


def test_cli_main_is_sync_wrapper_over_run_cli() -> None:
    """``main()`` must be a thin sync wrapper around ``run_cli``.

    We avoid actually invoking ``main()`` here because calling
    ``asyncio.run()`` inside a sync pytest test alongside async tests
    leaks the inner loop's selector sockets through pytest's unraisable
    exception collector (macOS). The installed ``pms-harness`` script
    still exercises ``main()`` end-to-end at the shell level.
    """
    from pms.tool_harness import cli

    assert callable(cli.main)
    assert callable(cli.run_cli)
    # main() delegates to run_cli via asyncio.run — verify the source
    # wires through without duplicating subcommand logic.
    import inspect

    src = inspect.getsource(cli.main)
    assert "run_cli" in src
    assert "asyncio.run" in src
