"""CLI entry point for the tool harness.

Usage::

    uv run pms-harness evaluate --module data_connector
    uv run pms-harness evaluate-all --output-dir reports/phase2-run

The ``evaluate`` subcommand runs one module benchmark; ``evaluate-all``
walks every benchmark in ``benchmarks/`` and emits a cross-module
``eval_results.yaml`` summary that auto-research consumes as feedback.

The CLI uses :mod:`argparse` rather than Typer/Click so the harness
stays on stdlib-only runtime dependencies (:mod:`pyyaml` is the single
non-stdlib dep carried over from CP02).
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

from .aggregate import (
    build_eval_results,
    write_eval_results_yaml,
)
from .loader import load_benchmark, load_candidate
from .mock_candidate import MockCandidate
from .reports import ModuleReport, ReportGenerator, utc_now
from .runner import HarnessRunner
from .schema import (
    Benchmark,
    BenchmarkValidationError,
    Candidate,
    FunctionalTest,
    FunctionalTestResult,
    SurvivalGateItem,
)
from .subprocess_runner import DEFAULT_PROBES_DIR, SubprocessRunnerFactory


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pms-harness",
        description="Tool harness for evaluating pms module candidates.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    eval_parser = subparsers.add_parser(
        "evaluate",
        help="Evaluate candidates against a module benchmark.",
    )
    eval_parser.add_argument(
        "--module",
        required=True,
        help="Module name, e.g. 'data_connector' (selects benchmarks/<module>.yaml).",
    )
    eval_parser.add_argument(
        "--benchmarks-dir",
        default=Path("benchmarks"),
        type=Path,
        help="Directory containing benchmark YAMLs (default: benchmarks/).",
    )
    eval_parser.add_argument(
        "--candidates-dir",
        default=Path("candidates"),
        type=Path,
        help="Directory containing candidate YAMLs (default: candidates/).",
    )
    eval_parser.add_argument(
        "--output-dir",
        default=Path("reports"),
        type=Path,
        help="Where to write report.md and scores.json (default: reports/).",
    )
    eval_parser.add_argument(
        "--probes-dir",
        default=DEFAULT_PROBES_DIR,
        type=Path,
        help=(
            "Directory containing probe scripts for real candidates "
            "(default: candidates/probes/)."
        ),
    )

    # ------------------------------------------------------------------
    # P2-05: ``evaluate-all`` walks every benchmark and writes one
    # cross-module ``eval_results.yaml`` plus per-module reports.
    # ------------------------------------------------------------------
    all_parser = subparsers.add_parser(
        "evaluate-all",
        help="Run every benchmark in the benchmarks dir and produce a "
        "cross-module eval_results.yaml summary.",
    )
    all_parser.add_argument(
        "--benchmarks-dir",
        default=Path("benchmarks"),
        type=Path,
        help="Directory containing benchmark YAMLs (default: benchmarks/).",
    )
    all_parser.add_argument(
        "--candidates-dir",
        default=Path("candidates"),
        type=Path,
        help="Directory containing candidate YAMLs (default: candidates/).",
    )
    all_parser.add_argument(
        "--output-dir",
        required=True,
        type=Path,
        help="Where to write per-module reports + eval_results.yaml.",
    )
    all_parser.add_argument(
        "--probes-dir",
        default=DEFAULT_PROBES_DIR,
        type=Path,
        help=(
            "Directory containing probe scripts for real candidates "
            "(default: candidates/probes/)."
        ),
    )
    return parser


def _discover_candidates(candidates_dir: Path, module: str) -> list[Candidate]:
    """Load every ``candidates/*.yaml`` whose ``module`` matches ``module``."""
    if not candidates_dir.exists():
        return []
    found: list[Candidate] = []
    for path in sorted(candidates_dir.glob("*.yaml")):
        try:
            candidate = load_candidate(path)
        except BenchmarkValidationError as exc:
            print(
                f"warning: skipping invalid candidate {path}: {exc}",
                file=sys.stderr,
            )
            continue
        if candidate.module == module:
            found.append(candidate)
    return found


async def _evaluate_one_module(
    benchmark: Benchmark,
    candidates: list[Candidate],
    probes_dir: Path,
) -> ModuleReport:
    """Run survival + functional gates for one module and return its report.

    The temp venvs/workdirs created by the subprocess runner are
    released via ``cleanup_all`` in the ``finally`` block so a crashed
    module run can't leak disk into ``/tmp``.
    """
    runner = HarnessRunner()
    # P2-02: real candidates run inside isolated subprocess venvs via the
    # SubprocessRunnerFactory; the in-process MockCandidate is reserved
    # for the special ``language: mock`` candidate used by self-tests.
    # Functional tests for real tools are still placeholder until P2-04;
    # the dispatch keeps them out of the runner's exception path so
    # survival results surface in the report.
    mocks: dict[str, MockCandidate] = {
        c.name: MockCandidate(name=c.name) for c in candidates if c.language == "mock"
    }
    subprocess_factory = SubprocessRunnerFactory(probes_dir=probes_dir)

    async def survival_fn(
        candidate: Candidate, item: SurvivalGateItem
    ) -> bool:
        if candidate.language == "mock":
            return await mocks[candidate.name].survival_check(item)
        return await subprocess_factory.survival_check(candidate, item)

    async def functional_fn(
        candidate: Candidate, test: FunctionalTest
    ) -> FunctionalTestResult:
        if candidate.language == "mock":
            return await mocks[candidate.name].functional_check(test)
        return FunctionalTestResult(
            test_id=test.id,
            metric=test.metric,
            value=0.0,
            score=0.0,
            error="functional probing not implemented for real candidates yet (P2-04)",
        )

    try:
        return await runner.evaluate_module(
            candidates=candidates,
            benchmark=benchmark,
            survival_test_fn=survival_fn,
            functional_test_fn=functional_fn,
        )
    finally:
        subprocess_factory.cleanup_all()


async def _cmd_evaluate(args: argparse.Namespace) -> int:
    module: str = args.module
    benchmark_path: Path = Path(args.benchmarks_dir) / f"{module}.yaml"
    candidates_dir: Path = Path(args.candidates_dir)
    output_dir: Path = Path(args.output_dir)

    if not benchmark_path.exists():
        print(
            f"error: benchmark not found for module '{module}': {benchmark_path}",
            file=sys.stderr,
        )
        return 2

    try:
        benchmark = load_benchmark(benchmark_path)
    except BenchmarkValidationError as exc:
        print(f"error: invalid benchmark {benchmark_path}: {exc}", file=sys.stderr)
        return 2

    candidates = _discover_candidates(candidates_dir, module)
    if not candidates:
        print(
            f"error: no candidates found for module '{module}' in {candidates_dir}",
            file=sys.stderr,
        )
        return 3

    report = await _evaluate_one_module(
        benchmark, candidates, Path(args.probes_dir)
    )

    output_dir.mkdir(parents=True, exist_ok=True)
    scores_path = output_dir / f"{module}-scores.json"
    report_path = output_dir / f"{module}-report.md"

    generator = ReportGenerator()
    generator.write_scores_json(report, scores_path)
    generator.write_report_md(report, report_path)

    if report.top_candidate is not None:
        print(
            f"Evaluated {len(candidates)} candidate(s) for module '{module}'. "
            f"Top: {report.top_candidate} (score {report.top_score:.2f}). "
            f"Reports: {scores_path}, {report_path}"
        )
    else:
        print(
            f"Evaluated {len(candidates)} candidate(s) for module '{module}'. "
            f"No candidates survived the survival gate. "
            f"Reports: {scores_path}, {report_path}"
        )

    return 0


async def _cmd_evaluate_all(args: argparse.Namespace) -> int:
    """``evaluate-all`` — run every benchmark and emit a cross-module summary.

    Discovers benchmarks via ``benchmarks_dir.glob("*.yaml")`` (sorted by
    name for deterministic output), evaluates each in sequence, and
    writes one ``eval_results.yaml`` plus the same per-module
    ``<module>-scores.json`` and ``<module>-report.md`` files the
    single-module ``evaluate`` command produces.

    A non-zero exit code is returned only when no benchmarks could be
    found; individual module failures are recorded in their reports
    (and surface as gaps in ``eval_results.yaml``) but do not abort the
    overall run.
    """
    benchmarks_dir = Path(args.benchmarks_dir)
    candidates_dir = Path(args.candidates_dir)
    output_dir = Path(args.output_dir)
    probes_dir = Path(args.probes_dir)

    if not benchmarks_dir.exists():
        print(
            f"error: benchmarks dir not found: {benchmarks_dir}", file=sys.stderr
        )
        return 2

    benchmark_paths = sorted(benchmarks_dir.glob("*.yaml"))
    if not benchmark_paths:
        print(
            f"error: no benchmark YAMLs in {benchmarks_dir}", file=sys.stderr
        )
        return 3

    output_dir.mkdir(parents=True, exist_ok=True)
    generator = ReportGenerator()
    reports: list[ModuleReport] = []

    for bm_path in benchmark_paths:
        try:
            benchmark = load_benchmark(bm_path)
        except BenchmarkValidationError as exc:
            print(
                f"warning: skipping invalid benchmark {bm_path}: {exc}",
                file=sys.stderr,
            )
            continue

        module = benchmark.module
        candidates = _discover_candidates(candidates_dir, module)
        if not candidates:
            print(
                f"warning: no candidates for module '{module}' — skipping",
                file=sys.stderr,
            )
            continue

        report = await _evaluate_one_module(benchmark, candidates, probes_dir)
        generator.write_scores_json(
            report, output_dir / f"{module}-scores.json"
        )
        generator.write_report_md(
            report, output_dir / f"{module}-report.md"
        )
        reports.append(report)
        if report.top_candidate is not None:
            print(
                f"[{module}] {len(candidates)} candidate(s) → "
                f"top: {report.top_candidate} ({report.top_score:.2f})"
            )
        else:
            print(
                f"[{module}] {len(candidates)} candidate(s) → "
                f"no survivors (gap)"
            )

    if not reports:
        print("error: no modules evaluated", file=sys.stderr)
        return 4

    eval_results = build_eval_results(reports, generated_at=utc_now())
    eval_results_path = output_dir / "eval_results.yaml"
    write_eval_results_yaml(eval_results, eval_results_path)
    print(
        f"Wrote eval_results.yaml ({len(eval_results.modules)} modules, "
        f"{len(eval_results.gaps)} gap(s)): {eval_results_path}"
    )
    return 0


async def run_cli(argv: list[str]) -> int:
    """Async CLI entry point — parses ``argv`` and dispatches to a subcommand.

    Separated from the sync :func:`main` wrapper so tests can ``await``
    this directly inside their own event loop (avoids spawning a second
    event loop via :func:`asyncio.run` when pytest-asyncio already owns
    one, which on macOS leaks the new loop's selector sockets at
    teardown).
    """
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.command == "evaluate":
        return await _cmd_evaluate(args)
    if args.command == "evaluate-all":
        return await _cmd_evaluate_all(args)

    parser.print_help(sys.stderr)
    return 1


def main() -> int:
    """Sync CLI entry point used by the ``pms-harness`` installed script."""
    return asyncio.run(run_cli(sys.argv[1:]))


if __name__ == "__main__":  # pragma: no cover — exercised via `pms-harness` script
    raise SystemExit(main())
