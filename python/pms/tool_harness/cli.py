"""CLI entry point for the tool harness.

Usage::

    uv run pms-harness evaluate --module data_connector

The ``evaluate`` subcommand:

1. Loads ``<benchmarks-dir>/<module>.yaml`` (defaults to ``benchmarks/``).
2. Scans ``<candidates-dir>/*.yaml`` for every candidate whose ``module``
   field matches the requested module.
3. Runs :meth:`pms.tool_harness.HarnessRunner.evaluate_module` against
   each candidate. CP03 only ships an in-process :class:`MockCandidate`;
   later checkpoints will replace the wrapped test functions with
   real subprocess-based runners without touching this module.
4. Writes ``<output-dir>/<module>-scores.json`` and
   ``<output-dir>/<module>-report.md``.
5. Prints a one-line summary to stdout.

The CLI uses :mod:`argparse` rather than Typer/Click so the harness
stays on stdlib-only runtime dependencies (:mod:`pyyaml` is the single
non-stdlib dep carried over from CP02).
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

from .loader import load_benchmark, load_candidate
from .mock_candidate import MockCandidate
from .reports import ReportGenerator
from .runner import HarnessRunner
from .schema import (
    BenchmarkValidationError,
    Candidate,
    FunctionalTest,
    FunctionalTestResult,
    SurvivalGateItem,
)


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

    runner = HarnessRunner()
    # CP03 wires every candidate to the in-process MockCandidate; later
    # checkpoints will swap this for a subprocess-based runner.
    mocks: dict[str, MockCandidate] = {
        c.name: MockCandidate(name=c.name) for c in candidates
    }

    async def survival_fn(
        candidate: Candidate, item: SurvivalGateItem
    ) -> bool:
        return await mocks[candidate.name].survival_check(item)

    async def functional_fn(
        candidate: Candidate, test: FunctionalTest
    ) -> FunctionalTestResult:
        return await mocks[candidate.name].functional_check(test)

    report = await runner.evaluate_module(
        candidates=candidates,
        benchmark=benchmark,
        survival_test_fn=survival_fn,
        functional_test_fn=functional_fn,
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

    parser.print_help(sys.stderr)
    return 1


def main() -> int:
    """Sync CLI entry point used by the ``pms-harness`` installed script."""
    return asyncio.run(run_cli(sys.argv[1:]))


if __name__ == "__main__":  # pragma: no cover — exercised via `pms-harness` script
    raise SystemExit(main())
