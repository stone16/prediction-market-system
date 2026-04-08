"""Report generation for tool-harness module evaluations.

CP03 extends the CP02 harness with three pieces:

* :class:`CandidateResult` and :class:`ModuleReport` — frozen value types
  that bundle a single candidate's survival + functional results with a
  rank and verdict, and the aggregated per-module report the CLI emits.
* :meth:`HarnessRunner.evaluate_module` — runs survival gate and (if it
  passes) functional tests against every candidate, then ranks survivors
  by ``overall_score`` descending. Candidates that fail the survival gate
  are tagged ``verdict="eliminated"`` and do **not** receive a rank; they
  are still included in the report for diagnostics.
* :class:`ReportGenerator` — writes ``scores.json`` (machine-readable) and
  ``report.md`` (human-readable) from a :class:`ModuleReport`.

Design notes:

* ``CandidateResult.verdict`` is a :class:`typing.Literal` string so both
  JSON output and mypy get cheap, accurate type coverage without adding a
  full :class:`enum.Enum`.
* Functional tests are only run when the survival gate passes (see spec
  §harness: survival gate is a hard filter). The ``functional`` field is
  therefore ``None`` for eliminated candidates rather than an empty
  :class:`FunctionalResult`, so callers can't confuse "ran and scored 0"
  with "never ran".
* ``ModuleReport.evaluated_at`` is always timezone-aware UTC so downstream
  serializers produce stable, sortable strings.
* Ranking is stable: when two evaluated candidates share a score, they
  keep their original list order. This is what ``list.sort`` guarantees
  and what the spec's example top-N selection expects.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

from .schema import (
    Candidate,
    FunctionalResult,
    SurvivalResult,
)


Verdict = Literal["passed", "eliminated", "evaluated"]


# ---------------------------------------------------------------------------
# Report value types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CandidateResult:
    """A single candidate's evaluation outcome.

    ``functional`` is ``None`` iff the survival gate failed. ``rank`` is
    set only for candidates with ``verdict == "evaluated"``; eliminated
    candidates carry ``rank = None``.
    """

    candidate: Candidate
    survival: SurvivalResult
    functional: FunctionalResult | None
    rank: int | None
    verdict: Verdict


@dataclass(frozen=True)
class ModuleReport:
    """Aggregated evaluation report for one module.

    ``candidates`` is ordered so that evaluated candidates come first in
    rank order (1, 2, 3, ...), followed by eliminated candidates in the
    order they were submitted. ``top_candidate`` and ``top_score`` point
    at the rank-1 evaluated candidate; both are ``None`` / ``0.0`` when
    every candidate was eliminated.
    """

    module: str
    benchmark_version: int
    evaluated_at: datetime
    candidates: list[CandidateResult]
    top_candidate: str | None
    top_score: float


# ---------------------------------------------------------------------------
# Report generator
# ---------------------------------------------------------------------------


class ReportGenerator:
    """Render :class:`ModuleReport` instances to JSON and Markdown files.

    The generator is stateless; each ``write_*`` method takes the target
    path and produces a deterministic file on disk. Parent directories
    are created on demand so callers only need to supply the final path.
    """

    def write_scores_json(self, report: ModuleReport, output_path: Path) -> None:
        """Write a machine-readable JSON report.

        Schema (per spec §CP03):

        .. code-block:: text

            {
              "module": str,
              "benchmark_version": int,
              "evaluated_at": iso8601 str,
              "top_candidate": str | null,
              "top_score": float,
              "candidates": [
                {
                  "name": str,
                  "rank": int | null,
                  "verdict": "evaluated" | "eliminated",
                  "survival_passed": bool,
                  "overall_score": float | null,
                  "category_scores": {<category>: float}
                }
              ]
            }
        """
        output_path.parent.mkdir(parents=True, exist_ok=True)
        payload: dict[str, object] = {
            "module": report.module,
            "benchmark_version": report.benchmark_version,
            "evaluated_at": report.evaluated_at.isoformat(),
            "top_candidate": report.top_candidate,
            "top_score": report.top_score,
            "candidates": [self._candidate_to_dict(cr) for cr in report.candidates],
        }
        output_path.write_text(
            json.dumps(payload, indent=2, sort_keys=False) + "\n",
            encoding="utf-8",
        )

    def write_report_md(self, report: ModuleReport, output_path: Path) -> None:
        """Write a human-readable Markdown report.

        The top section lists metadata and the winning candidate; the
        ``## Ranked Results`` section shows one ``###`` subsection per
        evaluated candidate (with category breakdown) in rank order; the
        ``### Eliminated`` subsection lists candidates that failed the
        survival gate along with the first failing gate's error (or
        ``"unknown failure"`` if the test simply returned ``False``).
        """
        output_path.parent.mkdir(parents=True, exist_ok=True)
        lines: list[str] = []
        lines.append(f"# Module Evaluation: {report.module}")
        lines.append("")
        lines.append(f"- **Benchmark version**: {report.benchmark_version}")
        lines.append(f"- **Evaluated at**: {report.evaluated_at.isoformat()}")

        evaluated = [cr for cr in report.candidates if cr.verdict == "evaluated"]
        eliminated = [cr for cr in report.candidates if cr.verdict == "eliminated"]
        lines.append(
            f"- **Candidates evaluated**: {len(report.candidates)} "
            f"({len(evaluated)} survived)"
        )
        if report.top_candidate is not None:
            lines.append(
                f"- **Top candidate**: {report.top_candidate} "
                f"(score {report.top_score:.2f})"
            )
        else:
            lines.append("- **Top candidate**: _none (all candidates eliminated)_")
        lines.append("")

        lines.append("## Ranked Results")
        lines.append("")
        if not evaluated:
            lines.append("_No candidates survived the survival gate._")
            lines.append("")
        for cr in evaluated:
            assert cr.functional is not None  # type narrowing for mypy
            lines.append(
                f"### {cr.rank}. {cr.candidate.name} — "
                f"{cr.functional.overall_score:.2f}"
            )
            survived = sum(1 for it in cr.survival.items if it.passed)
            total = len(cr.survival.items)
            lines.append(
                f"- **Survival**: all {total} gates passed ({survived}/{total})"
            )
            for cat_result in cr.functional.categories:
                pretty = _prettify(cat_result.category)
                lines.append(
                    f"- **{pretty}**: {cat_result.category_score:.2f} "
                    f"(weight {cat_result.weight:.2f})"
                )
            if cr.candidate.notes:
                lines.append(f"- **Notes**: {cr.candidate.notes}")
            lines.append("")

        lines.append("### Eliminated")
        lines.append("")
        if not eliminated:
            lines.append("_None — every candidate survived the gate._")
        else:
            for cr in eliminated:
                reason = _first_failure_reason(cr.survival)
                lines.append(f"- **{cr.candidate.name}**: {reason}")
        lines.append("")

        output_path.write_text("\n".join(lines), encoding="utf-8")

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _candidate_to_dict(self, cr: CandidateResult) -> dict[str, object]:
        entry: dict[str, object] = {
            "name": cr.candidate.name,
            "rank": cr.rank,
            "verdict": cr.verdict,
            "survival_passed": cr.survival.all_passed,
        }
        if cr.functional is None:
            entry["overall_score"] = None
            entry["category_scores"] = {}
        else:
            entry["overall_score"] = cr.functional.overall_score
            entry["category_scores"] = {
                cat.category: cat.category_score for cat in cr.functional.categories
            }
        return entry


# ---------------------------------------------------------------------------
# Markdown rendering helpers
# ---------------------------------------------------------------------------


def _prettify(name: str) -> str:
    """Turn a ``snake_case`` category id into a Title Case heading."""
    return name.replace("_", " ").title()


def _first_failure_reason(survival: SurvivalResult) -> str:
    """Return a short human-readable reason for the first failed gate.

    Falls back to ``"unknown failure"`` if the test returned ``False``
    with no error attached.
    """
    for item in survival.items:
        if not item.passed:
            if item.error:
                return f"Failed survival gate '{item.item_id}': {item.error}"
            return f"Failed survival gate '{item.item_id}'"
    return "unknown failure"  # pragma: no cover — only reachable if misused


# ---------------------------------------------------------------------------
# Factory for evaluated_at (patchable in tests if needed)
# ---------------------------------------------------------------------------


def utc_now() -> datetime:
    """Return the current UTC timestamp (timezone-aware)."""
    return datetime.now(timezone.utc)
