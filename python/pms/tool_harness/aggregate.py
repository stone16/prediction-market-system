"""Cross-module aggregation for ``pms-harness evaluate-all`` (P2-05).

The CLI's ``evaluate-all`` subcommand runs every available benchmark in
sequence and writes per-module reports plus a single ``eval_results.yaml``
that auto-research consumes as feedback for the next research round.

The YAML format is intentionally narrow — it carries the few facts
auto-research needs to decide whether to keep researching:

* per-module ``evaluated_count`` and ``survived_count``
* the winning candidate (or ``null``)
* a ``request_more_candidates`` flag for any module that did not produce
  a survivor
* a ``search_hints`` list per module that auto-research can echo into
  the next research topic
* a top-level ``gaps`` list summarising modules with no survivor

The full per-candidate breakdown stays in the per-module ``scores.json``
files; ``eval_results.yaml`` is the executive summary.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

import yaml

from .reports import ModuleReport


# ---------------------------------------------------------------------------
# Aggregate value types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ModuleAggregate:
    """One module's slot in the cross-module summary."""

    module: str
    evaluated_count: int
    survived_count: int
    top_candidate: str | None
    top_score: float
    request_more_candidates: bool
    search_hints: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class EvalResults:
    """The full aggregate emitted to ``eval_results.yaml``.

    Attributes:
        schema_version: Bumped on any breaking change to the YAML shape so
            auto-research can refuse incompatible inputs explicitly.
        generated_at: ISO8601 UTC timestamp from the run.
        modules: Per-module aggregate summaries, sorted by module name.
        gaps: Modules where no candidate survived the gate.
    """

    schema_version: int
    generated_at: datetime
    modules: list[ModuleAggregate]
    gaps: list[str]


SCHEMA_VERSION: int = 1


# ---------------------------------------------------------------------------
# Aggregation logic
# ---------------------------------------------------------------------------


def build_module_aggregate(report: ModuleReport) -> ModuleAggregate:
    """Reduce a per-module :class:`ModuleReport` to a :class:`ModuleAggregate`.

    ``request_more_candidates`` is set whenever no candidate survived the
    gate. ``search_hints`` carries human-readable next-step suggestions
    derived from the report — e.g. "no candidate passed survival" or
    "winning candidate scored below 0.5".
    """
    evaluated_count = len(report.candidates)
    survived = [
        cr
        for cr in report.candidates
        if cr.verdict == "evaluated" and cr.survival.all_passed
    ]
    survived_count = len(survived)

    request_more_candidates = survived_count == 0
    hints: list[str] = []
    if survived_count == 0:
        hints.append(
            f"No candidate survived the {report.module} survival gate; "
            f"research additional tools."
        )
    elif report.top_score < 0.5:
        hints.append(
            f"Top {report.module} candidate scored {report.top_score:.2f}; "
            f"consider higher-quality alternatives."
        )

    return ModuleAggregate(
        module=report.module,
        evaluated_count=evaluated_count,
        survived_count=survived_count,
        top_candidate=report.top_candidate,
        top_score=report.top_score,
        request_more_candidates=request_more_candidates,
        search_hints=hints,
    )


def build_eval_results(
    reports: list[ModuleReport], generated_at: datetime
) -> EvalResults:
    """Combine per-module reports into a single :class:`EvalResults`."""
    aggregates = sorted(
        (build_module_aggregate(r) for r in reports),
        key=lambda m: m.module,
    )
    gaps = [m.module for m in aggregates if m.request_more_candidates]
    return EvalResults(
        schema_version=SCHEMA_VERSION,
        generated_at=generated_at,
        modules=aggregates,
        gaps=gaps,
    )


# ---------------------------------------------------------------------------
# YAML serialisation
# ---------------------------------------------------------------------------


def eval_results_to_dict(results: EvalResults) -> dict[str, object]:
    """Convert :class:`EvalResults` to a plain dict for YAML serialisation."""
    return {
        "schema_version": results.schema_version,
        "generated_at": results.generated_at.isoformat(),
        "modules": [
            {
                "module": m.module,
                "evaluated_count": m.evaluated_count,
                "survived_count": m.survived_count,
                "top_candidate": m.top_candidate,
                "top_score": m.top_score,
                "request_more_candidates": m.request_more_candidates,
                "search_hints": list(m.search_hints),
            }
            for m in results.modules
        ],
        "gaps": list(results.gaps),
        "summary": {
            "total_modules": len(results.modules),
            "modules_with_winner": sum(
                1 for m in results.modules if m.top_candidate is not None
            ),
            "modules_with_gap": len(results.gaps),
        },
    }


def write_eval_results_yaml(results: EvalResults, output_path: Path) -> None:
    """Write ``eval_results.yaml`` to disk in the auto-research feedback shape."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    payload = eval_results_to_dict(results)
    output_path.write_text(
        yaml.safe_dump(payload, sort_keys=False, default_flow_style=False),
        encoding="utf-8",
    )
