"""Convert ``eval_results.yaml`` → ``human_feedback.md`` for auto-research (P2-06).

The harness's ``evaluate-all`` subcommand emits an ``eval_results.yaml``
that summarises every module's evaluation outcome (top candidate,
survival counts, gaps, search hints). auto-research consumes a
human-friendly Markdown counterpart at
``runs/<task>/human_feedback.md`` to plan the next research round.

This script is the bridge: it reads the YAML, formats a Markdown
document grouped by module, and writes it next to the YAML (or to a
caller-specified path). It is intentionally pure I/O — no network, no
auto-research SDK — so it can be invoked from CI, from a Makefile, or
manually after a Phase 2 run.

Usage::

    uv run python scripts/export_to_auto_research.py \
        --eval-results reports/phase2-run-2026-04-08/eval_results.yaml

By default the output lands at ``<eval-results>.parent / human_feedback.md``.
Pass ``--output PATH`` to override.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

import yaml


# ---------------------------------------------------------------------------
# Public API — importable for tests
# ---------------------------------------------------------------------------


def render_human_feedback(eval_results: dict[str, Any]) -> str:
    """Render an ``eval_results`` dict into the auto-research Markdown shape.

    The dict shape matches what
    :func:`pms.tool_harness.aggregate.eval_results_to_dict` produces:

    * ``schema_version`` — int
    * ``generated_at`` — ISO8601 string
    * ``modules`` — list of per-module dicts
    * ``gaps`` — list of module names with no survivor
    * ``summary`` — totals dict

    Returns a string ready to be written to ``human_feedback.md``.
    """
    schema = eval_results.get("schema_version", "?")
    generated_at = eval_results.get("generated_at", "unknown")
    modules: list[dict[str, Any]] = list(eval_results.get("modules", []))
    gaps: list[str] = list(eval_results.get("gaps", []))
    summary: dict[str, Any] = dict(eval_results.get("summary", {}))

    lines: list[str] = []
    lines.append("# Tool Evaluation Feedback — pms")
    lines.append("")
    lines.append(f"- **Generated**: {generated_at}")
    lines.append(f"- **Schema**: pms-eval-results v{schema}")
    lines.append(
        f"- **Modules evaluated**: {summary.get('total_modules', len(modules))}"
    )
    lines.append(
        f"- **Modules with winner**: {summary.get('modules_with_winner', '?')}"
    )
    lines.append(
        f"- **Modules requesting more research**: "
        f"{summary.get('modules_with_gap', len(gaps))}"
    )
    lines.append("")

    lines.append("## Per-module results")
    lines.append("")
    if not modules:
        lines.append("_No modules evaluated._")
        lines.append("")
    for entry in modules:
        lines.extend(_render_module_section(entry))

    lines.append("## Requested research")
    lines.append("")
    if not gaps:
        lines.append(
            "_None — every module has at least one surviving candidate._"
        )
        lines.append("")
    else:
        lines.append(
            "The harness could not find a surviving candidate for the "
            "following modules. auto-research should prioritise additional "
            "candidate discovery for these slots:"
        )
        lines.append("")
        for module_name in gaps:
            lines.append(f"- `{module_name}`")
        lines.append("")
        lines.append(
            "See the per-module sections above for the search hints "
            "captured at evaluation time."
        )
        lines.append("")

    return "\n".join(lines)


def _render_module_section(entry: dict[str, Any]) -> list[str]:
    """Render one module's per-section markdown chunk."""
    name = entry.get("module", "<unknown>")
    top = entry.get("top_candidate")
    score = entry.get("top_score", 0.0)
    evaluated = entry.get("evaluated_count", 0)
    survived = entry.get("survived_count", 0)
    request_more = bool(entry.get("request_more_candidates", False))
    hints: list[str] = list(entry.get("search_hints", []))

    status_emoji = "❌" if request_more else "✅"
    status_text = "Gap — request more candidates" if request_more else "Has winner"

    lines: list[str] = []
    lines.append(f"### `{name}`")
    if top is not None:
        lines.append(f"- **Top candidate**: `{top}` (score {float(score):.2f})")
    else:
        lines.append("- **Top candidate**: _none_")
    lines.append(f"- **Evaluated**: {evaluated} candidate(s)")
    lines.append(f"- **Survived**: {survived}")
    lines.append(f"- **Status**: {status_emoji} {status_text}")
    if hints:
        lines.append("- **Search hints**:")
        for hint in hints:
            lines.append(f"  - {hint}")
    else:
        lines.append("- **Search hints**: _(none)_")
    lines.append("")
    return lines


def load_eval_results(path: Path) -> dict[str, Any]:
    """Read and lightly validate an ``eval_results.yaml`` file.

    Raises:
        FileNotFoundError: if the file does not exist.
        ValueError: if the YAML root is not a mapping or required keys
            are missing.
    """
    if not path.exists():
        raise FileNotFoundError(f"eval_results.yaml not found: {path}")
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"eval_results.yaml root must be a mapping: {path}")
    if "modules" not in raw:
        raise ValueError(
            f"eval_results.yaml missing required key 'modules': {path}"
        )
    return raw


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="export_to_auto_research",
        description=(
            "Convert eval_results.yaml → human_feedback.md for auto-research."
        ),
    )
    parser.add_argument(
        "--eval-results",
        required=True,
        type=Path,
        help="Path to eval_results.yaml emitted by `pms-harness evaluate-all`.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help=(
            "Where to write human_feedback.md. Defaults to "
            "<eval-results>.parent / human_feedback.md."
        ),
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    eval_path: Path = args.eval_results
    output_path: Path = args.output or (eval_path.parent / "human_feedback.md")

    try:
        results = load_eval_results(eval_path)
    except (FileNotFoundError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    text = render_human_feedback(results)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(text, encoding="utf-8")
    print(
        f"Wrote human_feedback.md ({len(results.get('modules', []))} modules, "
        f"{len(results.get('gaps', []))} gap(s)): {output_path}"
    )
    return 0


if __name__ == "__main__":  # pragma: no cover — exercised via tests + manual run
    raise SystemExit(main())
