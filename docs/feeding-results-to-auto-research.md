# Feeding Phase 2 results back to auto-research (P2-06) — HISTORICAL (pms-v1)

> **⚠ Deprecated — pms-v1 historical.** This workflow depended on
> `pms.tool_harness.aggregate.write_eval_results_yaml`, the
> `pms-harness evaluate-all` CLI, and `scripts/export_to_auto_research.py` —
> none of which exist in pms-v2. The v2 architecture does not ship a
> benchmark/candidate evaluation pipeline. If a future phase re-introduces
> tool-catalog feedback to auto-research, this file can serve as a shape
> reference, but do not expect any of the commands below to run against
> the current tree.

---


After a `pms-harness evaluate-all` run produces `eval_results.yaml`,
the manual handoff back to `auto-research` is two steps:

1. Convert the YAML into the Markdown shape `auto-research` consumes.
2. Drop the resulting `human_feedback.md` into the matching `auto-research`
   run directory and trigger the next research round.

This document walks through both steps. The conversion is automated by
`scripts/export_to_auto_research.py`; the placement and trigger are
manual because they happen across two repos.

## Prerequisites

- A completed `pms-harness evaluate-all` run that produced an
  `eval_results.yaml` (typically under `reports/phase2-run-<date>/`).
- A clone of `auto-research` next to this repo
  (`../auto-research`); see `docs/continuation-guide.md` Section 2 for
  the layout.

## Step 1 — Generate `human_feedback.md`

```bash
uv run python scripts/export_to_auto_research.py \
    --eval-results reports/phase2-run-2026-04-08/eval_results.yaml
```

By default the rendered Markdown lands next to the input YAML at
`reports/phase2-run-2026-04-08/human_feedback.md`. Pass `--output PATH`
to redirect it elsewhere — e.g. directly into the auto-research run
directory:

```bash
uv run python scripts/export_to_auto_research.py \
    --eval-results reports/phase2-run-2026-04-08/eval_results.yaml \
    --output ../auto-research/runs/pms-tool-eval/human_feedback.md
```

The script reads the YAML, validates it has the required keys
(`schema_version`, `modules`, `gaps`, `summary`), and writes a
human-readable Markdown file with one section per module plus a
top-level "Requested research" list of every module that did not have
a surviving candidate.

## Step 2 — Hand off to auto-research

Once `human_feedback.md` is in the auto-research run directory, the
auto-research operator should:

1. Inspect the file to confirm the requested gaps make sense (sometimes
   "no survivor" means the install command parser rejected the
   candidate's shape — e.g. `uv sync` — rather than a real research
   gap).
2. Append any free-form context to the bottom of the file (constraints,
   licensing concerns, deprecated tools to avoid, etc.). The Markdown
   shape is intentionally append-friendly.
3. Trigger the next auto-research round; the agent will read
   `human_feedback.md` as one of its inputs and target the requested
   modules in the new search.

## Schema reference

The YAML the script consumes is produced by
`pms.tool_harness.aggregate.write_eval_results_yaml` and has this shape
(see `python/pms/tool_harness/aggregate.py` for the canonical
definition):

```yaml
schema_version: 1
generated_at: "2026-04-08T07:56:05+00:00"
modules:
  - module: data_connector
    evaluated_count: 5
    survived_count: 3
    top_candidate: pmxt
    top_score: 0.85
    request_more_candidates: false
    search_hints: []
gaps:
  - realtime_feed
summary:
  total_modules: 10
  modules_with_winner: 8
  modules_with_gap: 2
```

The script tolerates missing optional keys (`top_candidate=null`,
`search_hints=[]`, etc.) but raises `ValueError` if `modules` is
missing entirely so a malformed input fails fast instead of silently
producing an empty Markdown file.

## When to bump `schema_version`

If a future release of the harness changes the per-module fields in
`eval_results.yaml` in a way that breaks downstream consumers
(renamed key, removed field, changed type), bump
`pms.tool_harness.aggregate.SCHEMA_VERSION`. The Markdown exporter
uses the schema string only for display, not for branching logic, so
older eval results stay convertible until the field shape itself
changes.
