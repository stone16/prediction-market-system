"""Tests for ``scripts/export_to_auto_research.py`` (P2-06).

The script bridges ``eval_results.yaml`` (produced by
``pms-harness evaluate-all``) into the Markdown shape auto-research
consumes as feedback. These tests cover the conversion logic, CLI
defaults, and error paths — they intentionally avoid touching the real
``../auto-research`` repo.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from typing import Any

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "scripts" / "export_to_auto_research.py"


def _load_script_module() -> Any:
    """Import the ``export_to_auto_research`` script as a module.

    The script lives outside the ``python/pms`` package, so we load it
    via ``importlib.util`` rather than relying on ``sys.path``. The
    returned module is cached in ``sys.modules`` so each test reuses
    the same instance.
    """
    name = "export_to_auto_research_test_module"
    if name in sys.modules:
        return sys.modules[name]
    spec = importlib.util.spec_from_file_location(name, SCRIPT_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


# ---------------------------------------------------------------------------
# Sample fixtures
# ---------------------------------------------------------------------------


def _sample_results(with_gap: bool = True) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "generated_at": "2026-04-08T07:56:05+00:00",
        "modules": [
            {
                "module": "data_connector",
                "evaluated_count": 5,
                "survived_count": 3,
                "top_candidate": "pmxt",
                "top_score": 0.85,
                "request_more_candidates": False,
                "search_hints": [],
            },
            {
                "module": "realtime_feed",
                "evaluated_count": 1,
                "survived_count": 0 if with_gap else 1,
                "top_candidate": None if with_gap else "real-time-data-client",
                "top_score": 0.0 if with_gap else 0.7,
                "request_more_candidates": with_gap,
                "search_hints": (
                    [
                        "No candidate survived the realtime_feed survival "
                        "gate; research additional tools."
                    ]
                    if with_gap
                    else []
                ),
            },
        ],
        "gaps": ["realtime_feed"] if with_gap else [],
        "summary": {
            "total_modules": 2,
            "modules_with_winner": 1 if with_gap else 2,
            "modules_with_gap": 1 if with_gap else 0,
        },
    }


# ---------------------------------------------------------------------------
# render_human_feedback
# ---------------------------------------------------------------------------


def test_render_includes_top_level_metadata() -> None:
    mod = _load_script_module()
    text = mod.render_human_feedback(_sample_results())
    assert "# Tool Evaluation Feedback — pms" in text
    assert "2026-04-08T07:56:05+00:00" in text
    assert "pms-eval-results v1" in text
    assert "Modules evaluated" in text


def test_render_per_module_section_with_winner() -> None:
    mod = _load_script_module()
    text = mod.render_human_feedback(_sample_results())
    assert "### `data_connector`" in text
    assert "Top candidate**: `pmxt` (score 0.85)" in text
    assert "Evaluated**: 5 candidate(s)" in text
    assert "Survived**: 3" in text
    assert "✅" in text  # winner marker


def test_render_per_module_section_with_gap() -> None:
    mod = _load_script_module()
    text = mod.render_human_feedback(_sample_results())
    assert "### `realtime_feed`" in text
    assert "Top candidate**: _none_" in text
    assert "❌" in text  # gap marker
    assert "research additional tools" in text


def test_render_requested_research_lists_gaps() -> None:
    mod = _load_script_module()
    text = mod.render_human_feedback(_sample_results())
    assert "## Requested research" in text
    assert "`realtime_feed`" in text


def test_render_no_gaps_message() -> None:
    mod = _load_script_module()
    text = mod.render_human_feedback(_sample_results(with_gap=False))
    assert "every module has at least one surviving candidate" in text


def test_render_handles_empty_modules() -> None:
    mod = _load_script_module()
    text = mod.render_human_feedback(
        {
            "schema_version": 1,
            "generated_at": "2026-04-08T00:00:00Z",
            "modules": [],
            "gaps": [],
            "summary": {
                "total_modules": 0,
                "modules_with_winner": 0,
                "modules_with_gap": 0,
            },
        }
    )
    assert "_No modules evaluated._" in text


# ---------------------------------------------------------------------------
# load_eval_results
# ---------------------------------------------------------------------------


def test_load_eval_results_happy_path(tmp_path: Path) -> None:
    mod = _load_script_module()
    yaml_path = tmp_path / "eval_results.yaml"
    yaml_path.write_text(
        yaml.safe_dump(_sample_results()), encoding="utf-8"
    )
    results = mod.load_eval_results(yaml_path)
    assert results["schema_version"] == 1
    assert len(results["modules"]) == 2


def test_load_eval_results_missing_file_raises(tmp_path: Path) -> None:
    mod = _load_script_module()
    with pytest.raises(FileNotFoundError):
        mod.load_eval_results(tmp_path / "nope.yaml")


def test_load_eval_results_non_mapping_root_raises(tmp_path: Path) -> None:
    mod = _load_script_module()
    bad_path = tmp_path / "bad.yaml"
    bad_path.write_text("- this is a list\n", encoding="utf-8")
    with pytest.raises(ValueError, match="root must be a mapping"):
        mod.load_eval_results(bad_path)


def test_load_eval_results_missing_modules_key_raises(tmp_path: Path) -> None:
    mod = _load_script_module()
    bad_path = tmp_path / "bad.yaml"
    bad_path.write_text("schema_version: 1\n", encoding="utf-8")
    with pytest.raises(ValueError, match="missing required key 'modules'"):
        mod.load_eval_results(bad_path)


# ---------------------------------------------------------------------------
# CLI main()
# ---------------------------------------------------------------------------


def test_main_writes_default_output_next_to_yaml(tmp_path: Path) -> None:
    mod = _load_script_module()
    yaml_path = tmp_path / "eval_results.yaml"
    yaml_path.write_text(
        yaml.safe_dump(_sample_results()), encoding="utf-8"
    )

    rc = mod.main(["--eval-results", str(yaml_path)])
    assert rc == 0

    output_path = tmp_path / "human_feedback.md"
    assert output_path.exists()
    text = output_path.read_text(encoding="utf-8")
    assert "data_connector" in text
    assert "realtime_feed" in text


def test_main_honors_explicit_output_path(tmp_path: Path) -> None:
    mod = _load_script_module()
    yaml_path = tmp_path / "eval_results.yaml"
    yaml_path.write_text(
        yaml.safe_dump(_sample_results()), encoding="utf-8"
    )
    custom = tmp_path / "subdir" / "feedback.md"

    rc = mod.main(
        ["--eval-results", str(yaml_path), "--output", str(custom)]
    )
    assert rc == 0
    assert custom.exists()
    assert "data_connector" in custom.read_text(encoding="utf-8")


def test_main_returns_nonzero_for_missing_input(tmp_path: Path) -> None:
    mod = _load_script_module()
    rc = mod.main(["--eval-results", str(tmp_path / "nope.yaml")])
    assert rc == 2
