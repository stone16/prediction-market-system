from __future__ import annotations

import ast
from pathlib import Path
import re


REPO_ROOT = Path(__file__).resolve().parents[2]
TARGET_PATHS = [
    REPO_ROOT / "src/pms/evaluation",
    REPO_ROOT / "src/pms/storage/eval_store.py",
    REPO_ROOT / "src/pms/storage/feedback_store.py",
]
AGGREGATE_PATTERN = re.compile(r"\b(count|avg|sum|min|max)\s*\(", re.IGNORECASE)
INNER_RING_PATTERN = re.compile(r"\b(eval_records|fills)\b", re.IGNORECASE)
GROUP_BY_PATTERN = re.compile(
    r"GROUP\s+BY\s+strategy_id\s*,\s*strategy_version_id",
    re.IGNORECASE,
)


def _iter_python_files() -> list[Path]:
    files: list[Path] = []
    for path in TARGET_PATHS:
        if path.is_dir():
            files.extend(sorted(path.rglob("*.py")))
        else:
            files.append(path)
    return files


def _nearest_prior_non_empty_line(lines: list[str], lineno: int) -> str:
    for index in range(lineno - 2, -1, -1):
        candidate = lines[index].strip()
        if candidate:
            return candidate
    return ""


def test_sql_aggregates_over_eval_and_fill_rows_are_strategy_grouped_or_ops_view() -> None:
    violations: list[str] = []

    for path in _iter_python_files():
        source = path.read_text()
        tree = ast.parse(source, filename=str(path))
        lines = source.splitlines()

        for node in ast.walk(tree):
            if not isinstance(node, ast.Constant) or not isinstance(node.value, str):
                continue
            literal = node.value
            if not AGGREGATE_PATTERN.search(literal) or not INNER_RING_PATTERN.search(literal):
                continue
            if GROUP_BY_PATTERN.search(literal):
                continue
            if _nearest_prior_non_empty_line(lines, node.lineno) == "# ops-view:":
                continue
            violations.append(f"{path.relative_to(REPO_ROOT)}:{node.lineno}")

    assert violations == []
