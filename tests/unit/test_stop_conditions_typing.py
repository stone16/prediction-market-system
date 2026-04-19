from __future__ import annotations

import ast
from pathlib import Path
import tokenize


FORBIDDEN_PREFIXES = (
    "strategy:",
    "model_id:",
    "routing:",
    "model:",
    "version:",
)


def test_stop_conditions_strings_remain_gate_threshold_only() -> None:
    violations: list[str] = []
    for path in Path("src/pms").rglob("*.py"):
        with path.open("rb") as stream:
            for token in tokenize.tokenize(stream.readline):
                if token.type != tokenize.STRING:
                    continue
                try:
                    value = ast.literal_eval(token.string)
                except (SyntaxError, ValueError):
                    continue
                if not isinstance(value, str):
                    continue
                if any(prefix in value for prefix in FORBIDDEN_PREFIXES):
                    violations.append(f"{path}:{token.start[0]}:{value}")

    assert violations == []
