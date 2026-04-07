"""YAML loaders for benchmark and candidate config files.

Both loaders return frozen schema objects on success and raise
:class:`BenchmarkValidationError` on any schema violation. Validation is
intentionally hand-rolled (rather than pulling in pydantic) so that error
messages can pinpoint the exact YAML field path and expected type — which
the spec requires for the CP02 acceptance criteria.

Design notes:

* `bool` is checked **before** `int` because Python treats `bool` as a
  subclass of `int`; otherwise ``true`` would silently satisfy
  ``timeout_seconds: int``.
* Category weights must sum to 1.0 (within 1e-6) — this is the contract
  documented in spec.md and exercised by the unit tests.
* `lower_is_better` defaults to ``False`` per spec.
* `config` on a candidate defaults to an empty dict so simple test
  candidates don't need an explicit ``config: {}`` block.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from .schema import (
    Benchmark,
    BenchmarkValidationError,
    Candidate,
    FunctionalCategory,
    FunctionalTest,
    SurvivalGateItem,
)


# ---------------------------------------------------------------------------
# Internal type-checking helpers
# ---------------------------------------------------------------------------


def _require(mapping: dict[str, Any], key: str, expected_py: type, path: str) -> Any:
    """Return ``mapping[key]`` after asserting its type, else raise.

    `expected_py` may be ``str``, ``int``, ``float``, ``bool``, ``list``,
    or ``dict``. ``int`` excludes ``bool`` since ``isinstance(True, int)``
    is True in Python.
    """
    if key not in mapping:
        raise BenchmarkValidationError(f"{path}.{key}" if path else key, expected_py.__name__)
    value = mapping[key]
    return _check_type(value, expected_py, f"{path}.{key}" if path else key)


def _check_type(value: Any, expected_py: type, field_path: str) -> Any:
    if expected_py is int:
        # Reject bool — `True`/`False` are ints in Python but not what we want.
        if isinstance(value, bool) or not isinstance(value, int):
            raise BenchmarkValidationError(field_path, "int")
        return value
    if expected_py is float:
        if isinstance(value, bool):
            raise BenchmarkValidationError(field_path, "float")
        if not isinstance(value, (int, float)):
            raise BenchmarkValidationError(field_path, "float")
        return float(value)
    if expected_py is bool:
        if not isinstance(value, bool):
            raise BenchmarkValidationError(field_path, "bool")
        return value
    if expected_py is str:
        if not isinstance(value, str):
            raise BenchmarkValidationError(field_path, "str")
        return value
    if expected_py is list:
        if not isinstance(value, list):
            raise BenchmarkValidationError(field_path, "list")
        return value
    if expected_py is dict:
        if not isinstance(value, dict):
            raise BenchmarkValidationError(field_path, "dict")
        return value
    # Defensive — should never happen for our small set of types.
    raise BenchmarkValidationError(field_path, expected_py.__name__)  # pragma: no cover


def _require_mapping_root(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise BenchmarkValidationError("<root>", "mapping")
    return raw


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def load_benchmark(path: str | Path) -> Benchmark:
    """Parse and validate a benchmark YAML file.

    Raises :class:`BenchmarkValidationError` on any schema violation, with
    `field_path` and `expected` populated for the offending field.
    """
    raw = _read_yaml(path)
    data = _require_mapping_root(raw)

    module = _require(data, "module", str, "")
    version = _require(data, "version", int, "")

    survival_raw = _require(data, "survival_gate", list, "")
    survival_gate: list[SurvivalGateItem] = []
    for idx, item in enumerate(survival_raw):
        item_path = f"survival_gate[{idx}]"
        if not isinstance(item, dict):
            raise BenchmarkValidationError(item_path, "mapping")
        survival_gate.append(
            SurvivalGateItem(
                id=_require(item, "id", str, item_path),
                test=_require(item, "test", str, item_path),
                timeout_seconds=_require(item, "timeout_seconds", int, item_path),
            )
        )

    functional_raw = _require(data, "functional_tests", dict, "")
    functional_tests: list[FunctionalCategory] = []
    for cat_name, cat_body in functional_raw.items():
        cat_path = f"functional_tests.{cat_name}"
        if not isinstance(cat_body, dict):
            raise BenchmarkValidationError(cat_path, "mapping")
        weight = _require(cat_body, "weight", float, cat_path)
        tests_raw = _require(cat_body, "tests", list, cat_path)
        tests: list[FunctionalTest] = []
        for t_idx, t in enumerate(tests_raw):
            t_path = f"{cat_path}.tests[{t_idx}]"
            if not isinstance(t, dict):
                raise BenchmarkValidationError(t_path, "mapping")
            baseline_value = t.get("baseline", None)
            baseline: float | None
            if baseline_value is None:
                baseline = None
            else:
                if isinstance(baseline_value, bool) or not isinstance(
                    baseline_value, (int, float)
                ):
                    raise BenchmarkValidationError(f"{t_path}.baseline", "float|null")
                baseline = float(baseline_value)
            lower_is_better_raw = t.get("lower_is_better", False)
            if not isinstance(lower_is_better_raw, bool):
                raise BenchmarkValidationError(f"{t_path}.lower_is_better", "bool")
            tests.append(
                FunctionalTest(
                    id=_require(t, "id", str, t_path),
                    test=_require(t, "test", str, t_path),
                    metric=_require(t, "metric", str, t_path),
                    baseline=baseline,
                    lower_is_better=lower_is_better_raw,
                )
            )
        functional_tests.append(
            FunctionalCategory(name=cat_name, weight=weight, tests=tests)
        )

    if functional_tests:
        total = sum(c.weight for c in functional_tests)
        if abs(total - 1.0) > 1e-6:
            raise BenchmarkValidationError(
                "functional_tests",
                f"weights to sum to 1.0 (got {total:.4f})",
            )

    return Benchmark(
        module=module,
        version=version,
        survival_gate=survival_gate,
        functional_tests=functional_tests,
    )


def load_candidate(path: str | Path) -> Candidate:
    """Parse and validate a candidate YAML file.

    Raises :class:`BenchmarkValidationError` on any schema violation.
    """
    raw = _read_yaml(path)
    data = _require_mapping_root(raw)

    name = _require(data, "name", str, "")
    repo = _require(data, "repo", str, "")
    language = _require(data, "language", str, "")
    install = _require(data, "install", str, "")
    platforms_raw = _require(data, "platforms", list, "")
    for i, p in enumerate(platforms_raw):
        if not isinstance(p, str):
            raise BenchmarkValidationError(f"platforms[{i}]", "str")
    module = _require(data, "module", str, "")
    notes = _require(data, "notes", str, "")
    config_raw = data.get("config", {})
    if not isinstance(config_raw, dict):
        raise BenchmarkValidationError("config", "dict")

    return Candidate(
        name=name,
        repo=repo,
        language=language,
        install=install,
        platforms=list(platforms_raw),
        module=module,
        notes=notes,
        config=dict(config_raw),
    )


# ---------------------------------------------------------------------------
# Internals
# ---------------------------------------------------------------------------


def _read_yaml(path: str | Path) -> Any:
    with open(path, "r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)
