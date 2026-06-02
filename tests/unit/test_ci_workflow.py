from __future__ import annotations

from pathlib import Path
from typing import Any, cast

import yaml


WORKFLOW_PATH = Path(".github/workflows/ci.yml")


def test_ci_test_job_has_fail_fast_timeouts() -> None:
    workflow = yaml.safe_load(WORKFLOW_PATH.read_text(encoding="utf-8"))
    jobs = cast(dict[str, Any], workflow["jobs"])
    test_job = cast(dict[str, Any], jobs["test"])
    steps = cast(list[dict[str, Any]], test_job["steps"])
    steps_by_name = {str(step["name"]): step for step in steps}

    assert test_job["timeout-minutes"] <= 20
    assert steps_by_name["Run PostgreSQL integration tests"]["timeout-minutes"] <= 10
