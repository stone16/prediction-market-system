from __future__ import annotations

from pathlib import Path
import shutil
import subprocess
import sys

import pytest


REPO_ROOT = Path(__file__).resolve().parents[2]
SENSOR_AGGREGATE_CONTRACT = "Sensor strategy-agnostic: no aggregate import"
ACTUATOR_AGGREGATE_CONTRACT = "Actuator strategy-agnostic: no aggregate import"
SENSOR_ACTUATOR_CONTROLLER_CONTRACT = "Sensor + Actuator: no controller import"
SENSOR_MARKET_SELECTION_CONTRACT = "Sensor: no market_selection import"
ACTUATOR_MARKET_SELECTION_CONTRACT = "Actuator: no market_selection import"
MARKET_SELECTION_AGGREGATE_CONTRACT = "Market selection: no aggregate import"

IMPORT_LINTER_CONFIG = """
[project]
name = "scratch-import-linter"
version = "0.0.1"
requires-python = ">=3.11"

[tool.importlinter]
root_packages = ["pms"]

[[tool.importlinter.contracts]]
name = "Sensor strategy-agnostic: no aggregate import"
type = "forbidden"
source_modules = ["pms.sensor"]
forbidden_modules = ["pms.strategies.aggregate"]

[[tool.importlinter.contracts]]
name = "Actuator strategy-agnostic: no aggregate import"
type = "forbidden"
source_modules = ["pms.actuator"]
forbidden_modules = ["pms.strategies.aggregate"]

[[tool.importlinter.contracts]]
name = "Sensor + Actuator: no controller import"
type = "forbidden"
source_modules = ["pms.sensor", "pms.actuator"]
forbidden_modules = ["pms.controller"]

[[tool.importlinter.contracts]]
name = "Sensor: no market_selection import"
type = "forbidden"
source_modules = ["pms.sensor"]
forbidden_modules = ["pms.market_selection"]

[[tool.importlinter.contracts]]
name = "Actuator: no market_selection import"
type = "forbidden"
source_modules = ["pms.actuator"]
forbidden_modules = ["pms.market_selection"]

[[tool.importlinter.contracts]]
name = "Market selection: no aggregate import"
type = "forbidden"
source_modules = ["pms.market_selection"]
forbidden_modules = ["pms.strategies.aggregate"]
""".strip()


def _lint_imports_command() -> list[str]:
    executable = REPO_ROOT / ".venv" / "bin" / "lint-imports"
    if executable.exists():
        return [str(executable)]

    executable = Path(sys.executable).resolve().with_name("lint-imports")
    if executable.exists():
        return [str(executable)]

    resolved = shutil.which("lint-imports")
    if resolved is None:
        pytest.fail("lint-imports executable is not available in the active environment")
    return [resolved]


@pytest.mark.parametrize(
    ("violating_module", "source_text", "expected_contract"),
    [
        (
            "pms/sensor/bad.py",
            "from pms.strategies.aggregate import Strategy\n",
            SENSOR_AGGREGATE_CONTRACT,
        ),
        (
            "pms/sensor/bad.py",
            "from pms.market_selection.selector import MarketSelector\n",
            SENSOR_MARKET_SELECTION_CONTRACT,
        ),
        (
            "pms/actuator/bad.py",
            "from pms.market_selection.selector import MarketSelector\n",
            ACTUATOR_MARKET_SELECTION_CONTRACT,
        ),
        (
            "pms/market_selection/bad.py",
            "from pms.strategies.aggregate import Strategy\n",
            MARKET_SELECTION_AGGREGATE_CONTRACT,
        ),
    ],
)
def test_import_linter_rejects_forbidden_dependencies(
    tmp_path: Path,
    violating_module: str,
    source_text: str,
    expected_contract: str,
) -> None:
    (tmp_path / "pyproject.toml").write_text(f"{IMPORT_LINTER_CONFIG}\n")
    for package in [
        "pms",
        "pms/sensor",
        "pms/actuator",
        "pms/controller",
        "pms/strategies",
        "pms/market_selection",
    ]:
        package_dir = tmp_path / package
        package_dir.mkdir(parents=True, exist_ok=True)
        (package_dir / "__init__.py").write_text("")
    (tmp_path / "pms/strategies/aggregate.py").write_text("class Strategy: ...\n")
    (tmp_path / "pms/market_selection/selector.py").write_text(
        "class MarketSelector: ...\n"
    )
    (tmp_path / violating_module).write_text(source_text)

    result = subprocess.run(
        [
            *_lint_imports_command(),
            "--config",
            str(tmp_path / "pyproject.toml"),
        ],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode != 0
    assert expected_contract in result.stdout


def test_current_tree_lints_clean() -> None:
    result = subprocess.run(
        _lint_imports_command(),
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    for contract in [
        SENSOR_AGGREGATE_CONTRACT,
        ACTUATOR_AGGREGATE_CONTRACT,
        SENSOR_ACTUATOR_CONTROLLER_CONTRACT,
        SENSOR_MARKET_SELECTION_CONTRACT,
        ACTUATOR_MARKET_SELECTION_CONTRACT,
        MARKET_SELECTION_AGGREGATE_CONTRACT,
    ]:
        assert contract in result.stdout
