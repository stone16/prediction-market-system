from __future__ import annotations

from pathlib import Path
import subprocess


REPO_ROOT = Path(__file__).resolve().parents[2]
SENSOR_AGGREGATE_CONTRACT = "Sensor strategy-agnostic: no aggregate import"

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
""".strip()


def test_import_linter_rejects_sensor_aggregate_violation(tmp_path: Path) -> None:
    (tmp_path / "pyproject.toml").write_text(f"{IMPORT_LINTER_CONFIG}\n")
    for package in [
        "pms",
        "pms/sensor",
        "pms/actuator",
        "pms/controller",
        "pms/strategies",
    ]:
        package_dir = tmp_path / package
        package_dir.mkdir(parents=True, exist_ok=True)
        (package_dir / "__init__.py").write_text("")
    (tmp_path / "pms/strategies/aggregate.py").write_text("class Strategy: ...\n")
    (tmp_path / "pms/sensor/bad.py").write_text(
        "from pms.strategies.aggregate import Strategy\n"
    )

    result = subprocess.run(
        [
            "uv",
            "run",
            "--project",
            str(REPO_ROOT),
            "lint-imports",
            "--config",
            str(tmp_path / "pyproject.toml"),
        ],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode != 0
    assert SENSOR_AGGREGATE_CONTRACT in result.stdout


def test_current_tree_lints_clean() -> None:
    result = subprocess.run(
        ["uv", "run", "lint-imports"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stdout + result.stderr
