from __future__ import annotations

import os
import platform
import sys
import types
from pathlib import Path


# Pytest 9 imports `readline` during startup to work around libedit capture
# issues on macOS. In this uv-managed environment, importing the extension
# segfaults before pytest can even parse CLI arguments. A lightweight stub is
# sufficient for pytest's startup path because it only requires the import to
# succeed; the test suite does not use readline features.
def _is_pytest_process() -> bool:
    if os.environ.get("PYTEST_CURRENT_TEST"):
        return True
    if not sys.argv:
        return False
    command = Path(sys.argv[0]).name
    return command in {"pytest", "py.test"} or sys.argv[:2] == ["-m", "pytest"]


if platform.system() == "Darwin" and _is_pytest_process():
    readline_stub = types.ModuleType("readline")
    setattr(readline_stub, "set_completer", lambda completer=None: None)
    setattr(readline_stub, "get_completer", lambda: None)
    setattr(readline_stub, "parse_and_bind", lambda spec: None)
    sys.modules.setdefault("readline", readline_stub)
