from __future__ import annotations

import platform
import sys
import types


# Pytest 9 imports `readline` during startup to work around libedit capture
# issues on macOS. In this uv-managed environment, importing the extension
# segfaults before pytest can even parse CLI arguments. A lightweight stub is
# sufficient for pytest's startup path because it only requires the import to
# succeed; the test suite does not use readline features.
if platform.system() == "Darwin":
    sys.modules.setdefault("readline", types.ModuleType("readline"))
