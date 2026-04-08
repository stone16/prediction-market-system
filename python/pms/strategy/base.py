"""Optional convenience base class for ``StrategyProtocol`` implementations.

Strategies only need to satisfy the Protocol structurally — they do NOT need
to subclass ``StrategyBase``. This class exists purely as a small anchor for
a shared ``name`` field and to document the expected shape for future
strategies (e.g., CP09+).
"""

from __future__ import annotations


class StrategyBase:
    """Minimal shared state for built-in strategies.

    The only invariant is that concrete strategies expose a ``name`` string,
    which the feedback engine uses to route ``StrategyFeedback`` packets.
    """

    name: str = "base"
