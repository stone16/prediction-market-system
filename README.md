# Prediction Market System (pms)

Modular prediction market trading system with three phases:

1. **Tool Evaluation Harness** — systematically evaluate open-source prediction market tools against benchmarks.
2. **Trading Pipeline Skeleton** — sense → strategy → risk → execute → evaluate → feedback, with pluggable Protocol-based modules.
3. **Correlation Engine** — embedding-based cross-market correlation and arbitrage detection.

Target platforms: Polymarket and Kalshi.

## Layout

```
python/pms/         # Single Python package
  models/           # Frozen dataclasses (core domain types)
  protocols/        # Protocol interfaces (structural typing)
rust/               # Rust workspace stub (reserved for future perf paths)
tests/              # Pytest test suite
```

## Development

```bash
uv sync
uv run pytest
uv run mypy python/ --strict
```
