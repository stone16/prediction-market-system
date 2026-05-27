FROM python:3.13-slim-bookworm AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && curl -LsSf https://astral.sh/uv/install.sh | sh

ENV PATH="/root/.local/bin:${PATH}"

COPY pyproject.toml uv.lock README.md ./
COPY alembic.ini ./alembic.ini
COPY alembic ./alembic
COPY src ./src
COPY scripts ./scripts
COPY config.live-soak.yaml ./config.live-soak.yaml

RUN uv sync --frozen --no-dev --extra live --extra llm

EXPOSE 8000

CMD ["uv", "run", "--no-sync", "--no-dev", "pms-api", "--port", "8000"]
