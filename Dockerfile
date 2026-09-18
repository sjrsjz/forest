FROM ghcr.io/astral-sh/uv:0.7.15 AS uv

FROM python:3.11.13-slim AS build
COPY --from=uv /uv /uvx /bin/
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    UV_NATIVE_TLS=true
WORKDIR /app
COPY pyproject.toml uv.lock README.md ./
COPY src ./src
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-editable

FROM build AS test
COPY tests ./tests
COPY examples ./examples
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-editable
ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    HOME=/tmp
USER 10001:10001
CMD ["python", "-m", "pytest", "-q", "-p", "no:cacheprovider"]

FROM python:3.11.13-slim AS runtime
ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    HOME=/tmp \
    FOREST_DATA_DIR=/data
WORKDIR /app
COPY --from=build /app/.venv /app/.venv
USER 10001:10001
STOPSIGNAL SIGTERM
HEALTHCHECK --interval=10s --timeout=3s --start-period=15s --retries=3 \
    CMD ["python", "-c", "from pathlib import Path; import sys; sys.exit(0 if Path('/data/runtime-status.json').is_file() else 1)"]
CMD ["forest", "bot"]
