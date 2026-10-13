# Multi-stage Dockerfile for async-agent-dispatcher
FROM python:3.11-slim AS builder
WORKDIR /build

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml README.md ./
COPY src/ ./src/

RUN pip install --no-cache-dir --prefix=/install ".[dev]"

FROM python:3.11-slim AS runtime
WORKDIR /app

RUN apt-get update && apt-get upgrade -y && rm -rf /var/lib/apt/lists/*

COPY --from=builder /install /usr/local

RUN python -m pip uninstall -y pip setuptools wheel 2>/dev/null || true && \
    rm -rf /usr/local/lib/python3.11/ensurepip

COPY tests/ ./tests/
COPY pyproject.toml ./

RUN useradd -m appuser && chown -R appuser:appuser /app
USER appuser

ENTRYPOINT ["pytest"]
CMD ["tests/", "-v", "--tb=short"]
