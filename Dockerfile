# Multi-stage / Cached Python 3.12 Dockerfile
FROM python:3.12-slim AS builder

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt


# Final Runtime Stage
FROM python:3.12-slim AS runner

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

# Copy installed packages from builder
COPY --from=builder /install /usr/local

# Create non-root app user and ensure writable /data directory
RUN groupadd -g 1000 appgroup && \
    useradd -u 1000 -g appgroup -s /bin/bash -m appuser && \
    mkdir -p /data && \
    chown -R appuser:appgroup /data /app

# Copy application source code, contracts, and lab fixtures
COPY --chown=appuser:appgroup contracts /app/contracts
COPY --chown=appuser:appgroup fixtures /app/fixtures
COPY --chown=appuser:appgroup src /app/src

USER appuser

# Default command (overridden in compose.yaml per service)
CMD ["python", "--version"]
