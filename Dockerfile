# ============================================================
# Pub-Sub Log Aggregator — Dockerfile
# Base: python:3.11-slim (non-root user, dependency caching)
# ============================================================

FROM python:3.11-slim

# ---------- OS-level hardening ----------
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# ---------- Non-root user ----------
RUN adduser --disabled-password --gecos '' appuser

WORKDIR /app
RUN mkdir -p /app/data && chown -R appuser:appuser /app

# ---------- Python deps (cached layer) ----------
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# ---------- Application code ----------
COPY --chown=appuser:appuser src/ ./src/

USER appuser

# ---------- Runtime ----------
EXPOSE 8080

HEALTHCHECK --interval=15s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

CMD ["python", "-m", "uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8080"]
