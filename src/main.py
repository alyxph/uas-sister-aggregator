"""
main.py — FastAPI entry point for Pub-Sub Log Aggregator.

Endpoints:
    POST /publish          — Publish single or batch events
    GET  /events?topic=... — List unique processed events (optional topic filter)
    GET  /stats            — Aggregation counters + uptime
    GET  /health           — Liveness probe

Design:
    - lifespan context manager starts/stops the async consumer workers.
    - DedupStore (PostgreSQL) and QueueManager (Redis) are singletons
      instantiated at module level so they survive across requests.
    - DATABASE_URL and BROKER_URL are configurable via environment variables.
    - Startup includes retry logic to wait for PostgreSQL and Redis readiness.
"""

from __future__ import annotations

import logging
import os
import time
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse

from .dedup_store import DedupStore
from .models import Event
from .queue_manager import QueueManager

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

_DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://user:pass@localhost:5432/logdb")
_BROKER_URL = os.environ.get("BROKER_URL", "redis://localhost:6379/0")
_START_TIME = time.time()

# ---------------------------------------------------------------------------
# Singletons (initialised with retry in _init_services)
# ---------------------------------------------------------------------------

dedup_store: Optional[DedupStore] = None
queue_manager: Optional[QueueManager] = None


def _init_services(max_retries: int = 30, delay: float = 2.0) -> None:
    """
    Initialise DedupStore and QueueManager with retry logic.

    PostgreSQL and Redis may not be immediately ready when the aggregator
    container starts (even with depends_on). This retry loop implements
    exponential backoff to handle startup ordering gracefully.
    """
    global dedup_store, queue_manager

    for attempt in range(1, max_retries + 1):
        try:
            logger.info("Connecting to PostgreSQL (attempt %d/%d)...", attempt, max_retries)
            dedup_store = DedupStore(database_url=_DATABASE_URL)
            logger.info("PostgreSQL connected successfully.")
            break
        except Exception as exc:
            logger.warning("PostgreSQL not ready: %s. Retrying in %.1fs...", exc, delay)
            if attempt == max_retries:
                logger.error("Could not connect to PostgreSQL after %d attempts. Exiting.", max_retries)
                raise
            time.sleep(delay)

    queue_manager = QueueManager(
        dedup_store=dedup_store,
        broker_url=_BROKER_URL,
        n_workers=2,
    )


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):  # noqa: ANN001
    _init_services()
    queue_manager.start()
    logger.info("Aggregator service started. DB: %s | Broker: %s", _DATABASE_URL.split("@")[-1], _BROKER_URL)
    yield
    queue_manager.stop()
    await queue_manager.close()
    dedup_store.close()
    logger.info("Aggregator service stopped.")


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

app = FastAPI(
    title="Pub-Sub Log Aggregator",
    description="Idempotent consumer with persistent deduplication, Redis broker, and PostgreSQL storage (UAS Sistem Terdistribusi)",
    version="2.0.0",
    lifespan=lifespan,
)


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.post("/publish", status_code=202)
async def publish(body: dict):
    """
    Accept a single event or a batch.

    Single event body:
        { "topic": "...", "event_id": "...", "timestamp": "...", "source": "...", "payload": {...} }

    Batch body:
        { "events": [ <event>, <event>, ... ] }
    """
    try:
        if "events" in body:
            # Batch path
            raw_events: list[dict] = body["events"]
            events = [Event(**e) for e in raw_events]
        else:
            # Single-event path
            events = [Event(**body)]
    except Exception as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    queued = await queue_manager.enqueue(events)
    return {"queued": queued, "message": "Events accepted and queued for processing"}


@app.get("/events")
async def get_events(topic: Optional[str] = Query(default=None, description="Filter by topic")):
    """
    Return all unique processed events.
    Optionally filter by ?topic=<name>.
    """
    # Give the async consumer a moment to flush the queue
    await queue_manager.drain(timeout=2.0)
    events = dedup_store.get_events(topic=topic)
    return {"events": events, "count": len(events), "topic_filter": topic}


@app.get("/stats")
async def get_stats():
    """
    Return aggregation counters and system metadata.

    Fields:
        received          — total events received (persisted in PostgreSQL)
        unique_processed  — events committed to dedup store (authoritative from DB)
        duplicate_dropped — duplicates caught (persisted in PostgreSQL)
        topics            — list of distinct topics in dedup store
        queue_size        — events currently waiting in Redis queue
        uptime_seconds    — seconds since service start
    """
    return {
        "received": dedup_store.get_stat("received"),
        "unique_processed": dedup_store.count_unique(),   # DB-authoritative
        "duplicate_dropped": dedup_store.get_stat("duplicate_dropped"),
        "topics": dedup_store.get_topics(),
        "queue_size": await queue_manager.get_queue_size(),
        "uptime_seconds": round(time.time() - _START_TIME, 2),
    }


@app.get("/health")
async def health():
    """Liveness probe for Docker health-check."""
    return {"status": "ok", "uptime_seconds": round(time.time() - _START_TIME, 2)}


# ---------------------------------------------------------------------------
# Dev entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn

    uvicorn.run("src.main:app", host="0.0.0.0", port=8080, reload=False)
