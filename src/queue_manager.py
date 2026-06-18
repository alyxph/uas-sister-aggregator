"""
queue_manager.py — Redis-backed message broker and idempotent consumer.

Architecture:
    Publisher → Redis List (LPUSH) → Consumer Workers (BRPOP) → DedupStore → Processed events

The consumer loop runs as asyncio tasks. Each worker uses BRPOP to
block-wait for new events from the Redis list. This provides:
- Durable message buffering (Redis persistence via AOF/RDB)
- Fair distribution across multiple consumer workers
- Atomic dequeue (BRPOP removes and returns atomically)

Deduplication is delegated entirely to DedupStore.mark_processed(), which
provides atomic INSERT ... ON CONFLICT DO NOTHING semantics at the
PostgreSQL level.

At-least-once delivery is achieved because:
1. Publisher pushes event to Redis → acknowledged
2. Consumer BRPOP dequeues → processes → mark_processed
3. If consumer crashes after BRPOP but before mark_processed, the event
   is lost from Redis but the publisher's retry mechanism will resend it.
"""

from __future__ import annotations

import asyncio
import json
import logging

import redis.asyncio as aioredis

from .dedup_store import DedupStore
from .models import Event

logger = logging.getLogger(__name__)

REDIS_QUEUE_KEY = "logqueue:events"


class QueueManager:
    """
    Manages the Redis-backed message queue and consumer workers.

    Events are serialised as JSON and pushed to a Redis list.
    Consumer workers use BRPOP to dequeue and process events.
    """

    def __init__(
        self,
        dedup_store: DedupStore,
        broker_url: str = "redis://broker:6379/0",
        n_workers: int = 2,
    ) -> None:
        self._redis = aioredis.from_url(broker_url, decode_responses=True)
        self._dedup = dedup_store
        self._n_workers = n_workers
        self._tasks: list[asyncio.Task] = []

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Spawn consumer worker coroutines. Must be called inside an event loop."""
        for i in range(self._n_workers):
            task = asyncio.create_task(self._consume(worker_id=i))
            self._tasks.append(task)
            logger.info("Consumer worker #%d started", i)

    def stop(self) -> None:
        """Cancel all consumer tasks gracefully."""
        for task in self._tasks:
            task.cancel()
        logger.info("All consumer workers stopped")

    async def close(self) -> None:
        """Close the Redis connection (aclose() required by redis-py >= 5.0.1)."""
        await self._redis.aclose()

    # ------------------------------------------------------------------
    # Publisher interface
    # ------------------------------------------------------------------

    async def enqueue(self, events: list[Event]) -> int:
        """
        Serialize events to JSON and push them onto the Redis list.
        Returns the number of events enqueued.
        """
        self._dedup.increment_stat("received", len(events))
        pipe = self._redis.pipeline()
        for event in events:
            pipe.lpush(REDIS_QUEUE_KEY, event.model_dump_json())
        await pipe.execute()
        return len(events)

    async def drain(self, timeout: float = 5.0) -> None:
        """Wait until the Redis queue is empty or timeout elapses."""
        import time
        deadline = time.time() + timeout
        while time.time() < deadline:
            qsize = await self._redis.llen(REDIS_QUEUE_KEY)
            if qsize == 0:
                return
            await asyncio.sleep(0.1)
        remaining = await self._redis.llen(REDIS_QUEUE_KEY)
        if remaining > 0:
            logger.warning("drain() timed out; %d items still in queue", remaining)

    # ------------------------------------------------------------------
    # Consumer
    # ------------------------------------------------------------------

    async def _consume(self, worker_id: int) -> None:
        """
        Consumer loop using BRPOP for blocking dequeue from Redis.

        BRPOP is atomic: it removes and returns the rightmost element.
        This ensures no two workers process the same queue item.
        """
        logger.info("Worker #%d ready", worker_id)
        while True:
            try:
                result = await self._redis.brpop(REDIS_QUEUE_KEY, timeout=1)
                if result is None:
                    continue

                _key, raw = result
                event_data = json.loads(raw)
                event = Event(**event_data)

                is_new = self._dedup.mark_processed(event)
                if is_new:
                    logger.info(
                        "[W%d] PROCESSED  topic=%-20s event_id=%s",
                        worker_id,
                        event.topic,
                        event.event_id,
                    )
                else:
                    self._dedup.increment_stat("duplicate_dropped", 1)
                    logger.warning(
                        "[W%d] DUPLICATE  topic=%-20s event_id=%s — DROPPED",
                        worker_id,
                        event.topic,
                        event.event_id,
                    )
            except asyncio.CancelledError:
                logger.info("Worker #%d cancelled", worker_id)
                return
            except Exception as exc:  # noqa: BLE001
                logger.error("[W%d] Error processing event: %s", worker_id, exc)
                await asyncio.sleep(0.5)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @property
    async def queue_size(self) -> int:
        """Return the current length of the Redis queue."""
        return await self._redis.llen(REDIS_QUEUE_KEY)

    async def get_queue_size(self) -> int:
        """Async method to get queue size (for use in endpoints)."""
        return await self._redis.llen(REDIS_QUEUE_KEY)
