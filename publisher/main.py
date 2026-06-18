
from __future__ import annotations

import asyncio
import logging
import os
import random
import time
import uuid
from datetime import datetime, timezone

import httpx

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

TARGET_URL: str = os.environ.get("TARGET_URL", "http://localhost:8080/publish")
AGGREGATOR_BASE: str = TARGET_URL.rsplit("/publish", 1)[0] if "/publish" in TARGET_URL else TARGET_URL
TOTAL_EVENTS: int = int(os.environ.get("TOTAL_EVENTS", "6000"))
DUPLICATE_RATE: float = float(os.environ.get("DUPLICATE_RATE", "0.35"))
BATCH_SIZE: int = int(os.environ.get("BATCH_SIZE", "100"))
TOPIC_COUNT: int = int(os.environ.get("TOPIC_COUNT", "5"))
MAX_RETRIES: int = int(os.environ.get("MAX_RETRIES", "5"))


def make_event(topic: str, event_id: str) -> dict:
    return {
        "topic": topic,
        "event_id": event_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": "publisher-sim",
        "payload": {"value": random.randint(1, 9999), "seq": event_id},
    }


async def send_batch_with_retry(
    client: httpx.AsyncClient,
    batch: list[dict],
    batch_num: int,
) -> tuple[int, int]:
    """
    Send a batch with exponential backoff retry.

    Returns (sent_count, error_count).
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = await client.post("/publish", json={"events": batch})
            resp.raise_for_status()
            return len(batch), 0
        except Exception as exc:
            delay = min(30, 0.5 * (2 ** attempt)) + random.uniform(0, 0.5)
            logger.warning(
                "Batch %d attempt %d/%d failed: %s. Retrying in %.1fs...",
                batch_num, attempt, MAX_RETRIES, exc, delay,
            )
            if attempt == MAX_RETRIES:
                logger.error("Batch %d failed after %d attempts. Dropping.", batch_num, MAX_RETRIES)
                return 0, 1
            await asyncio.sleep(delay)
    return 0, 1


async def run() -> None:
    topics = [f"topic.{chr(65 + i)}" for i in range(TOPIC_COUNT)]
    unique_events = [
        make_event(random.choice(topics), str(uuid.uuid4())) for _ in range(TOTAL_EVENTS)
    ]

    # Build send list: unique + synthetic duplicates (>= 30% duplication)
    send_list = list(unique_events)
    n_dups = int(TOTAL_EVENTS * DUPLICATE_RATE)
    dup_events = [dict(e, timestamp=datetime.now(timezone.utc).isoformat())
                  for e in random.choices(unique_events, k=n_dups)]
    send_list.extend(dup_events)
    random.shuffle(send_list)

    total_to_send = len(send_list)
    logger.info(
        "Publisher: %d unique + %d duplicates (%.0f%%) = %d total events across %d topics",
        TOTAL_EVENTS,
        n_dups,
        DUPLICATE_RATE * 100,
        total_to_send,
        TOPIC_COUNT,
    )

    sent = 0
    errors = 0
    t0 = time.perf_counter()

    async with httpx.AsyncClient(base_url=AGGREGATOR_BASE, timeout=30) as client:
        # Wait for aggregator readiness with backoff
        for attempt in range(30):
            try:
                r = await client.get("/health")
                if r.status_code == 200:
                    logger.info("Aggregator is ready.")
                    break
            except Exception:
                pass
            delay = min(10, 1 * (1.5 ** min(attempt, 10)))
            logger.info("Waiting for aggregator... (attempt %d, next retry in %.1fs)", attempt + 1, delay)
            await asyncio.sleep(delay)
        else:
            logger.error("Aggregator not reachable at %s — aborting.", AGGREGATOR_BASE)
            return

        for i in range(0, total_to_send, BATCH_SIZE):
            batch = send_list[i : i + BATCH_SIZE]
            batch_num = i // BATCH_SIZE
            s, e = await send_batch_with_retry(client, batch, batch_num)
            sent += s
            errors += e

    elapsed = time.perf_counter() - t0
    rate = sent / elapsed if elapsed > 0 else 0
    logger.info(
        "Done. Sent %d events in %.2fs (%.0f events/s). Errors: %d",
        sent,
        elapsed,
        rate,
        errors,
    )

    # Wait a moment for processing to complete, then print stats
    await asyncio.sleep(3)
    try:
        async with httpx.AsyncClient(base_url=AGGREGATOR_BASE, timeout=10) as client:
            resp = await client.get("/stats")
            logger.info("Aggregator stats: %s", resp.json())
    except Exception:
        pass


if __name__ == "__main__":
    asyncio.run(run())
