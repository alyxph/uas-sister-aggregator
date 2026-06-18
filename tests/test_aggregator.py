"""
tests/test_aggregator.py — Unit & integration tests for Pub-Sub Log Aggregator (UAS).

Coverage (16 tests):
    1.  Schema validation — valid event passes.
    2.  Schema validation — missing required field raises.
    3.  Schema validation — malformed ISO 8601 timestamp raises.
    4.  Schema validation — empty string fields raise.
    5.  Deduplication — duplicate event is dropped (processed only once).
    6.  Deduplication persistence — simulated restart via fresh DedupStore instance.
    7.  POST /publish single event → 202 accepted.
    8.  POST /publish batch events → queued count matches.
    9.  POST /publish invalid event → 422 error.
    10. GET /events returns only unique events.
    11. GET /stats returns consistent counters.
    12. Concurrent race condition — multi-thread dedup atomicity.
    13. Stats consistency under concurrent writes.
    14. Stress test — 20,000 events with ≥30% duplicates.
    15. Batch deduplication — batch with duplicate event_ids.
    16. GET /health returns ok status.

Run with:
    pytest tests/ -v

Note: Tests 7-11, 16 require a running PostgreSQL and Redis instance.
      For local testing without Docker, tests 1-6 and 12-14 can run against
      a local PostgreSQL. Set DATABASE_URL and BROKER_URL env vars accordingly.

      For unit-only testing (no external services), run:
          pytest tests/ -v -k "not app_client"
"""

from __future__ import annotations

import os
import time
import uuid
import json
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Barrier

import pytest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ts() -> str:
    """Return current UTC timestamp in ISO 8601 format."""
    return datetime.now(timezone.utc).isoformat()


def _eid() -> str:
    """Return a random UUID string as event_id."""
    return str(uuid.uuid4())


def _event(
    topic: str = "test.topic",
    event_id: str | None = None,
    source: str = "unit-test",
    payload: dict | None = None,
) -> dict:
    return {
        "topic": topic,
        "event_id": event_id or _eid(),
        "timestamp": _ts(),
        "source": source,
        "payload": payload or {},
    }


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="function")
def pg_database_url():
    """
    Return a PostgreSQL DATABASE_URL for testing.

    Uses DATABASE_URL env var if set, otherwise defaults to Docker test DB.
    Uses 127.0.0.1 (IPv4) explicitly instead of 'localhost' to avoid
    Windows resolving it to ::1 (IPv6) which may hit a local PostgreSQL
    installation instead of the Docker container listening on 0.0.0.0:5432.
    """
    url = os.environ.get(
        "DATABASE_URL",
        "postgresql://user:pass@127.0.0.1:5432/logdb"
    )
    return url


@pytest.fixture(scope="function")
def dedup(pg_database_url):
    """Fresh DedupStore backed by PostgreSQL. Truncates tables before each test."""
    from src.dedup_store import DedupStore
    store = DedupStore(database_url=pg_database_url)
    # Clean slate for each test
    conn = store._get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE processed_events, system_stats RESTART IDENTITY")
            cur.execute("""
                INSERT INTO system_stats (stat_key, stat_value) VALUES
                    ('received', 0), ('duplicate_dropped', 0)
                ON CONFLICT (stat_key) DO NOTHING
            """)
        conn.commit()
    finally:
        store._put_conn(conn)
    yield store
    store.close()


@pytest.fixture(scope="function")
def broker_url():
    """Return a Redis BROKER_URL for testing."""
    return os.environ.get("BROKER_URL", "redis://localhost:6379/0")


@pytest.fixture(scope="function")
def app_client(pg_database_url, broker_url, monkeypatch):
    """
    TestClient for the FastAPI app with isolated PostgreSQL and Redis.
    Uses context manager to trigger lifespan (starts consumer workers).
    """
    import src.dedup_store as ds_mod
    import src.main as main_mod
    import src.queue_manager as qm_mod

    # Fresh isolated instances
    fresh_store = ds_mod.DedupStore(database_url=pg_database_url)

    # Truncate for clean slate
    conn = fresh_store._get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE processed_events, system_stats RESTART IDENTITY")
            cur.execute("""
                INSERT INTO system_stats (stat_key, stat_value) VALUES
                    ('received', 0), ('duplicate_dropped', 0)
                ON CONFLICT (stat_key) DO NOTHING
            """)
        conn.commit()
    finally:
        fresh_store._put_conn(conn)

    fresh_qm = qm_mod.QueueManager(
        dedup_store=fresh_store,
        broker_url=broker_url,
        n_workers=2,
    )

    # Inject before lifespan runs
    monkeypatch.setattr(main_mod, "dedup_store", fresh_store)
    monkeypatch.setattr(main_mod, "queue_manager", fresh_qm)

    # Patch _init_services to no-op (we already injected)
    monkeypatch.setattr(main_mod, "_init_services", lambda *a, **kw: None)

    from fastapi.testclient import TestClient

    with TestClient(main_mod.app) as client:
        yield client


# ---------------------------------------------------------------------------
# 1. Schema validation — valid event
# ---------------------------------------------------------------------------

def test_valid_event_schema():
    from src.models import Event

    e = Event(
        topic="logs.app",
        event_id="abc-123",
        timestamp="2024-01-01T00:00:00+00:00",
        source="svc-a",
        payload={"level": "INFO"},
    )
    assert e.topic == "logs.app"
    assert e.event_id == "abc-123"


# ---------------------------------------------------------------------------
# 2. Schema validation — missing required field
# ---------------------------------------------------------------------------

def test_missing_field_raises():
    from pydantic import ValidationError
    from src.models import Event

    with pytest.raises(ValidationError):
        Event(
            # topic intentionally omitted
            event_id="x",
            timestamp="2024-01-01T00:00:00Z",
            source="svc",
        )


# ---------------------------------------------------------------------------
# 3. Schema validation — invalid timestamp
# ---------------------------------------------------------------------------

def test_invalid_timestamp_raises():
    from pydantic import ValidationError
    from src.models import Event

    with pytest.raises(ValidationError):
        Event(
            topic="t",
            event_id="e",
            timestamp="not-a-date",
            source="s",
        )


# ---------------------------------------------------------------------------
# 4. Schema validation — empty string fields
# ---------------------------------------------------------------------------

def test_empty_string_fields_raise():
    from pydantic import ValidationError
    from src.models import Event

    with pytest.raises(ValidationError):
        Event(
            topic="",
            event_id="e",
            timestamp="2024-01-01T00:00:00Z",
            source="s",
        )

    with pytest.raises(ValidationError):
        Event(
            topic="t",
            event_id="  ",
            timestamp="2024-01-01T00:00:00Z",
            source="s",
        )


# ---------------------------------------------------------------------------
# 5. Deduplication — duplicate dropped
# ---------------------------------------------------------------------------

def test_dedup_drops_duplicate(dedup):
    from src.models import Event

    evt = Event(**_event(event_id="dup-001"))

    first = dedup.mark_processed(evt)
    second = dedup.mark_processed(evt)

    assert first is True,  "First insertion should succeed"
    assert second is False, "Second insertion should be detected as duplicate"
    assert dedup.count_unique() == 1


# ---------------------------------------------------------------------------
# 6. Deduplication persistence — simulated restart
# ---------------------------------------------------------------------------

def test_dedup_persists_across_restart(pg_database_url):
    """
    Simulates a container restart by discarding the first DedupStore instance
    and creating a new one pointing at the same PostgreSQL database.
    """
    from src.dedup_store import DedupStore
    from src.models import Event

    evt = Event(**_event(event_id="persist-evt-999"))

    store1 = DedupStore(database_url=pg_database_url)
    # Clean
    conn = store1._get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE processed_events, system_stats RESTART IDENTITY")
            cur.execute("""
                INSERT INTO system_stats (stat_key, stat_value) VALUES
                    ('received', 0), ('duplicate_dropped', 0)
                ON CONFLICT (stat_key) DO NOTHING
            """)
        conn.commit()
    finally:
        store1._put_conn(conn)

    assert store1.mark_processed(evt) is True
    store1.close()

    # "Restart" — new store instance, same database
    store2 = DedupStore(database_url=pg_database_url)
    assert store2.mark_processed(evt) is False, \
        "After restart, duplicate must still be rejected"
    assert store2.is_duplicate("test.topic", "persist-evt-999")
    store2.close()


# ---------------------------------------------------------------------------
# 7. POST /publish — single event accepted
# ---------------------------------------------------------------------------

def test_publish_single_event(app_client):
    payload = _event()
    resp = app_client.post("/publish", json=payload)
    assert resp.status_code == 202
    data = resp.json()
    assert data["queued"] == 1


# ---------------------------------------------------------------------------
# 8. POST /publish — batch accepted
# ---------------------------------------------------------------------------

def test_publish_batch(app_client):
    batch = {"events": [_event() for _ in range(10)]}
    resp = app_client.post("/publish", json=batch)
    assert resp.status_code == 202
    assert resp.json()["queued"] == 10


# ---------------------------------------------------------------------------
# 9. POST /publish — invalid event returns 422
# ---------------------------------------------------------------------------

def test_publish_invalid_event(app_client):
    invalid = {"topic": "t", "event_id": "e"}  # missing timestamp, source
    resp = app_client.post("/publish", json=invalid)
    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# 10. GET /events — returns unique events only
# ---------------------------------------------------------------------------

def test_get_events_unique_only(app_client):
    shared_id = _eid()

    # Send the same event 3 times
    for _ in range(3):
        app_client.post("/publish", json=_event(event_id=shared_id))

    # Send 4 unique events
    for _ in range(4):
        app_client.post("/publish", json=_event())

    # Poll until queue drains (max 10s)
    deadline = time.time() + 10
    while time.time() < deadline:
        stats = app_client.get("/stats").json()
        if stats["queue_size"] == 0:
            break
        time.sleep(0.2)

    resp = app_client.get("/events")
    assert resp.status_code == 200
    events = resp.json()["events"]
    ids = [e["event_id"] for e in events]
    assert ids.count(shared_id) == 1, "Duplicate event_id must appear only once"
    assert len(set(ids)) == len(ids), "All returned event_ids must be unique"


# ---------------------------------------------------------------------------
# 11. GET /stats — consistent counters
# ---------------------------------------------------------------------------

def test_stats_consistent(app_client):
    n_unique = 5
    n_dups = 3
    base_id = _eid()

    # Send unique events
    for _ in range(n_unique):
        app_client.post("/publish", json=_event())

    # Send duplicates
    for _ in range(n_dups):
        app_client.post("/publish", json=_event(event_id=base_id))

    # Poll until queue drains (max 10s)
    deadline = time.time() + 10
    while time.time() < deadline:
        stats_check = app_client.get("/stats").json()
        if stats_check["queue_size"] == 0:
            break
        time.sleep(0.2)

    resp = app_client.get("/stats")
    assert resp.status_code == 200
    stats = resp.json()
    assert stats["received"] >= n_unique + n_dups
    assert stats["duplicate_dropped"] >= n_dups - 1  # first send is unique
    assert "uptime_seconds" in stats
    assert isinstance(stats["topics"], list)


# ---------------------------------------------------------------------------
# 12. Concurrent race condition — multi-thread dedup atomicity
#     (Rubrik: Transaksi & Konkurensi — 16 poin)
# ---------------------------------------------------------------------------

def test_concurrent_race_condition(dedup):
    """
    Spawn 10 threads that simultaneously attempt to mark_processed()
    the SAME event. Only ONE should succeed (return True); the rest
    must return False.

    This proves that PostgreSQL's INSERT ... ON CONFLICT DO NOTHING
    with the unique constraint (topic, event_id) is atomic under
    concurrent access. No double-processing is possible.

    Isolation level: READ COMMITTED is sufficient because we rely on
    constraint-based conflict resolution, not read-dependent writes.
    """
    from src.models import Event

    evt = Event(**_event(event_id="race-condition-test-001"))
    n_threads = 10
    results = []

    # Use a Barrier to ensure all threads start mark_processed at ~same time
    barrier = Barrier(n_threads)

    def worker():
        barrier.wait()
        return dedup.mark_processed(evt)

    with ThreadPoolExecutor(max_workers=n_threads) as pool:
        futures = [pool.submit(worker) for _ in range(n_threads)]
        results = [f.result() for f in as_completed(futures)]

    true_count = results.count(True)
    false_count = results.count(False)

    assert true_count == 1, f"Exactly 1 thread should succeed, got {true_count}"
    assert false_count == n_threads - 1, f"Remaining threads should fail, got {false_count}"
    assert dedup.count_unique() == 1, "Only 1 event should be in DB"


# ---------------------------------------------------------------------------
# 13. Stats consistency under concurrent writes
#     (Rubrik: Transaksi & Konkurensi — lost-update prevention)
# ---------------------------------------------------------------------------

def test_stats_consistency_concurrent(dedup):
    """
    Run 50 concurrent threads, each incrementing 'received' by 1.
    Final value must be exactly 50 — no lost updates.

    PostgreSQL's UPDATE ... SET stat_value = stat_value + 1 acquires
    a row-level lock, ensuring atomic read-modify-write even under
    READ COMMITTED isolation.
    """
    n_threads = 50
    barrier = Barrier(n_threads)

    def worker():
        barrier.wait()
        dedup.increment_stat("received", 1)

    with ThreadPoolExecutor(max_workers=n_threads) as pool:
        futures = [pool.submit(worker) for _ in range(n_threads)]
        for f in as_completed(futures):
            f.result()  # raise if any thread failed

    final = dedup.get_stat("received")
    assert final == n_threads, f"Expected {n_threads}, got {final}. Lost-update detected!"


# ---------------------------------------------------------------------------
# 14. Stress test — 20,000 events, ≥30% duplicates, < 60s
# ---------------------------------------------------------------------------

def test_stress_20000_events(dedup):
    """
    Sends 20,000 unique events plus 6,000 duplicates (30%) through
    DedupStore directly (bypasses HTTP overhead for speed).
    Asserts correctness and timing.

    Uses mark_processed_batch() for efficient bulk inserts via executemany()
    in chunks of 500 events per transaction, dramatically reducing TCP/IO
    overhead compared to per-row mark_processed() calls.

    Requirement: >= 20,000 events with >= 30% duplicates must process
    correctly and remain responsive (< 60s).
    """
    from src.models import Event
    import random

    total_unique = 20000
    dup_rate = 0.30
    chunk_size = 500  # events per batch transaction

    events = [Event(**_event(topic="stress.topic", event_id=str(i))) for i in range(total_unique)]

    dup_events = random.choices(events, k=int(total_unique * dup_rate))
    all_events = events + dup_events
    random.shuffle(all_events)

    t0 = time.perf_counter()
    new_count = 0
    dup_count = 0

    # Process in batches for efficiency
    for i in range(0, len(all_events), chunk_size):
        chunk = all_events[i : i + chunk_size]
        new, dup = dedup.mark_processed_batch(chunk)
        new_count += new
        dup_count += dup

    elapsed = time.perf_counter() - t0

    assert new_count == total_unique, f"Expected {total_unique} unique, got {new_count}"
    assert dup_count == int(total_unique * dup_rate), \
        f"Expected {int(total_unique * dup_rate)} duplicates, got {dup_count}"
    assert elapsed < 60, f"Processing took too long: {elapsed:.2f}s"
    assert dedup.count_unique() == total_unique
    print(f"\n  Stress test: {len(all_events)} events in {elapsed:.2f}s "
          f"({len(all_events)/elapsed:.0f} events/s)")


# ---------------------------------------------------------------------------
# 15. Batch deduplication — batch with duplicate event_ids
# ---------------------------------------------------------------------------

def test_batch_dedup(dedup):
    """
    Submit a batch where some events share the same event_id.
    Only unique (topic, event_id) pairs should be stored.
    """
    from src.models import Event

    events = [
        Event(**_event(topic="batch.test", event_id="batch-dup-001")),
        Event(**_event(topic="batch.test", event_id="batch-dup-001")),  # dup
        Event(**_event(topic="batch.test", event_id="batch-dup-002")),
        Event(**_event(topic="batch.test", event_id="batch-dup-002")),  # dup
        Event(**_event(topic="batch.test", event_id="batch-dup-003")),
    ]

    results = [dedup.mark_processed(e) for e in events]
    assert results == [True, False, True, False, True]
    assert dedup.count_unique() == 3


# ---------------------------------------------------------------------------
# 16. GET /health — returns ok status
# ---------------------------------------------------------------------------

def test_health_endpoint(app_client):
    resp = app_client.get("/health")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "ok"
    assert "uptime_seconds" in data
