"""
dedup_store.py — Persistent deduplication store backed by PostgreSQL.

Design decisions:
- PostgreSQL is chosen for its robust ACID guarantees, configurable
  isolation levels, and native JSONB support.
- Primary key (topic, event_id) enforces uniqueness at the DB level,
  eliminating TOCTOU race conditions.
- All writes use INSERT ... ON CONFLICT DO NOTHING for atomic idempotency —
  no separate SELECT-then-INSERT that could race under concurrent consumers.
- Isolation level: READ COMMITTED (PostgreSQL default). This is sufficient
  because the unique constraint on (topic, event_id) prevents duplicate
  inserts even under concurrent transactions. SERIALIZABLE would add
  overhead without benefit here since we rely on constraint-based conflict
  resolution, not read-dependent writes.
- Stats counters use UPDATE ... SET stat_value = stat_value + N within
  transactions to prevent lost-update anomalies under concurrent workers.
- Connection pool: maxconn=60 to support up to 50 concurrent threads in
  tests (test_stats_consistency_concurrent uses 50 threads) plus workers.
  minconn=2 keeps idle resource usage low.
- Batch processing: mark_processed_batch() inserts events in a single
  transaction using executemany(), dramatically reducing per-row TCP/IO
  overhead for high-throughput scenarios (stress test: 26k events).
"""

from __future__ import annotations

import json
import logging
import time
from typing import Optional

import psycopg2
import psycopg2.extras
import psycopg2.pool

logger = logging.getLogger(__name__)

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS processed_events (
    topic        TEXT             NOT NULL,
    event_id     TEXT             NOT NULL,
    timestamp    TEXT             NOT NULL,
    source       TEXT             NOT NULL,
    payload      JSONB            NOT NULL DEFAULT '{}',
    processed_at DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (topic, event_id)
);
"""

_CREATE_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_topic ON processed_events (topic);
"""

_CREATE_STATS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS system_stats (
    stat_key   TEXT PRIMARY KEY,
    stat_value INTEGER NOT NULL DEFAULT 0
);
"""

_INIT_STATS_SQL = """
INSERT INTO system_stats (stat_key, stat_value) VALUES
    ('received', 0),
    ('duplicate_dropped', 0)
ON CONFLICT (stat_key) DO NOTHING;
"""


class DedupStore:
    """
    Thread-safe, persistent deduplication store backed by PostgreSQL.

    Uses a ThreadedConnectionPool for safe concurrent access from multiple
    asyncio consumer workers running in the default executor.

    Pool size: maxconn=60 ensures up to 50 concurrent threads (unit tests)
    plus background workers can all hold a connection simultaneously without
    exhausting the pool.

    Isolation level: READ COMMITTED (default).
    Conflict resolution: unique constraint on (topic, event_id) +
    INSERT ... ON CONFLICT DO NOTHING ensures atomic idempotent writes.
    """

    def __init__(self, database_url: str = "postgresql://user:pass@storage:5432/logdb") -> None:
        self.database_url = database_url
        self._pool = psycopg2.pool.ThreadedConnectionPool(
            minconn=2,
            maxconn=60,  # raised from 10 to 60 to support 50-thread concurrent tests
            dsn=database_url,
        )
        self._init_db()
        logger.info("DedupStore initialised with PostgreSQL at %s", database_url.split("@")[-1])

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get_conn(self):
        """Get a connection from the pool."""
        return self._pool.getconn()

    def _put_conn(self, conn):
        """Return a connection to the pool."""
        self._pool.putconn(conn)

    def _init_db(self) -> None:
        conn = self._get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(_CREATE_TABLE_SQL)
                cur.execute(_CREATE_INDEX_SQL)
                cur.execute(_CREATE_STATS_TABLE_SQL)
                cur.execute(_INIT_STATS_SQL)
            conn.commit()
        finally:
            self._put_conn(conn)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def mark_processed(self, event) -> bool:
        """
        Atomically insert the event using INSERT ... ON CONFLICT DO NOTHING.

        This runs within a transaction (PostgreSQL auto-commit is off by
        default with psycopg2). The unique constraint (topic, event_id)
        ensures that even under concurrent workers, only one INSERT
        succeeds — the other gets rowcount=0.

        Isolation level: READ COMMITTED.
        This is safe because we do NOT read-then-write; we rely solely
        on the constraint for conflict detection. No phantom reads or
        write skew can affect this pattern.

        Returns:
            True  — event was new and has been stored.
            False — event was a duplicate; nothing written.
        """
        conn = self._get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO processed_events
                        (topic, event_id, timestamp, source, payload, processed_at)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (topic, event_id) DO NOTHING
                    """,
                    (
                        event.topic,
                        event.event_id,
                        event.timestamp,
                        event.source,
                        json.dumps(event.payload),
                        time.time(),
                    ),
                )
            conn.commit()
            return cur.rowcount == 1
        except Exception as exc:
            conn.rollback()
            logger.error("DedupStore write error: %s", exc)
            raise
        finally:
            self._put_conn(conn)

    def mark_processed_batch(self, events: list) -> tuple[int, int]:
        """
        Insert a batch of events in a SINGLE transaction using executemany().

        Dramatically reduces per-row TCP/IO overhead vs calling mark_processed()
        individually for each event. Suitable for high-throughput scenarios
        such as the stress test (20,000+ events).

        Uses INSERT ... ON CONFLICT DO NOTHING so duplicates are silently
        skipped at the DB level without raising an error.

        Returns:
            (new_count, dup_count) — count of new events and duplicates.
        """
        if not events:
            return 0, 0

        now = time.time()
        params = [
            (
                evt.topic,
                evt.event_id,
                evt.timestamp,
                evt.source,
                json.dumps(evt.payload),
                now,
            )
            for evt in events
        ]

        conn = self._get_conn()
        try:
            with conn.cursor() as cur:
                cur.executemany(
                    """
                    INSERT INTO processed_events
                        (topic, event_id, timestamp, source, payload, processed_at)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (topic, event_id) DO NOTHING
                    """,
                    params,
                )
                new_count = cur.rowcount
            conn.commit()
            dup_count = len(events) - new_count
            return new_count, dup_count
        except Exception as exc:
            conn.rollback()
            logger.error("DedupStore batch write error: %s", exc)
            raise
        finally:
            self._put_conn(conn)

    def is_duplicate(self, topic: str, event_id: str) -> bool:
        """Read-only check without writing — useful for unit tests."""
        conn = self._get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM processed_events WHERE topic=%s AND event_id=%s",
                    (topic, event_id),
                )
                return cur.fetchone() is not None
        finally:
            self._put_conn(conn)

    def get_events(self, topic: Optional[str] = None) -> list[dict]:
        """Return all stored (unique) events, optionally filtered by topic."""
        conn = self._get_conn()
        try:
            with conn.cursor() as cur:
                if topic:
                    cur.execute(
                        "SELECT topic, event_id, timestamp, source, payload "
                        "FROM processed_events WHERE topic=%s ORDER BY processed_at ASC",
                        (topic,),
                    )
                else:
                    cur.execute(
                        "SELECT topic, event_id, timestamp, source, payload "
                        "FROM processed_events ORDER BY processed_at ASC"
                    )
                rows = cur.fetchall()
                return [
                    {
                        "topic": row[0],
                        "event_id": row[1],
                        "timestamp": row[2],
                        "source": row[3],
                        "payload": row[4] if isinstance(row[4], dict) else json.loads(row[4]),
                    }
                    for row in rows
                ]
        finally:
            self._put_conn(conn)

    def get_topics(self) -> list[str]:
        """Return list of distinct topics seen so far."""
        conn = self._get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT DISTINCT topic FROM processed_events ORDER BY topic"
                )
                return [row[0] for row in cur.fetchall()]
        finally:
            self._put_conn(conn)

    def count_unique(self) -> int:
        """Total unique events stored."""
        conn = self._get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM processed_events")
                return cur.fetchone()[0]
        finally:
            self._put_conn(conn)

    def increment_stat(self, stat_key: str, amount: int = 1) -> None:
        """
        Increment a system stat counter atomically within a transaction.

        Uses UPDATE ... SET stat_value = stat_value + N which is safe
        under READ COMMITTED isolation: PostgreSQL locks the row during
        UPDATE, preventing lost-update anomalies even when multiple
        workers call this concurrently.
        """
        conn = self._get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE system_stats SET stat_value = stat_value + %s WHERE stat_key = %s",
                    (amount, stat_key),
                )
            conn.commit()
        except Exception as exc:
            conn.rollback()
            logger.error("increment_stat error: %s", exc)
            raise
        finally:
            self._put_conn(conn)

    def get_stat(self, stat_key: str) -> int:
        """Retrieve a system stat counter."""
        conn = self._get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT stat_value FROM system_stats WHERE stat_key = %s",
                    (stat_key,),
                )
                row = cur.fetchone()
                return row[0] if row else 0
        finally:
            self._put_conn(conn)

    def close(self) -> None:
        """Close all connections in the pool."""
        if self._pool:
            self._pool.closeall()
            logger.info("DedupStore connection pool closed")
