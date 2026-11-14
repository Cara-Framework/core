"""Relay replicas run in parallel without one shared lock, and stay exact.

Before 2026-11-13 the whole tenant-fair claim ran under ONE advisory lock,
so every replica queued behind it: a second relay added +36 %, a third and
fourth nothing (1M-row stress). The claim is now an unlocked plan and a
reserve serialized per QUEUE. These tests run real replicas — one connection
each, on threads — against a scratch ledger and check what the global lock
used to guarantee:

* a queue's broker window is never overfilled, however many replicas reserve;
* no row is ever leased twice (exactly one publication per delivery row);
* a seller's tenant window holds, and single-job sellers are served before a
  backlogged one fills the window;
* replicas reserving different queues never wait on each other.

Needs a migrated scratch database::

    DB_DATABASE=synk_scratch_<label> CARA_RUN_INTEGRATION=1 pytest <this file>
"""

from __future__ import annotations

import os
import threading
import uuid
from collections import Counter
from contextlib import contextmanager
from types import SimpleNamespace

import pytest

from cara.queues.delivery import QueueJobDeliveryStore

pytestmark = pytest.mark.skipif(
    os.getenv("CARA_RUN_INTEGRATION") != "1"
    or not os.getenv("DB_DATABASE", "").startswith("synk_scratch_"),
    reason="set CARA_RUN_INTEGRATION=1 and a migrated DB_DATABASE named synk_scratch_*",
)

_WINDOW = 24
_TENANT_WINDOW = 6
_REPLICAS = 4


class _Replica:
    """One relay replica's database: its own connection, real transactions."""

    def __init__(self, schema: str):
        import psycopg2

        self.connection = psycopg2.connect(dbname=os.environ["DB_DATABASE"])
        self.connection.autocommit = True
        with self.connection.cursor() as cursor:
            cursor.execute(f"SET search_path TO {schema}, public")

    def _cursor(self):
        import psycopg2.extras

        return self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    @contextmanager
    def transaction(self):
        with self._cursor() as cursor:
            cursor.execute("BEGIN")
        try:
            yield self
        except BaseException:
            with self._cursor() as cursor:
                cursor.execute("ROLLBACK")
            raise
        with self._cursor() as cursor:
            cursor.execute("COMMIT")

    def advisory_xact_lock(self, *keys):
        self.select(
            "SELECT pg_advisory_xact_lock(hashtextextended(requested.key, 0)) "
            "FROM (SELECT DISTINCT key FROM unnest(%s::text[]) AS key) "
            "AS requested ORDER BY requested.key",
            [list(keys)],
        )

    def select_one(self, sql, params=None):
        with self._cursor() as cursor:
            cursor.execute(sql, params)
            return cursor.fetchone() if cursor.description else None

    def select(self, sql, params=None):
        with self._cursor() as cursor:
            cursor.execute(sql, params)
            return cursor.fetchall() if cursor.description else []

    def statement(self, sql, params=None):
        with self._cursor() as cursor:
            cursor.execute(sql, params)
            return cursor.rowcount


class _Application:
    def __init__(self, db):
        self.db = db

    def has(self, key):
        return key == "DB"

    def make(self, key):
        return self.db


def _store(db) -> QueueJobDeliveryStore:
    return QueueJobDeliveryStore(
        _Application(db),
        SimpleNamespace(),
        {
            "canonical_queues": ("bulk", "sync"),
            "delivery_claim_batch": 10,
            "delivery_broker_window_per_queue": _WINDOW,
            "delivery_tenant_window_per_queue": _TENANT_WINDOW,
        },
    )


@pytest.fixture
def ledger():
    """A private copy of the ledger (its own schema), dropped afterwards."""
    import psycopg2

    schema = f"relay_parallel_{uuid.uuid4().hex[:10]}"
    admin = psycopg2.connect(dbname=os.environ["DB_DATABASE"])
    admin.autocommit = True
    with admin.cursor() as cursor:
        cursor.execute(f"CREATE SCHEMA {schema}")
        # LIKE copies columns, defaults, checks and indexes — not the foreign
        # keys, so the rows need no job tracker.
        cursor.execute(
            f"CREATE TABLE {schema}.queue_job_delivery "
            "(LIKE public.queue_job_delivery INCLUDING ALL)"
        )

    def seed(rows: list[tuple[int, str]]) -> None:
        with admin.cursor() as cursor:
            cursor.execute(
                f"INSERT INTO {schema}.queue_job_delivery (job_id, db_job_id, "
                "payload_sha256, signed_envelope, tenant_mode, tenant_id, queue, "
                "priority, status, attempts, expires_at, available_at, "
                "publish_status, publish_attempts, publish_retry_at, created_at, "
                "updated_at) "
                "SELECT 'r-' || seed.ordinality, seed.ordinality, repeat('a', 64), "
                "'{}'::jsonb, 'tenant', seed.tenant, seed.queue, 'default', "
                "'pending', 0, NOW() + INTERVAL '1 day', "
                "NOW() - INTERVAL '1 minute' + seed.ordinality * INTERVAL '1 ms', "
                "'pending', 0, NOW() - INTERVAL '1 minute', NOW(), NOW() "
                "FROM unnest(%s::bigint[], %s::varchar[]) WITH ORDINALITY "
                "AS seed(tenant, queue)",
                [[tenant for tenant, _q in rows], [queue for _t, queue in rows]],
            )

    def complete(job_ids: list[str]) -> None:
        with admin.cursor() as cursor:
            cursor.execute(
                f"UPDATE {schema}.queue_job_delivery SET status = 'completed', "
                "completed_at = NOW(), publish_status = 'published', "
                "published_at = NOW(), publish_lease_token = NULL, "
                "publish_lease_expires_at = NULL WHERE job_id = ANY(%s)",
                [job_ids],
            )

    def held() -> dict[tuple[str, int], int]:
        with admin.cursor() as cursor:
            cursor.execute(
                f"SELECT queue, tenant_id, COUNT(*) FROM {schema}.queue_job_delivery "
                "WHERE status = 'pending' AND publish_status = 'processing' "
                "GROUP BY queue, tenant_id"
            )
            return {(queue, tenant): count for queue, tenant, count in cursor.fetchall()}

    replicas = [_Replica(schema) for _ in range(_REPLICAS)]
    try:
        yield SimpleNamespace(
            schema=schema,
            admin=admin,
            replicas=replicas,
            seed=seed,
            complete=complete,
            held=held,
        )
    finally:
        for replica in replicas:
            replica.connection.close()
        with admin.cursor() as cursor:
            cursor.execute(f"DROP SCHEMA {schema} CASCADE")
        admin.close()


def _round(ledger) -> list[str]:
    """Every replica claims until its queue is full; return every leased id."""
    leased: list[str] = []
    guard = threading.Lock()
    errors: list[BaseException] = []
    start = threading.Barrier(len(ledger.replicas))

    def relay(db):
        store = _store(db)
        try:
            start.wait()
            while True:
                claimed = store._claim_publish_batch(store.claim_batch)
                if claimed is None:
                    return
                with guard:
                    leased.extend(str(row["job_id"]) for row in claimed[0])
        except BaseException as exc:  # surfaced on the main thread
            errors.append(exc)

    threads = [threading.Thread(target=relay, args=(db,)) for db in ledger.replicas]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(60)
    assert not errors, errors
    return leased


def test_parallel_replicas_fill_the_window_exactly_and_lease_each_row_once(ledger):
    ledger.seed([(9_600_000 + index % 40, "sync") for index in range(800)])

    every_lease: list[str] = []
    for _ in range(6):
        leased = _round(ledger)
        # The window, exactly: never overfilled by racing replicas.
        assert len(leased) == _WINDOW
        assert max(ledger.held().values()) <= _TENANT_WINDOW
        every_lease.extend(leased)
        # The workers finish this window; the next round refills it.
        ledger.complete(leased)

    duplicates = [job_id for job_id, count in Counter(every_lease).items() if count > 1]
    assert duplicates == []
    assert len(every_lease) == 6 * _WINDOW


def test_single_job_sellers_are_served_before_a_backlog_fills_the_window(ledger):
    backlog = 9_700_000
    newcomers = [9_700_100 + index for index in range(12)]
    ledger.seed(
        [(backlog, "sync") for _ in range(300)]
        + [(tenant, "sync") for tenant in newcomers]
    )

    _round(ledger)

    held = ledger.held()
    # Every newcomer got its one row in the first fill of the window ...
    assert all(held.get(("sync", tenant)) == 1 for tenant in newcomers)
    # ... and the backlogged seller stopped at its tenant window.
    assert held[("sync", backlog)] == _TENANT_WINDOW


def test_a_reserve_on_one_queue_never_waits_for_another_queue(ledger):
    ledger.seed([(9_800_000 + index, "bulk") for index in range(5)])
    blocker = ledger.replicas[0]
    with blocker.transaction():
        # Another replica is mid-reserve on ``sync``.
        blocker.advisory_xact_lock("queue_job_delivery:publish-window:sync")
        other = ledger.replicas[1]
        with other._cursor() as cursor:
            cursor.execute("SET lock_timeout = '2s'")
        claimed = _store(other)._claim_publish_batch(10)
    assert claimed is not None
    assert len(claimed[0]) == 5
