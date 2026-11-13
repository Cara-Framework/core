"""The relay's claim plan does not depend on when ``ANALYZE`` last ran.

Stress 2026-11-12/13: after a quiet period ``queue_job_delivery`` was
ANALYZEd while its open set was empty, so the sampled ``status`` /
``publish_status`` statistics said "no row is pending". The next burst's
claim then priced every open-set index at one row, picked the smallest one
(``queue_job_delivery_open_expiry_idx``) and FILTERED the whole open set in
every per-tenant probe: 5.8 s per claim at 20k open rows / 1,000 tenants
(10-53 s at the stress run's size), so the fleet's one publisher published
nothing until autoanalyze caught up 60-90 s later.

Those two columns describe a queue's moment, not the table: any sample of
them is wrong by the time a burst arrives. The model therefore declares
their statistics target 0 AND clears any sample already taken (a 0 target
alone keeps old samples forever), so the planner prices the open set with
its fixed default selectivity and the ordered index probes win at every
moment. The integration test reproduces the stale state on a session-local
copy of the ledger and checks the claim's plan and time.
"""

from __future__ import annotations

import json
import os
import time
from contextlib import contextmanager
from types import SimpleNamespace

import pytest

from cara.models.QueueJobDelivery import QueueJobDelivery
from cara.queues.delivery import QueueJobDeliveryStore
from cara.schema.Objects import created_objects

_VOLATILE_STATE = {"status", "publish_status"}


def _statistics_entries() -> list[dict]:
    return [
        entry
        for entry in QueueJobDelivery.__indexes__
        if any(
            name.startswith("statistics:") for name in created_objects(entry["up"]) or ()
        )
    ]


def test_the_ledger_never_samples_its_volatile_state_columns():
    declared = set()
    for entry in _statistics_entries():
        declared |= created_objects(entry["up"]) or set()
    assert {f"statistics:{column}=0" for column in _VOLATILE_STATE} <= declared


def test_the_statistics_entry_also_clears_samples_already_taken():
    """``SET STATISTICS 0`` alone leaves existing ``pg_statistic`` rows in
    place through every later ANALYZE — the 2026-11-12 attempt that changed
    nothing on the live ledger."""
    (entry,) = _statistics_entries()
    up = entry["up"]
    assert "pg_clear_attribute_stats" in up  # PostgreSQL 18+
    assert "format_type(" in up  # older servers: re-assert the column's own type
    assert "SET STATISTICS -1" in entry["down"]


# ── real Postgres ─────────────────────────────────────────────────────

_integration = pytest.mark.skipif(
    os.getenv("CARA_RUN_INTEGRATION") != "1"
    or not os.getenv("DB_DATABASE", "").startswith("synk_scratch_"),
    reason="set CARA_RUN_INTEGRATION=1 and a migrated DB_DATABASE named synk_scratch_*",
)


class _PostgresDB:
    def __init__(self, connection):
        self.connection = connection

    def _cursor(self):
        import psycopg2.extras

        return self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    @contextmanager
    def transaction(self):
        with self._cursor() as cursor:
            cursor.execute("BEGIN")
        try:
            yield self
        finally:
            # Every claim is rolled back: the open set stays as seeded.
            with self._cursor() as cursor:
                cursor.execute("ROLLBACK")

    def select_one(self, sql, params=None):
        with self._cursor() as cursor:
            cursor.execute(sql, params)
            return cursor.fetchone()

    def select(self, sql, params=None):
        with self._cursor() as cursor:
            cursor.execute(sql, params)
            return cursor.fetchall()


class _Application:
    def __init__(self, db):
        self.db = db

    def has(self, key):
        return key == "DB"

    def make(self, key):
        return self.db


_HISTORY = 120_000
_BURST = 12_000
_TENANTS = 600


@pytest.fixture
def stale_ledger():
    """A retained history ANALYZEd with an empty open set, then a burst."""
    import psycopg2

    connection = psycopg2.connect(dbname=os.environ["DB_DATABASE"])
    connection.autocommit = True
    with connection.cursor() as cursor:
        # pg_temp leads the search path, so the store's (and the entry's)
        # unqualified table name resolves to this session's copy.
        cursor.execute(
            "CREATE TEMP TABLE queue_job_delivery "
            "(LIKE public.queue_job_delivery INCLUDING ALL)"
        )
        for entry in _statistics_entries():
            cursor.execute(entry["up"])
        cursor.execute(
            "INSERT INTO queue_job_delivery (job_id, db_job_id, payload_sha256, "
            "signed_envelope, tenant_mode, tenant_id, queue, priority, status, "
            "attempts, completed_at, expires_at, available_at, publish_status, "
            "publish_attempts, publish_retry_at, published_at, "
            "post_hooks_completed_at, created_at, updated_at) "
            "SELECT 'h-' || g, g, repeat('a', 64), '{}'::jsonb, 'tenant', "
            f"9500000 + g % {_TENANTS}, "
            "(ARRAY['sync','bulk','orders','interactive'])[1 + g % 4], "
            "(ARRAY['critical','high','default','low'])[1 + g % 4], "
            "'completed', 1, NOW() - INTERVAL '1 hour', NOW() + INTERVAL '1 day', "
            "NOW() - INTERVAL '2 hours', 'published', 1, NOW() - INTERVAL '2 hours', "
            "NOW() - INTERVAL '2 hours' + (g % 3600) * INTERVAL '1 second', "
            "NOW() - INTERVAL '1 hour', NOW() - INTERVAL '2 hours', NOW() "
            f"FROM generate_series(1, {_HISTORY}) g"
        )
        # The quiet moment: nothing open while the sample is taken.
        cursor.execute("ANALYZE queue_job_delivery")
        cursor.execute(
            "INSERT INTO queue_job_delivery (job_id, db_job_id, payload_sha256, "
            "signed_envelope, tenant_mode, tenant_id, queue, priority, status, "
            "attempts, expires_at, available_at, publish_status, publish_attempts, "
            "publish_retry_at, created_at, updated_at) "
            "SELECT 'b-' || g, 1000000 + g, repeat('b', 64), '{}'::jsonb, 'tenant', "
            f"9500000 + g % {_TENANTS}, 'sync', 'default', 'pending', 0, "
            "NOW() + INTERVAL '1 day', NOW() - INTERVAL '1 second', 'pending', 0, "
            "NOW() - INTERVAL '1 second', NOW(), NOW() "
            f"FROM generate_series(1, {_BURST}) g"
        )
    try:
        yield connection
    finally:
        with connection.cursor() as cursor:
            cursor.execute("DROP TABLE IF EXISTS pg_temp.queue_job_delivery")
        connection.close()


def _store(db) -> QueueJobDeliveryStore:
    return QueueJobDeliveryStore(
        _Application(db),
        SimpleNamespace(),
        {
            "canonical_queues": ("sync", "bulk", "orders", "interactive"),
            "delivery_claim_batch": 100,
            "delivery_broker_window_per_queue": 64,
        },
    )


def _index_scans(node: dict) -> list[tuple[str, float]]:
    found = []
    if node.get("Index Name"):
        found.append((node["Index Name"], node.get("Actual Loops", 0)))
    for child in node.get("Plans", []) or []:
        found.extend(_index_scans(child))
    return found


@_integration
def test_a_burst_after_a_quiet_analyze_is_claimed_through_the_ordered_probes(
    stale_ledger,
):
    db = _PostgresDB(stale_ledger)
    store = _store(db)
    captured = {}
    original = db.select_one

    def select_one(sql, params=None):
        if sql.startswith("WITH RECURSIVE"):
            captured["plan"] = original("EXPLAIN (ANALYZE, FORMAT JSON) " + sql, params)[
                "QUERY PLAN"
            ]
            return original(sql, params)
        return original(sql, params)

    db.select_one = select_one
    store._claim_publish_batch(store.claim_batch)
    plan = captured["plan"]
    if isinstance(plan, str):
        plan = json.loads(plan)
    scans = _index_scans(plan[0]["Plan"])
    # The per-tenant probes run once per tenant: none of them may filter
    # the open set through an index that is merely small. ``LIKE ...
    # INCLUDING ALL`` renames the copies after their keys, so the
    # tenant-keyed ones (``tenant_head_idx``, ``tenant_served_idx``) read
    # ``..._coalesce_...`` here and ``open_expiry_idx`` ``..._expires_at_idx``.
    looped = {name for name, loops in scans if loops > 1}
    assert looped, scans
    assert all("coalesce" in name or name.endswith("_pkey") for name in looped), scans

    db.select_one = original
    started = time.perf_counter()
    claimed = store._claim_publish_batch(store.claim_batch)
    elapsed = time.perf_counter() - started
    assert claimed is not None
    # Stale plan: seconds per claim. Ordered probes: tens of milliseconds.
    assert elapsed < 1.0, elapsed


@_integration
def test_the_entry_clears_a_sample_an_existing_ledger_already_holds():
    """An evolved database ANALYZEd long before the entry landed."""
    import psycopg2

    connection = psycopg2.connect(dbname=os.environ["DB_DATABASE"])
    connection.autocommit = True
    sampled = (
        "SELECT attname FROM pg_stats WHERE tablename = 'queue_job_delivery' "
        "AND schemaname = (SELECT nspname FROM pg_namespace "
        "WHERE oid = pg_my_temp_schema()) AND attname IN ('status', 'publish_status')"
    )
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "CREATE TEMP TABLE queue_job_delivery "
                "(LIKE public.queue_job_delivery INCLUDING ALL)"
            )
            cursor.execute(
                "INSERT INTO queue_job_delivery (job_id, db_job_id, payload_sha256, "
                "signed_envelope, tenant_mode, queue, priority, status, "
                "completed_at, expires_at, available_at, publish_status, "
                "publish_retry_at, published_at, created_at, updated_at) "
                "SELECT 'h-' || g, g, repeat('a', 64), '{}'::jsonb, 'central', "
                "'sync', 'default', 'completed', NOW(), NOW() + INTERVAL '1 day', "
                "NOW(), 'published', NOW(), NOW(), NOW(), NOW() "
                "FROM generate_series(1, 500) g"
            )
            cursor.execute("ANALYZE queue_job_delivery")
            cursor.execute(sampled)
            assert {row[0] for row in cursor.fetchall()} == _VOLATILE_STATE

            for entry in _statistics_entries():
                cursor.execute(entry["up"])
            cursor.execute("ANALYZE queue_job_delivery")
            cursor.execute(sampled)
            assert cursor.fetchall() == []
    finally:
        with connection.cursor() as cursor:
            cursor.execute("DROP TABLE IF EXISTS pg_temp.queue_job_delivery")
        connection.close()
