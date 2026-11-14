"""A seller's due backlog is not slowed by other tenants' rows that are not due yet.

Perf cycle 2 (2026-11-14). The plan's per-tenant share,
``CEIL(plan_limit / COUNT(DISTINCT tenant_key))``, counts every tenant with an
OPEN row in the lane (``status = pending``), including tenants whose only rows
are not leasable yet: a retry waiting out its backoff (``available_at`` in the
future), a coalescing debounce, a ``later()`` dispatch, or a publish backoff
(``publish_retry_at`` in the future). Those tenants offer nothing to the plan,
yet each one shrinks the due seller's share. At >= ``2 * claim_batch`` such
tenants the share is 1: the lone due seller gets ONE row per relay iteration
instead of its free tenant-window slots — the 77 rows/s single-seller drain
the share was introduced to fix (2026-11-14), brought back by any retry storm
or debounce wave across the fleet.

Production-shaped windows: claim batch 100, broker 4, tenant 3, urgent 1,
critical reserve 1, aging 300 s.

Needs a scratch database holding ``public.queue_job_delivery``::

    DB_DATABASE=synk_scratch_<label> CARA_RUN_INTEGRATION=1 pytest <this file>
"""

from __future__ import annotations

import os
import uuid
from types import SimpleNamespace

import pytest

from cara.queues.delivery import QueueJobDeliveryStore

pytestmark = pytest.mark.skipif(
    os.getenv("CARA_RUN_INTEGRATION") != "1"
    or not os.getenv("DB_DATABASE", "").startswith("synk_scratch_"),
    reason="set CARA_RUN_INTEGRATION=1 and a DB_DATABASE named synk_scratch_*",
)

_DUE_SELLER = 9_920_001
_WAITING_TENANTS = 200  # = 2 * claim batch: the share collapses to 1
_CLAIM_BATCH = 100


class _DB:
    def __init__(self, connection):
        self.connection = connection

    def _cursor(self):
        import psycopg2.extras

        return self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    class _Tx:
        def __init__(self, db):
            self.db = db

        def __enter__(self):
            with self.db._cursor() as cursor:
                cursor.execute("BEGIN")
            return self.db

        def __exit__(self, kind, *_):
            with self.db._cursor() as cursor:
                cursor.execute("ROLLBACK" if kind else "COMMIT")
            return False

    def transaction(self):
        return self._Tx(self)

    def advisory_xact_lock(self, *keys):
        self.select("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", [keys[0]])

    def select_one(self, sql, params=None):
        with self._cursor() as cursor:
            cursor.execute(sql, params)
            return cursor.fetchone() if cursor.description else None

    def select(self, sql, params=None):
        with self._cursor() as cursor:
            cursor.execute(sql, params)
            return cursor.fetchall() if cursor.description else []


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
            "canonical_queues": ("sync",),
            "delivery_claim_batch": _CLAIM_BATCH,
            "delivery_broker_window_per_queue": 4,
            "delivery_tenant_window_per_queue": 3,
            "delivery_tenant_urgent_slots_per_queue": 1,
            "delivery_critical_reserve_per_queue": 1,
            "delivery_priority_aging_seconds": 300,
        },
    )


@pytest.fixture
def ledger():
    import psycopg2

    schema = f"relay_share_{uuid.uuid4().hex[:10]}"
    connection = psycopg2.connect(dbname=os.environ["DB_DATABASE"])
    connection.autocommit = True
    with connection.cursor() as cursor:
        cursor.execute(f"CREATE SCHEMA {schema}")
        cursor.execute(
            f"CREATE TABLE {schema}.queue_job_delivery "
            "(LIKE public.queue_job_delivery INCLUDING ALL)"
        )
        cursor.execute(f"SET search_path TO {schema}, public")

    def seed(prefix, *, tenants, per_tenant, due_in, retry_in=0, first_tenant):
        """``tenants`` tenants x ``per_tenant`` rows, due in ``due_in`` s."""
        with connection.cursor() as cursor:
            cursor.execute(
                "INSERT INTO queue_job_delivery (job_id, db_job_id, payload_sha256, "
                "signed_envelope, tenant_mode, tenant_id, queue, priority, lane_key, "
                "status, attempts, expires_at, available_at, publish_status, "
                "publish_attempts, publish_retry_at, created_at, updated_at) "
                "SELECT %s || t || '-' || g, t::bigint * 100000 + g, repeat('a', 64), "
                "'{}'::jsonb, 'tenant', t, 'sync', 'default', 'channel:1', "
                "'pending', 0, NOW() + INTERVAL '1 day', "
                "NOW() + %s * INTERVAL '1 second' + g * INTERVAL '1 ms', 'pending', "
                "0, NOW() + %s * INTERVAL '1 second', NOW(), NOW() "
                "FROM generate_series(%s, %s) t, generate_series(1, %s) g",
                [
                    prefix,
                    due_in,
                    retry_in,
                    first_tenant,
                    first_tenant + tenants - 1,
                    per_tenant,
                ],
            )

    try:
        yield SimpleNamespace(seed=seed, db=_DB(connection))
    finally:
        with connection.cursor() as cursor:
            cursor.execute(f"DROP SCHEMA {schema} CASCADE")
        connection.close()


def _one_relay_iteration(store) -> list[str]:
    """Exactly what :meth:`publish_due` leases in one iteration, minus the broker."""
    remaining = store.claim_batch
    planned = store._plan_publish(store.claim_batch)
    leased: list[str] = []
    while remaining > 0 and planned:
        claimed = store._reserve_publish(planned, remaining)
        if claimed is None:
            break
        taken = [str(row["job_id"]) for row in claimed[0]]
        leased += taken
        planned = [entry for entry in planned if entry[0] not in set(taken)]
        remaining -= len(taken)
    return leased


def _due_seller_backlog(ledger):
    ledger.seed("due-", tenants=1, per_tenant=500, due_in=-60, first_tenant=_DUE_SELLER)


def test_control_a_lone_due_seller_fills_its_tenant_window_in_one_iteration(ledger):
    _due_seller_backlog(ledger)

    assert len(_one_relay_iteration(_store(ledger.db))) == 3


@pytest.mark.parametrize(
    ("due_in", "retry_in", "why"),
    [
        (60, 0, "retry backoff / debounce / later(): available_at in the future"),
        (-60, 60, "publish backoff: publish_retry_at in the future"),
    ],
)
def test_tenants_with_nothing_due_do_not_shrink_the_due_sellers_turn(
    ledger, due_in, retry_in, why
):
    _due_seller_backlog(ledger)
    # 200 other sellers, each with one row that is NOT leasable yet.
    ledger.seed(
        "wait-",
        tenants=_WAITING_TENANTS,
        per_tenant=1,
        due_in=due_in,
        retry_in=retry_in,
        first_tenant=1,
    )

    leased = _one_relay_iteration(_store(ledger.db))

    # Nothing else is due, so the seller's free tenant-window slots (3) are
    # its turn — not one row per iteration.
    assert len(leased) == 3, why


def _plan_lanes_visited(ledger) -> int:
    """How many (tenant, lane) pairs one plan's lane skip scan visits (EXPLAIN ANALYZE)."""
    import json as _json

    captured: dict = {}
    select_one = ledger.db.select_one

    def capture(sql, params=None):
        captured.setdefault("sql", sql)
        captured.setdefault("params", params)
        return select_one(sql, params)

    ledger.db.select_one = capture
    try:
        _store(ledger.db)._plan_publish(_CLAIM_BATCH)
    finally:
        ledger.db.select_one = select_one
    explained = ledger.db.select_one(
        "EXPLAIN (ANALYZE, FORMAT JSON) " + captured["sql"], captured["params"]
    )
    plan = explained["QUERY PLAN"]
    plan = _json.loads(plan) if isinstance(plan, str) else plan

    def walk(node):
        if node.get("Subplan Name") == "CTE lane_keys":
            return int(node["Actual Rows"]) * int(node.get("Actual Loops", 1))
        for child in node.get("Plans", ()):
            found = walk(child)
            if found is not None:
                return found
        return None

    visited = walk(plan[0]["Plan"])
    assert visited is not None, "the plan has no lane_keys CTE"
    return visited


def test_shops_waiting_out_a_retry_cost_the_plan_nothing(ledger):
    """Central webhook deliveries take one lane PER SHOP (``source:<shop>``).
    A webhook retry storm leaves thousands of shops with only a future-dated
    retry in the central tenant: the plan must skip those lanes inside the
    index, as it skips tenants with nothing due. Visited, each cost two
    probes per relay iteration: 474 ms at 50k waiting shops (scratch stack
    2026-11-14), a relay publishing ~2 batches/s for the whole fleet."""
    ledger.db.select(
        "INSERT INTO queue_job_delivery (job_id, db_job_id, payload_sha256, "
        "signed_envelope, tenant_mode, tenant_id, queue, priority, lane_key, "
        "status, attempts, expires_at, available_at, publish_status, "
        "publish_attempts, publish_retry_at, created_at, updated_at) "
        "SELECT 'shop-' || g, 800000000 + g, repeat('a', 64), '{}'::jsonb, "
        "'central', NULL, 'sync', 'default', 'source:shop-' || g, 'pending', 0, "
        "NOW() + INTERVAL '1 day', "
        "NOW() + CASE WHEN g = 1 THEN -60 ELSE 120 END * INTERVAL '1 second', "
        "'pending', 0, NOW(), NOW(), NOW() "
        "FROM generate_series(1, 2000) g RETURNING 1"
    )
    ledger.db.select("ANALYZE queue_job_delivery")

    # Only shop 1's sale is due: one lane to visit, not 2,000.
    assert _plan_lanes_visited(ledger) <= 2
    assert _one_relay_iteration(_store(ledger.db)) == ["shop-1"]
