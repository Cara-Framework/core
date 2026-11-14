"""One channel's aged backlog never holds another channel's push at the relay.

Live 2026-11-13 (tenant 3213): one WooCommerce store's 9,488 bulk ``end``
pushes aged to the top tier, and the claim — fair across tenants but ordered
by aged tier INSIDE one tenant — gave every freed slot of the seller's tenant
window to them, so a push of the same seller to another channel waited behind
all of them. Rows now carry a ``lane_key`` (the channel) and the claim takes
turns across a tenant's lanes. Production-shaped windows: broker 4, tenant 3,
urgent 1, critical reserve 1, aging 300 s.

Needs a migrated scratch database::

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
    reason="set CARA_RUN_INTEGRATION=1 and a migrated DB_DATABASE named synk_scratch_*",
)

_SELLER = 9_910_001
_AGED_ENDS = 200


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
            "delivery_claim_batch": 10,
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

    schema = f"relay_lane_{uuid.uuid4().hex[:10]}"
    connection = psycopg2.connect(dbname=os.environ["DB_DATABASE"])
    connection.autocommit = True
    with connection.cursor() as cursor:
        cursor.execute(f"CREATE SCHEMA {schema}")
        cursor.execute(
            f"CREATE TABLE {schema}.queue_job_delivery "
            "(LIKE public.queue_job_delivery INCLUDING ALL)"
        )
        cursor.execute(f"SET search_path TO {schema}, public")

    def seed(prefix, count, *, lane, priority, age, publish_status="pending"):
        published = publish_status == "published"
        with connection.cursor() as cursor:
            cursor.execute(
                "INSERT INTO queue_job_delivery (job_id, db_job_id, payload_sha256, "
                "signed_envelope, tenant_mode, tenant_id, queue, priority, lane_key, "
                "status, attempts, expires_at, available_at, publish_status, "
                "publish_attempts, publish_retry_at, published_at, created_at, "
                "updated_at) "
                "SELECT %s || g, g, repeat('a', 64), '{}'::jsonb, 'tenant', %s, "
                "'sync', %s, %s, 'pending', 0, NOW() + INTERVAL '1 day', "
                "NOW() - %s * INTERVAL '1 second' + g * INTERVAL '1 ms', %s, 0, "
                "NOW() - %s * INTERVAL '1 second', "
                "CASE WHEN %s THEN NOW() ELSE NULL END, NOW(), NOW() "
                "FROM generate_series(1, %s) g",
                [
                    prefix,
                    _SELLER,
                    priority,
                    lane,
                    age,
                    publish_status,
                    age,
                    published,
                    count,
                ],
            )

    try:
        yield SimpleNamespace(seed=seed, db=_DB(connection))
    finally:
        with connection.cursor() as cursor:
            cursor.execute(f"DROP SCHEMA {schema} CASCADE")
        connection.close()


def _claimed_ids(store, limit) -> list[str]:
    claimed = store._claim_publish_batch(limit)
    return [] if claimed is None else [str(row["job_id"]) for row in claimed[0]]


def test_another_channels_push_gets_the_sellers_next_slot_not_the_aged_ends(ledger):
    # Channel A: 200 bulk ends queued an hour ago (aged to the top tier).
    ledger.seed("a-", _AGED_ENDS, lane="channel:A", priority="low", age=3600)
    # Channel B: one ordinary push the seller just made.
    ledger.seed("b-", 1, lane="channel:B", priority="default", age=1)

    claimed = _claimed_ids(_store(ledger.db), 10)

    # The seller's tenant window (3) is filled with B's push among them.
    assert len(claimed) == 3
    assert "b-1" in claimed


def test_same_tier_push_of_another_channel_is_seen_behind_a_full_lane(ledger):
    # Both channels' rows in ONE priority lane: B's head must still reach
    # the plan, not only A's oldest rows.
    ledger.seed("a-", _AGED_ENDS, lane="channel:A", priority="default", age=3600)
    ledger.seed("b-", 1, lane="channel:B", priority="default", age=1)

    assert "b-1" in _claimed_ids(_store(ledger.db), 10)


def test_when_the_ends_hold_the_window_the_next_free_slot_goes_to_the_push(ledger):
    # A already holds two of the seller's three slots; one slot is free.
    ledger.seed(
        "held-", 2, lane="channel:A", priority="low", age=3600, publish_status="published"
    )
    ledger.seed("a-", _AGED_ENDS, lane="channel:A", priority="low", age=3600)
    ledger.seed("b-", 1, lane="channel:B", priority="default", age=1)
    store = _store(ledger.db)

    first = _claimed_ids(store, 1)
    # The oldest-overdue turn may take this one slot for A's oldest row...
    if first != ["b-1"]:
        ledger.db.select(
            "UPDATE queue_job_delivery SET status = 'completed', "
            "completed_at = NOW(), publish_status = 'published', "
            "published_at = NOW(), publish_lease_token = NULL, "
            "publish_lease_expires_at = NULL WHERE job_id = ANY(%s) RETURNING 1",
            [first],
        )
        # ...but the next freed slot is B's, never the 199 ends behind it.
        assert _claimed_ids(store, 1) == ["b-1"]


def test_a_stock_push_still_goes_before_another_channels_turn(ledger):
    """Lane turns never demote the seller's declared-critical stock push."""
    ledger.seed("b-", 3, lane="channel:B", priority="default", age=2)
    ledger.seed("stock-", 1, lane="channel:A", priority="critical", age=1)

    assert _claimed_ids(_store(ledger.db), 1) == ["stock-1"]
