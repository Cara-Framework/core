"""The relay gauge snapshot never walks the retained ledger.

``delivery_metrics`` was one ``COUNT(*) FILTER`` pass over the whole
``queue_job_delivery`` table. Live on 2026-11-12 the ledger held 707k rows
(1.5 GB of heap) and that sequential scan took 0.6 s warm and up to 12 s
cold, every 15 s, in the relay and in the hooks runner, inline in the relay
loop. Every family now reads a scope an index answers, and those scopes are
implied by the table's CHECK constraints, so each count means what it meant.

The unit tests pin the shape. The Postgres tests (``CARA_RUN_INTEGRATION=1``
and a ``DB_DATABASE`` named ``synk_scratch_*`` migrated from the models)
prove it on real plans and real counts: a session-local copy of the ledger
(``LIKE ... INCLUDING ALL`` — every CHECK and index, no foreign key) is
seeded with every state, then

* with sequential scans priced out, the plan has no ``Seq Scan`` on it — an
  index can serve every scope (the old whole-table pass cannot be);
* the snapshot equals the old whole-table aggregate, column for column.
"""

from __future__ import annotations

import os
import re
import uuid
from types import SimpleNamespace

import pytest

from cara.models.QueueJobDelivery import QueueJobDelivery
from cara.queues.delivery import QueueJobDeliveryStore


class _Application:
    def __init__(self, db):
        self.db = db

    def has(self, key):
        return key == "DB"

    def make(self, key):
        assert key == "DB"
        return self.db


class _CapturingDB:
    def __init__(self):
        self.aggregate: tuple[str, list] | None = None

    def select_one(self, sql, params):
        self.aggregate = (sql, params)
        return {}

    def select(self, _sql, _params):
        return []


def _store(db) -> QueueJobDeliveryStore:
    return QueueJobDeliveryStore(
        _Application(db), SimpleNamespace(), {"canonical_queues": ("sync",)}
    )


def _aggregate_sql() -> tuple[str, list]:
    db = _CapturingDB()
    _store(db).delivery_metrics()
    assert db.aggregate is not None
    return db.aggregate


def _declared_index(name: str) -> str:
    for entry in QueueJobDelivery.__indexes__:
        if entry["name"] == name:
            return entry["up"]
    raise AssertionError(f"{name} is not declared on QueueJobDelivery")


def test_every_ledger_read_in_the_snapshot_carries_a_scope():
    sql, params = _aggregate_sql()
    assert sql.count("%s") == len(params)
    reads = re.findall(r"FROM queue_job_delivery(?: AS \w+)?\s*(\S*)", sql)
    assert reads, sql
    # A bare ``FROM queue_job_delivery`` that ends the statement, or is
    # followed by a comma or a closing paren, is an unscoped pass over the
    # whole retained ledger.
    assert all(token == "WHERE" for token in reads), reads


def _outbox_health_sql() -> tuple[str, list]:
    """The watchdog's whole-outbox aggregate (the stall attribution that
    follows it reads aged queues only)."""
    from cara.queues.delivery import _DeliveryRecovery

    db = _CapturingDB()
    _DeliveryRecovery._delivery_outbox_health_metrics(_store(db))
    assert db.aggregate is not None
    return db.aggregate


def test_every_ledger_read_in_the_watchdog_carries_a_scope():
    """The scheduler's outbox watchdog read the whole ledger every minute."""
    sql, params = _outbox_health_sql()
    assert sql.count("%s") == len(params)
    reads = re.findall(r"FROM queue_job_delivery(?: AS \w+)?\s*(\S*)", sql)
    assert reads and all(token == "WHERE" for token in reads), reads
    assert "queue_job_delivery_last_publish_idx" in {
        entry["name"] for entry in QueueJobDelivery.__indexes__
    }


def test_publish_quarantine_reads_the_partial_index_predicate_verbatim():
    sql, _params = _aggregate_sql()
    predicate = QueueJobDeliveryStore._PUBLISH_QUARANTINE_FILTER
    assert predicate in sql
    assert "LIKE" not in sql
    assert predicate in _declared_index("queue_job_delivery_publish_quarantine_idx")


def test_failures_awaiting_replay_have_an_index_only_scope():
    up = _declared_index("queue_job_delivery_awaiting_replay_idx")
    assert "(status, job_id)" in up
    assert "WHERE status IN ('dead_lettered','expired')" in up


def test_no_ledger_index_is_keyed_on_expiry_across_settled_history():
    """The expiry sweep runs every relay cycle and reads open rows only. An
    index keyed on ``expires_at`` over the whole table makes each probe walk
    every retained row whose envelope has expired (317k entries live)."""
    keyed_on_expiry = [
        entry["up"]
        for entry in QueueJobDelivery.__indexes__
        if re.search(r"ON queue_job_delivery \(expires_at\b", entry["up"])
    ]
    assert keyed_on_expiry
    assert all(
        "WHERE status IN ('pending','processing')" in up for up in keyed_on_expiry
    ), keyed_on_expiry


# ── real Postgres ─────────────────────────────────────────────────────

_integration = pytest.mark.skipif(
    os.getenv("CARA_RUN_INTEGRATION") != "1"
    or not os.getenv("DB_DATABASE", "").startswith("synk_scratch_"),
    reason="set CARA_RUN_INTEGRATION=1 and a migrated DB_DATABASE named synk_scratch_*",
)

#: The whole-table aggregate this file replaced, kept as the reference the
#: scoped snapshot must equal.
_REFERENCE = (
    "SELECT "
    "COUNT(*) FILTER (WHERE status = 'pending') AS pending, "
    "COUNT(*) FILTER (WHERE status = 'processing') AS processing, "
    "COUNT(*) FILTER (WHERE status = 'completed') AS completed, "
    "COUNT(*) FILTER (WHERE status = 'retry_scheduled') AS retry_scheduled, "
    "COUNT(*) FILTER (WHERE status = 'dead_lettered' AND NOT EXISTS ("
    "SELECT 1 FROM queue_job_delivery c WHERE c.replay_of = q.job_id)) "
    "AS dead_lettered, "
    "COUNT(*) FILTER (WHERE status = 'expired' AND NOT EXISTS ("
    "SELECT 1 FROM queue_job_delivery c WHERE c.replay_of = q.job_id)) "
    "AS expired, "
    "COUNT(*) FILTER (WHERE status = 'pending' AND publish_status = 'pending') "
    "AS publish_pending, "
    "COUNT(*) FILTER (WHERE publish_status = 'processing') AS publish_processing, "
    "COUNT(*) FILTER (WHERE status = 'dead_lettered' AND "
    "terminal_reason LIKE 'publish_envelope_invalid:%') AS publish_quarantined, "
    "COUNT(*) FILTER (WHERE publish_status = 'processing' AND "
    "publish_lease_expires_at <= NOW()) AS stale_publish, "
    "COUNT(*) FILTER (WHERE status = 'processing' AND "
    "lease_expires_at <= NOW()) AS stale_execution, "
    "COUNT(*) FILTER (WHERE status IN ('completed','dead_lettered','expired') "
    "AND post_hooks_completed_at IS NULL AND post_hooks_lease_token IS NULL) "
    "AS hook_pending, "
    "COUNT(*) FILTER (WHERE status IN ('completed','dead_lettered','expired') "
    "AND post_hooks_completed_at IS NULL AND post_hooks_lease_token IS NOT NULL "
    "AND post_hooks_lease_expires_at > NOW()) AS hook_processing, "
    "COUNT(*) FILTER (WHERE status IN ('completed','dead_lettered','expired') "
    "AND post_hooks_completed_at IS NULL AND post_hooks_lease_token IS NOT NULL "
    "AND post_hooks_lease_expires_at <= NOW()) AS hook_stale, "
    "COUNT(*) FILTER (WHERE status IN ('completed','dead_lettered','expired') "
    "AND post_hooks_completed_at IS NULL AND post_hooks_last_error IS NOT NULL) "
    "AS hook_failed, "
    "COUNT(*) FILTER (WHERE status IN ('completed','dead_lettered','expired') "
    "AND post_hooks_quarantined_at IS NOT NULL) AS hook_quarantined, "
    "COUNT(*) FILTER (WHERE status = 'pending' AND publish_status != 'published' "
    "AND available_at <= NOW() AND priority = 'high') AS high_pending "
    "FROM queue_job_delivery q"
)


class _PostgresDB:
    def __init__(self, connection):
        self.connection = connection

    def _cursor(self):
        import psycopg2.extras

        return self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    def select_one(self, sql, params=None):
        with self._cursor() as cursor:
            cursor.execute(sql, params)
            return cursor.fetchone()

    def select(self, sql, params=None):
        with self._cursor() as cursor:
            cursor.execute(sql, params)
            return cursor.fetchall()


#: (status, publish_status, extra columns) — every state the CHECKs allow
#: that a gauge distinguishes.
_LEASE = "md5(random()::text)"
_STATES = [
    ("pending", "pending", {}),
    (
        "pending",
        "processing",
        {
            "publish_lease_token": _LEASE,
            "publish_lease_expires_at": "NOW() - INTERVAL '1 minute'",
        },
    ),
    (
        "pending",
        "processing",
        {
            "publish_lease_token": _LEASE,
            "publish_lease_expires_at": "NOW() + INTERVAL '5 minutes'",
        },
    ),
    ("pending", "published", {"published_at": "NOW()"}),
    (
        "processing",
        "published",
        {
            "published_at": "NOW()",
            "lease_token": _LEASE,
            "lease_expires_at": "NOW() - INTERVAL '1 minute'",
        },
    ),
    (
        "processing",
        "published",
        {
            "published_at": "NOW()",
            "lease_token": _LEASE,
            "lease_expires_at": "NOW() + INTERVAL '5 minutes'",
        },
    ),
    (
        "completed",
        "published",
        {
            "published_at": "NOW()",
            "completed_at": "NOW()",
            "post_hooks_completed_at": "NOW()",
        },
    ),
    ("completed", "published", {"published_at": "NOW()", "completed_at": "NOW()"}),
    (
        "completed",
        "published",
        {
            "published_at": "NOW()",
            "completed_at": "NOW()",
            "post_hooks_lease_token": _LEASE,
            "post_hooks_lease_expires_at": "NOW() + INTERVAL '5 minutes'",
        },
    ),
    (
        "completed",
        "published",
        {
            "published_at": "NOW()",
            "completed_at": "NOW()",
            "post_hooks_lease_token": _LEASE,
            "post_hooks_lease_expires_at": "NOW() - INTERVAL '5 minutes'",
            "post_hooks_last_error": "'boom'",
        },
    ),
    (
        "retry_scheduled",
        "published",
        {
            "published_at": "NOW()",
            "completed_at": "NOW()",
            "post_hooks_completed_at": "NOW()",
        },
    ),
    (
        "dead_lettered",
        "published",
        {
            "published_at": "NOW()",
            "completed_at": "NOW()",
            "post_hooks_quarantined_at": "NOW()",
            "post_hooks_last_error": "'quarantined'",
        },
    ),
    (
        "dead_lettered",
        "failed",
        {
            "completed_at": "NOW()",
            "terminal_reason": "'publish_envelope_invalid:bad sig'",
            "post_hooks_completed_at": "NOW()",
        },
    ),
    (
        "expired",
        "published",
        {
            "published_at": "NOW()",
            "completed_at": "NOW()",
            "post_hooks_completed_at": "NOW()",
        },
    ),
]


@pytest.fixture
def ledger():
    import psycopg2

    connection = psycopg2.connect(dbname=os.environ["DB_DATABASE"])
    connection.autocommit = True
    with connection.cursor() as cursor:
        # pg_temp leads the search path, so the store's unqualified table
        # name resolves to this session's copy; foreign keys are not copied.
        cursor.execute(
            "CREATE TEMP TABLE queue_job_delivery "
            "(LIKE public.queue_job_delivery INCLUDING ALL)"
        )
        for index, (status, publish, extra) in enumerate(_STATES * 40):
            columns = {
                "job_id": f"'{uuid.uuid4().hex}'",
                "db_job_id": str(index + 1),
                "payload_sha256": "repeat('a', 64)",
                "signed_envelope": "'{}'::jsonb",
                "tenant_mode": "'central'",
                "queue": "'sync'",
                "priority": "'high'" if index % 2 else "'default'",
                "status": f"'{status}'",
                "expires_at": "NOW() + INTERVAL '1 day'",
                "available_at": "NOW() - INTERVAL '1 minute'",
                "publish_status": f"'{publish}'",
                "publish_retry_at": "NOW()",
                "created_at": "NOW()",
                "updated_at": "NOW()",
                **extra,
            }
            cursor.execute(
                f"INSERT INTO queue_job_delivery ({', '.join(columns)}) "
                f"VALUES ({', '.join(columns.values())})"
            )
        # Half the dead letters and expiries were replayed: the replay child
        # names its source, so the source no longer awaits replay.
        cursor.execute(
            "SELECT job_id FROM queue_job_delivery "
            "WHERE status IN ('dead_lettered','expired') ORDER BY job_id"
        )
        sources = [row[0] for row in cursor.fetchall()][::2]
        for offset, source in enumerate(sources):
            cursor.execute(
                "INSERT INTO queue_job_delivery (job_id, db_job_id, replay_of, "
                "replay_requested_by, replay_reason, payload_sha256, "
                "signed_envelope, tenant_mode, queue, priority, status, "
                "expires_at, available_at, publish_status, publish_retry_at, "
                "completed_at, post_hooks_completed_at, published_at, "
                "created_at, updated_at) VALUES (%s, %s, %s, 'operator', "
                "'replay', repeat('b', 64), '{}'::jsonb, 'central', 'sync', "
                "'default', 'completed', NOW() + INTERVAL '1 day', NOW(), "
                "'published', NOW(), NOW(), NOW(), NOW(), NOW(), NOW())",
                [uuid.uuid4().hex, 100000 + offset, source],
            )
        cursor.execute("ANALYZE queue_job_delivery")
    try:
        yield connection
    finally:
        with connection.cursor() as cursor:
            cursor.execute("DROP TABLE IF EXISTS pg_temp.queue_job_delivery")
        connection.close()


_WATCHDOG_REFERENCE = (
    "SELECT "
    "COUNT(*) FILTER (WHERE status = 'pending' AND publish_status != 'published' "
    "AND available_at <= NOW()) AS due_pending, "
    "COALESCE(EXTRACT(EPOCH FROM (NOW() - MAX(published_at))), -1) "
    "AS last_publish_age, "
    "COUNT(*) FILTER (WHERE status IN ('completed','dead_lettered','expired') "
    "AND post_hooks_completed_at IS NULL AND post_hooks_quarantined_at IS NULL "
    "AND (post_hooks_lease_token IS NULL OR post_hooks_lease_expires_at IS NULL "
    "OR post_hooks_lease_expires_at <= NOW())) AS hook_due_pending "
    "FROM queue_job_delivery"
)


@pytest.mark.parametrize("read", [_aggregate_sql, _outbox_health_sql])
@_integration
def test_an_index_serves_every_scope_of_the_snapshot(ledger, read):
    sql, params = read()
    with ledger.cursor() as cursor:
        cursor.execute("SET enable_seqscan = off")
        cursor.execute("EXPLAIN " + sql, params)
        plan = "\n".join(row[0] for row in cursor.fetchall())
        cursor.execute("RESET enable_seqscan")
    assert "Seq Scan" not in plan, plan


@_integration
def test_the_scoped_snapshot_equals_the_whole_table_aggregate(ledger):
    store = _store(_PostgresDB(ledger))
    snapshot = store.delivery_metrics()
    reference = _PostgresDB(ledger).select_one(_REFERENCE)

    assert snapshot["statuses"] == {
        status: reference[status]
        for status in (
            "pending",
            "processing",
            "completed",
            "retry_scheduled",
            "dead_lettered",
            "expired",
        )
    }
    assert snapshot["publish_pending"] == reference["publish_pending"]
    assert snapshot["publish_processing"] == reference["publish_processing"]
    assert snapshot["publish_quarantined"] == reference["publish_quarantined"]
    assert snapshot["stale_leases"] == {
        "publish": reference["stale_publish"],
        "execution": reference["stale_execution"],
    }
    assert snapshot["hooks"] == {
        "pending": reference["hook_pending"],
        "processing": reference["hook_processing"],
        "stale": reference["hook_stale"],
        "failed": reference["hook_failed"],
        "quarantined": reference["hook_quarantined"],
    }
    assert snapshot["priority_backlog"]["high"]["pending"] == reference["high_pending"]
    # Every family is non-empty, so equality is not 0 == 0.
    assert all(
        reference[key] > 0
        for key in (
            "pending",
            "processing",
            "completed",
            "retry_scheduled",
            "dead_lettered",
            "expired",
            "publish_processing",
            "publish_quarantined",
            "stale_publish",
            "stale_execution",
            "hook_pending",
            "hook_processing",
            "hook_stale",
            "hook_failed",
            "hook_quarantined",
            "high_pending",
        )
    ), reference


@_integration
def test_the_scoped_watchdog_equals_the_whole_table_aggregate(ledger):
    from cara.queues.delivery import _DeliveryRecovery

    health = _DeliveryRecovery._delivery_outbox_health_metrics(
        _store(_PostgresDB(ledger))
    )
    reference = _PostgresDB(ledger).select_one(_WATCHDOG_REFERENCE)

    assert reference["due_pending"] > 0 and reference["hook_due_pending"] > 0
    assert health["due_pending"] == reference["due_pending"]
    assert health["hook_due_pending"] == reference["hook_due_pending"]
    assert health["last_publish_age"] == pytest.approx(
        float(reference["last_publish_age"]), abs=1.0
    )
