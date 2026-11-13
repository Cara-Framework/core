"""A burst of one ``CoalescingJob`` ask becomes one delivery, never a running one's.

WooCommerce round, 2026-11-12: 500 signed ``product.updated`` deliveries for 25
products were 500 group reads (drained in 179 s). A read that has not started
answers every ask made before it starts, so the ledger folds a dispatch into
a WAITING delivery of the same key; a delivery already running may have read
before the change, so it absorbs nothing and the next ask queues one more.

The integration half runs the store's real ``register`` against a
session-local copy of a migrated ledger (``synk_scratch_*``).
"""

from __future__ import annotations

import os
import uuid
from contextlib import contextmanager
from types import SimpleNamespace

import pendulum
import pytest

from cara.queues.contracts import CoalescingJob, Queueable, ShouldQueue, UniqueJob
from cara.queues.delivery import QueueJobDeliveryStore, UniqueDeliveryConflict
from cara.queues.serializers import SignedJsonJobSerializer

_KID = "coalesce-current"
_KEY = "coalesce-signing-key----" * 3


class QueueCoalescedReadJob(ShouldQueue, Queueable, CoalescingJob):
    def __init__(self, product: str = "1") -> None:
        self.product = product

    def coalesce_id(self) -> str:
        return f"read:{self.product}"

    async def handle(self):
        return None


def test_the_contract_names_its_key_and_bounds_the_wait():
    assert QueueCoalescedReadJob("7").coalesce_id() == "read:7"
    assert 0 < QueueCoalescedReadJob.coalesce_within_seconds <= 300
    with pytest.raises(NotImplementedError):
        CoalescingJob().coalesce_id()


def _store(db=None) -> QueueJobDeliveryStore:
    class _Application:
        def has(self, key):
            return key == "DB" and db is not None

        def make(self, _key):
            return db

    store = QueueJobDeliveryStore(
        _Application(), SimpleNamespace(), {"canonical_queues": ("sync",)}
    )
    store._ledger_schema_verified = True
    return store


class QueueUniqueAndCoalescedJob(ShouldQueue, Queueable, UniqueJob, CoalescingJob):
    def unique_id(self) -> str:
        return "u:1"

    def coalesce_id(self) -> str:
        return "read:1"

    async def handle(self):
        return None


def test_a_job_cannot_be_both_unique_and_coalescing():
    store = _store()
    body, payload, envelope = _signed(QueueUniqueAndCoalescedJob(), unique_key="u:1")

    class _NoDB:
        def select_one(self, *_a, **_k):
            raise AssertionError("refused before any read")

    with pytest.raises(Exception, match="unique or coalescing"):
        store.register(
            body=body,
            payload=payload,
            envelope=envelope,
            db=_NoDB(),
            coalesce_key="read:1",
            coalesce_within_seconds=60,
        )
    assert issubclass(UniqueJob, object)


# ── the wiring: a push folds, a delayed dispatch only records ────────


class _TxDB:
    @contextmanager
    def transaction(self):
        yield self

    def statement(self, _sql, _params=None):
        return 1


class _Ledger:
    def __init__(self) -> None:
        self.registered: list[dict] = []

    def _db(self):
        return _TxDB()

    def register(self, **kwargs):
        self.registered.append(kwargs)
        return True

    def mark_retry_scheduled(self, *_a, **_k):
        return None

    def publish_after_commit(self, _job_id):
        return None

    @staticmethod
    def execution_timeout_for(_job):
        return 300


class _Driver:
    options = {
        "allowed_job_prefixes": [__name__],
        "envelope_max_age_seconds": 2_678_400,
        "envelope_ttl_seconds": 604_800,
        "signing_key_id": _KID,
        "signing_keys": {_KID: _KEY},
    }

    def __init__(self) -> None:
        self._delivery_store = _Ledger()

    @staticmethod
    def _priority_name(_job, _options):
        return "default"

    @staticmethod
    def require_canonical_queue(queue_name):
        return queue_name or "sync"

    @staticmethod
    def _tenant_payload(_job, _options):
        return {"_tenant_mode": "tenant", "_tenant": _TENANT}

    @staticmethod
    def _create_job_record(_job, _job_id, _options):
        return 9

    def _serialize_payload(self, payload, opts):
        from cara.queues.drivers import _AMQPDelivery

        return _AMQPDelivery._amqp_delivery_serialize_payload(self, payload, opts)


def test_a_push_hands_its_key_and_its_wait_to_the_ledger():
    from cara.queues.drivers import _AMQPDelivery

    driver = _Driver()
    _AMQPDelivery._amqp_delivery_register_immediate_delivery(
        driver,
        job=QueueCoalescedReadJob("42"),
        job_id=str(uuid.uuid4()),
        merged_opts=dict(_Driver.options),
        job_opts={**_Driver.options, "queue": "sync"},
    )

    (registered,) = driver._delivery_store.registered
    assert registered["coalesce_key"] == "read:42"
    assert registered["coalesce_within_seconds"] == 60


class QueuePlainJob(ShouldQueue, Queueable):
    async def handle(self):
        return None


def test_a_plain_push_has_no_key():
    from cara.queues.drivers import _AMQPDelivery

    driver = _Driver()
    _AMQPDelivery._amqp_delivery_register_immediate_delivery(
        driver,
        job=QueuePlainJob(),
        job_id=str(uuid.uuid4()),
        merged_opts=dict(_Driver.options),
        job_opts={**_Driver.options, "queue": "sync"},
    )

    (registered,) = driver._delivery_store.registered
    assert registered["coalesce_key"] is None
    assert registered["coalesce_within_seconds"] is None


def test_a_folded_push_returns_the_waiting_deliverys_id():
    from cara.queues.drivers import _AMQPDelivery

    driver = _Driver()

    def folded(**_kwargs):
        raise UniqueDeliveryConflict("waiting-id")

    driver._delivery_store.register = folded
    accepted = _AMQPDelivery._amqp_delivery_register_immediate_delivery(
        driver,
        job=QueueCoalescedReadJob(),
        job_id=str(uuid.uuid4()),
        merged_opts=dict(_Driver.options),
        job_opts={**_Driver.options, "queue": "sync"},
    )

    assert accepted == "waiting-id"


def test_a_delayed_dispatch_records_its_key_without_a_wait():
    from cara.queues.delay import DurableDelayedJobStore

    class _App:
        def has(self, key):
            return key == "DB"

        def make(self, _key):
            return _TxDB()

    ledger = _Ledger()
    store = DurableDelayedJobStore(
        _App(), _Driver(), _Driver.options, delivery_store=ledger
    )
    store.schedule(
        QueueCoalescedReadJob("9"),
        pendulum.now("UTC").add(seconds=5),
        {"queue": "sync", "db_job_id": 9},
    )

    (registered,) = ledger.registered
    assert registered["coalesce_key"] == "read:9"
    assert registered.get("coalesce_within_seconds") is None


# ── real Postgres ─────────────────────────────────────────────────────

_integration = pytest.mark.skipif(
    os.getenv("CARA_RUN_INTEGRATION") != "1"
    or not os.getenv("DB_DATABASE", "").startswith("synk_scratch_"),
    reason="set CARA_RUN_INTEGRATION=1 and a migrated DB_DATABASE named synk_scratch_*",
)

_TENANT = 9_400_001


def _signed(job=None, *, delay: int = 0, unique_key: str | None = None):
    now = pendulum.now("UTC")
    job_id = str(uuid.uuid4())
    body = SignedJsonJobSerializer.serialize(
        {
            "obj": job if job is not None else QueueCoalescedReadJob(),
            "args": (),
            "callback": "handle",
            "created": now.to_iso8601_string(),
            "job_id": job_id,
            "db_job_id": 1,
            "timeout_seconds": 300,
            "attempts": 0,
            "throttle_attempts": 0,
            "_otel": {},
            "_tenant_mode": "tenant",
            "_tenant": _TENANT,
            "queue": "sync",
            "priority": "default",
            "dispatched_at": now.to_iso8601_string(),
            "replay_of": None,
            "unique_key": unique_key,
        },
        signing_key_id=_KID,
        signing_keys={_KID: _KEY},
        allowed_prefixes=[__name__],
        issued_at=now,
        not_before=now.add(seconds=delay),
        ttl_seconds=604_800,
        max_age_seconds=2_678_400,
    )
    envelope = SignedJsonJobSerializer.inspect_envelope(
        body,
        signing_keys={_KID: _KEY},
        now=now,
        clock_skew_seconds=30,
        max_age_seconds=2_678_400,
        allow_not_before=True,
    )
    return body, envelope["payload"], envelope


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
        except BaseException:
            with self._cursor() as cursor:
                cursor.execute("ROLLBACK")
            raise
        with self._cursor() as cursor:
            cursor.execute("COMMIT")

    def select_one(self, sql, params=None):
        with self._cursor() as cursor:
            cursor.execute(sql, params)
            return cursor.fetchone() if cursor.description else None

    def select(self, sql, params=None):
        with self._cursor() as cursor:
            cursor.execute(sql, params)
            return cursor.fetchall()

    def statement(self, sql, params=None):
        with self._cursor() as cursor:
            cursor.execute(sql, params)
            return cursor.rowcount


@pytest.fixture
def ledger():
    import psycopg2

    connection = psycopg2.connect(dbname=os.environ["DB_DATABASE"])
    connection.autocommit = True
    with connection.cursor() as cursor:
        # pg_temp leads the search path: the store's unqualified table name
        # resolves to this session's copy.
        cursor.execute(
            "CREATE TEMP TABLE queue_job_delivery "
            "(LIKE public.queue_job_delivery INCLUDING ALL)"
        )
    db = _PostgresDB(connection)
    store = _store(db)
    try:
        yield store, db
    finally:
        with connection.cursor() as cursor:
            cursor.execute("DROP TABLE IF EXISTS pg_temp.queue_job_delivery")
        connection.close()


def _dispatch(store, db, *, delay: int = 0, within: int | None = 60) -> str:
    """One ``push`` of the coalescing read: the delivery id the caller gets."""
    body, payload, envelope = _signed(delay=delay)
    try:
        with db.transaction():
            store.register(
                body=body,
                payload=payload,
                envelope=envelope,
                db=db,
                coalesce_key=QueueCoalescedReadJob().coalesce_id(),
                coalesce_within_seconds=within,
            )
    except UniqueDeliveryConflict as conflict:
        return conflict.job_id
    return str(payload["job_id"])


def _rows(db) -> list[dict]:
    return db.select("SELECT job_id, status, coalesce_key FROM queue_job_delivery")


@_integration
def test_a_burst_of_500_asks_is_one_waiting_delivery(ledger):
    store, db = ledger

    ids = {_dispatch(store, db) for _ in range(500)}

    rows = _rows(db)
    assert len(rows) == 1 and ids == {rows[0]["job_id"]}
    assert rows[0]["coalesce_key"] == "read:1"


@_integration
def test_a_running_delivery_absorbs_nothing_but_the_next_one_absorbs_the_rest(ledger):
    store, db = ledger
    first = _dispatch(store, db)
    db.statement(
        "UPDATE queue_job_delivery SET status = 'processing', attempts = 1, "
        "lease_token = 'lease', lease_expires_at = NOW() + INTERVAL '1 minute' "
        "WHERE job_id = %s",
        [first],
    )

    after = {_dispatch(store, db) for _ in range(200)}

    (second,) = after
    assert second != first
    assert len(_rows(db)) == 2


@_integration
def test_a_waiting_delivery_far_in_the_future_never_swallows_a_fresh_ask(ledger):
    store, db = ledger
    later = _dispatch(store, db, delay=3_600, within=None)  # a delayed retry

    now = _dispatch(store, db)

    assert now != later
    assert _dispatch(store, db) == now
    assert len(_rows(db)) == 2


@_integration
def test_a_delayed_dispatch_records_its_key_but_never_folds_itself(ledger):
    store, db = ledger
    waiting = _dispatch(store, db)

    retry = _dispatch(store, db, delay=5, within=None)

    assert retry != waiting
    assert {r["coalesce_key"] for r in _rows(db)} == {"read:1"}
    assert _dispatch(store, db) == waiting  # due first: the earliest wins
