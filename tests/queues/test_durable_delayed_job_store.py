from __future__ import annotations

import builtins
from contextlib import contextmanager
from types import SimpleNamespace

import pendulum
import pytest

from cara.commands.core.QueueWorkCommand import JobProcessor
from cara.context import Tenancy
from cara.exceptions import QueueException
from cara.queues.contracts import Queueable, ShouldQueue
from cara.queues.delay import DurableDelayedJobStore
from cara.queues.serializers import SignedJsonJobSerializer

_KEY = "delayed-queue-signing-key-" * 3
_KID = "delayed-current"


class DelayedTestJob(ShouldQueue, Queueable):
    def __init__(self, item_id: int, *, priority: str = "high"):
        super().__init__()
        self.item_id = item_id
        self.queue = "sync"
        self.priority = priority

    async def handle(self):
        return None


class CentralDelayedJob(DelayedTestJob):
    central_job = True


class _Application:
    def __init__(self, db):
        self.db = db

    def has(self, key):
        return key == "DB"

    def make(self, key):
        assert key == "DB"
        return self.db


class _DB:
    def __init__(self):
        self.events = []
        self.statements = []

    @contextmanager
    def transaction(self):
        self.events.append("begin")
        try:
            yield self
        except BaseException:
            self.events.append("rollback")
            raise
        else:
            self.events.append("commit")

    def statement(self, sql, params):
        self.statements.append((sql, params))
        return 1


class _DeliveryStore:
    def __init__(self, *, fail_source_settle=False):
        self.registered = []
        self.settled = []
        self.after_commit = []
        self.fail_source_settle = fail_source_settle

    def register(self, **kwargs):
        self.registered.append(kwargs)
        return True

    def mark_retry_scheduled(self, job_id, token, *, db=None):
        if self.fail_source_settle:
            raise QueueException("source settlement failed")
        self.settled.append((job_id, token, db))

    def publish_after_commit(self, job_id):
        self.after_commit.append(job_id)

    def publish_due(self):
        return {
            "claimed": 0,
            "published": 0,
            "retried": 0,
            "quarantined": 0,
            "settle_lost": 0,
            "expired": 0,
        }

    def backlog_metrics(self):
        return {"count": 0, "age": 0}

    @staticmethod
    def execution_timeout_for(_job):
        return 300


class _Driver:
    def __init__(self, db):
        self.application = _Application(db)
        self.options = {
            "allowed_job_prefixes": [__name__],
            "envelope_max_age_seconds": 2_678_400,
            "envelope_ttl_seconds": 604_800,
            "signing_key_id": _KID,
            "signing_keys": {_KID: _KEY},
        }

    @staticmethod
    def _priority_name(job, options):
        return str(options.get("priority") or job.priority)

    @staticmethod
    def require_canonical_queue(queue_name):
        if queue_name not in {"sync", "connector"}:
            raise QueueException("unknown canonical queue")
        return queue_name

    @staticmethod
    def _tenant_payload(job, options):
        if getattr(job, "central_job", False):
            if not Tenancy.is_central():
                raise QueueException("central scope required")
            return {"_tenant_mode": "central", "_tenant": None}
        tenant_id = options.get("tenant_id")
        if tenant_id is None:
            raise QueueException("tenant id required")
        return {"_tenant_mode": "tenant", "_tenant": tenant_id}

    @staticmethod
    def _create_job_record(_job, _job_id, _options):
        return 9


def _store(db, delivery=None):
    driver = _Driver(db)
    ledger = delivery or _DeliveryStore()
    return (
        DurableDelayedJobStore(
            driver.application,
            driver,
            driver.options,
            delivery_store=ledger,
        ),
        ledger,
    )


def test_schedule_registers_future_v2_delivery_and_settles_source_atomically():
    db = _DB()
    store, ledger = _store(db)
    available = pendulum.now("UTC").add(seconds=30)

    job_id = store.schedule(
        DelayedTestJob(17),
        available,
        {
            "attempts": 1,
            "db_job_id": 9,
            "deduplication_key": "retry:source-1:1",
            "source_delivery_job_id": "source-1",
            "source_delivery_lease_token": "lease-1",
            "tenant_id": 5,
        },
    )

    assert db.events == ["begin", "commit"]
    assert ledger.settled == [("source-1", "lease-1", db)]
    assert ledger.after_commit == [job_id]
    registered = ledger.registered[0]
    payload = registered["payload"]
    assert payload["job_id"] == job_id
    assert payload["_tenant_mode"] == "tenant"
    assert payload["_tenant"] == 5
    envelope = SignedJsonJobSerializer.inspect_envelope(
        registered["body"],
        signing_keys={_KID: _KEY},
        allow_not_before=True,
    )
    assert envelope["not_before"] == int(available.timestamp())


def test_schedule_rejects_oversized_body_before_transaction(monkeypatch):
    db = _DB()
    store, ledger = _store(db)
    job = DelayedTestJob(17)
    job.large_argument = "x" * 1024
    monkeypatch.setattr(SignedJsonJobSerializer, "MAX_PAYLOAD_BYTES", 512)

    with pytest.raises(QueueException, match="maximum wire size"):
        store.schedule(
            job,
            pendulum.now("UTC").add(seconds=30),
            {"deduplication_key": "retry:oversized:1", "tenant_id": 5},
        )

    assert db.events == ["begin", "rollback"]
    assert ledger.registered == []


def test_retry_transaction_rollback_publishes_nothing():
    db = _DB()
    ledger = _DeliveryStore(fail_source_settle=True)
    store, ledger = _store(db, ledger)

    with pytest.raises(QueueException, match="source settlement failed"):
        store.schedule(
            DelayedTestJob(17),
            pendulum.now("UTC").add(seconds=30),
            {
                "deduplication_key": "retry:source-1:1",
                "source_delivery_job_id": "source-1",
                "source_delivery_lease_token": "lease-1",
                "tenant_id": 5,
            },
        )

    assert db.events == ["begin", "rollback"]
    assert ledger.after_commit == []


class _FacadeApplication:
    def __init__(self, queue_service):
        self.queue_service = queue_service
        self.logger = SimpleNamespace(
            debug=lambda *args, **kwargs: None,
            error=lambda *args, **kwargs: None,
            info=lambda *args, **kwargs: None,
            warning=lambda *args, **kwargs: None,
        )

    def make(self, key):
        if key == "queue":
            return self.queue_service
        if key == "logger":
            return self.logger
        raise KeyError(key)


def test_worker_acks_only_after_atomic_retry_acceptance(monkeypatch):
    events = []

    class _RetryDriver:
        @staticmethod
        def _apply_retry_jitter(delay, instance):
            return delay

    queue_service = SimpleNamespace(
        driver=lambda: _RetryDriver(),
        later=lambda delay, instance, **options: events.append(("persist", options)),
    )
    monkeypatch.setattr(
        builtins,
        "app",
        lambda: _FacadeApplication(queue_service),
        raising=False,
    )
    channel = SimpleNamespace(
        basic_ack=lambda *, delivery_tag: events.append(("ack", delivery_tag))
    )
    msg = {
        "attempts": 0,
        "queue": "sync",
        "job_id": "source-77",
        "db_job_id": 9,
        "_tenant": 5,
        "_tenant_mode": "tenant",
        "_otel": {},
    }

    JobProcessor._requeue_with_delay(
        channel,
        SimpleNamespace(delivery_tag=77),
        msg,
        SimpleNamespace(retry_backoff=[5]),
        RuntimeError("retry"),
        "sync",
        "lease-77",
        SimpleNamespace(
            require_job_status_strict=lambda job_id, status: events.append(
                ("tracker", job_id, status)
            )
        ),
        9,
    )

    assert [event[0] for event in events] == ["persist", "tracker", "ack"]
    options = events[0][1]
    assert options["source_delivery_job_id"] == "source-77"
    assert options["source_delivery_lease_token"] == "lease-77"
    assert options["tenant_id"] == 5


def test_central_retry_reenters_verified_central_scope(monkeypatch):
    seen = []

    class _RetryDriver:
        @staticmethod
        def _apply_retry_jitter(delay, instance):
            return delay

    def _later(_delay, _instance, **options):
        seen.append((Tenancy.is_central(), options))

    queue_service = SimpleNamespace(driver=lambda: _RetryDriver(), later=_later)
    monkeypatch.setattr(
        builtins,
        "app",
        lambda: _FacadeApplication(queue_service),
        raising=False,
    )
    channel = SimpleNamespace(basic_ack=lambda **_kwargs: None)

    JobProcessor._requeue_with_delay(
        channel,
        SimpleNamespace(delivery_tag=1),
        {
            "attempts": 0,
            "queue": "sync",
            "job_id": "central-source",
            "db_job_id": 9,
            "_tenant": None,
            "_tenant_mode": "central",
            "_otel": {},
        },
        SimpleNamespace(retry_backoff=[5], central_job=True),
        RuntimeError("retry"),
        "sync",
        "central-lease",
        SimpleNamespace(require_job_status_strict=lambda *_args: None),
        9,
    )

    assert seen[0][0] is True
    assert "tenant_id" not in seen[0][1]


def test_worker_leaves_source_unacked_when_retry_acceptance_fails(monkeypatch):
    class _RetryDriver:
        @staticmethod
        def _apply_retry_jitter(delay, instance):
            return delay

    queue_service = SimpleNamespace(
        driver=lambda: _RetryDriver(),
        later=lambda *_args, **_kwargs: (_ for _ in ()).throw(
            QueueException("database unavailable")
        ),
    )
    monkeypatch.setattr(
        builtins,
        "app",
        lambda: _FacadeApplication(queue_service),
        raising=False,
    )
    acks = []

    with pytest.raises(QueueException, match="database unavailable"):
        JobProcessor._requeue_with_delay(
            SimpleNamespace(basic_ack=lambda *, delivery_tag: acks.append(delivery_tag)),
            SimpleNamespace(delivery_tag=88),
            {
                "attempts": 0,
                "queue": "sync",
                "job_id": "source-88",
                "_tenant": 5,
                "_tenant_mode": "tenant",
            },
            SimpleNamespace(retry_backoff=[5]),
            RuntimeError("retry"),
            "sync",
            "lease-88",
            SimpleNamespace(require_job_status_strict=lambda *_args: None),
            9,
        )

    assert acks == []
