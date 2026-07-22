from contextlib import contextmanager
from types import SimpleNamespace

import pendulum
import pytest

from cara.context import Tenancy
from cara.exceptions import QueueException
from cara.queues.contracts import Queueable, ShouldQueue
from cara.queues.drivers.AMQPDriver import AMQPDriver
from cara.queues.serializers import SignedJsonJobSerializer

_KEY = "amqp-priority-test-key-" * 3
_KID = "priority-current"
_JOB_ID = "22222222-2222-4222-8222-222222222222"
_LEVELS = {
    "critical": 4,
    "high": 3,
    "default": 2,
    "low": 1,
}


class PriorityJob(ShouldQueue, Queueable):
    def __init__(self, item_id: int, *, priority: str = "default"):
        self.item_id = int(item_id)
        super().__init__()
        self.queue = "sync"
        self.priority = priority

    async def handle(self):
        return None


class _Channel:
    is_open = True

    def __init__(self):
        self.declarations = []
        self.publishes = []

    def queue_declare(self, **kwargs):
        self.declarations.append(kwargs)

    def basic_publish(self, **kwargs):
        self.publishes.append(kwargs)
        return True


class _DB:
    @contextmanager
    def transaction(self):
        yield self


class _Application:
    def __init__(self):
        self.db = _DB()

    def has(self, key):
        return key == "DB"

    def make(self, key):
        assert key == "DB"
        return self.db


def _options():
    return {
        "allowed_job_prefixes": ("tests.queues",),
        "canonical_queues": ("connector", "sync"),
        "connection_options": {},
        "exchange": "",
        "max_length": 100000,
        "max_length_bytes": 1073741824,
        "max_priority": 4,
        "priority_levels": _LEVELS,
        "serializer": "json",
        "signing_key_id": _KID,
        "signing_keys": {_KID: _KEY},
        "tz": "UTC",
    }


def _payload(job):
    return {
        "obj": job,
        "args": (),
        "callback": "handle",
        "created": "2026-07-16T00:00:00Z",
        "job_id": _JOB_ID,
        "db_job_id": 9,
        "timeout_seconds": 300,
        "attempts": 0,
        "_otel": {},
        "_tenant": 5,
        "_tenant_mode": "tenant",
        "queue": "sync",
        "priority": job.priority,
        "dispatched_at": "2026-07-16T00:00:00Z",
        "replay_of": None,
    }


def test_publish_uses_reconciled_quorum_queue_and_high_message_priority():
    driver = AMQPDriver(SimpleNamespace(), _options())
    channel = _Channel()
    driver.channel = channel
    driver._acquire_thread_connection = lambda *_args: None
    driver._return_thread_connection = lambda *_args: None
    job = PriorityJob(1, priority="high")
    options = {**_options(), "queue": "sync"}
    body = driver._serialize_payload(_payload(job), options)
    payload = SignedJsonJobSerializer.inspect(
        body,
        signing_keys=options["signing_keys"],
    )

    driver._publish_registered_envelope(
        body,
        payload,
        capability=driver.delivery_store,
    )

    assert channel.declarations == []
    assert driver.canonical_queue_arguments("sync") == {
        "x-delivery-limit": 20,
        "x-dead-letter-exchange": "dead.letter.dlx",
        "x-dead-letter-routing-key": "dead.sync",
        "x-dead-letter-strategy": "at-least-once",
        "x-max-length": 100000,
        "x-max-length-bytes": 1073741824,
        "x-overflow": "reject-publish",
        "x-queue-type": "quorum",
    }
    publish = channel.publishes[0]
    assert publish["routing_key"] == "sync"
    assert publish["properties"].content_type == "application/json"
    assert publish["properties"].priority == 3
    assert publish["properties"].type == "cara.job.v3"


def test_quorum_priority_configuration_rejects_values_above_native_limit():
    options = {**_options(), "max_priority": 32}
    driver = AMQPDriver(SimpleNamespace(), options)

    with pytest.raises(
        QueueException,
        match="max_priority must be between 1 and 31",
    ):
        driver.canonical_queue_arguments("sync")


def test_direct_push_prefers_job_stage_over_driver_default(monkeypatch):
    driver = AMQPDriver(_Application(), _options())
    captured = []
    monkeypatch.setattr(driver, "_create_job_record", lambda *_args: 9)
    monkeypatch.setattr(
        driver.delivery_store,
        "register",
        lambda **kwargs: captured.append(kwargs),
    )
    monkeypatch.setattr(driver.delivery_store, "publish_after_commit", lambda _job_id: None)

    with Tenancy.as_tenant(5):
        driver.push(PriorityJob(2), options={})

    payload = captured[0]["payload"]
    assert payload["queue"] == "sync"
    assert payload["priority"] == "default"


def test_default_and_high_share_one_queue_but_have_different_properties():
    driver = AMQPDriver(_Application(), _options())
    assert driver._message_priority(PriorityJob(1), _options()) == 2
    assert (
        driver._message_priority(
            PriorityJob(2, priority="high"),
            _options(),
        )
        == 3
    )


def test_direct_publish_rejects_oversized_body_before_queue_declare(
    monkeypatch,
):
    driver = AMQPDriver(_Application(), _options())
    channel = _Channel()
    driver.channel = channel
    job = PriorityJob(1)
    job.large_argument = "x" * 1024
    monkeypatch.setattr(SignedJsonJobSerializer, "MAX_PAYLOAD_BYTES", 512)
    monkeypatch.setattr(driver, "_create_job_record", lambda *_args: 9)
    monkeypatch.setattr(
        driver.delivery_store,
        "register",
        lambda **_kwargs: (_ for _ in ()).throw(
            AssertionError("oversized body reached ledger")
        ),
    )

    with Tenancy.as_tenant(5), pytest.raises(
        QueueException,
        match="maximum wire size",
    ):
        driver.push(job, options={"queue": "sync"})

    assert channel.declarations == []
    assert channel.publishes == []


def test_direct_dispatch_rejects_missing_or_unknown_queue_before_persistence(
    monkeypatch,
):
    driver = AMQPDriver(_Application(), _options())
    monkeypatch.setattr(
        driver,
        "_create_job_record",
        lambda *_args: (_ for _ in ()).throw(
            AssertionError("invalid queue reached persistence")
        ),
    )

    with Tenancy.as_tenant(5), pytest.raises(
        QueueException,
        match="explicit canonical queue",
    ):
        driver.push(SimpleNamespace(), options={})

    job = PriorityJob(1)
    job.queue = "default"
    with Tenancy.as_tenant(5), pytest.raises(
        QueueException,
        match="not consumed",
    ):
        driver.push(job, options={})


def test_due_now_retry_stays_on_atomic_delayed_ledger(monkeypatch):
    driver = AMQPDriver(_Application(), _options())
    scheduled: list[tuple] = []
    monkeypatch.setattr(
        driver._delayed_store,
        "schedule",
        lambda *args: scheduled.append(args) or "retry-job-id",
    )
    monkeypatch.setattr(
        driver,
        "push",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("source retry bypassed atomic delayed ledger")
        ),
    )
    options = {
        "queue": "sync",
        "source_delivery_job_id": "source-job-id",
        "source_delivery_lease_token": "source-lease",
    }

    result = driver.schedule(
        PriorityJob(1),
        pendulum.now("UTC"),
        options,
    )

    assert result == "retry-job-id"
    assert scheduled[0][2] is options
