from __future__ import annotations

import builtins
import importlib
from contextlib import contextmanager
from types import SimpleNamespace

import pendulum
import pytest

from cara.queues.contracts import Queueable, ShouldQueue
from cara.queues.delivery import DeliveryClaim, QueueJobDeliveryStore
from cara.queues.serializers import SignedJsonJobSerializer
from cara.queues.tracking import JobTracker

_KID = "crash-recovery-current"
_KEY = "crash-recovery-signing-key-" * 3
_JOB_ID = "44444444-4444-4444-8444-444444444444"


class CrashRecoveryJob(ShouldQueue, Queueable):
    queue = "sync"

    def __init__(self, item_id: int):
        super().__init__()
        self.item_id = item_id

    async def handle(self):
        return None


class _CrashDB:
    def __init__(self, envelope: bytes, payload: dict):
        self.row = {
            "job_id": _JOB_ID,
            "db_job_id": 91,
            "payload_sha256": (
                SignedJsonJobSerializer.canonical_envelope_sha256(envelope)
            ),
            "signed_envelope": envelope,
            "tenant_mode": "tenant",
            "tenant_id": 5,
            "queue": "sync",
            "priority": "default",
            "status": "processing",
            "publish_status": "published",
            "publish_attempts": 0,
            "lease_token": "crashed-worker-lease",
            "lease_expires_at": pendulum.now("UTC").subtract(seconds=1),
            "expires_at": pendulum.now("UTC").add(days=1),
            "available_at": pendulum.now("UTC").subtract(seconds=5),
            "terminal_reason": None,
        }
        self.payload = payload
        self.tracker_status = "processing"
        self.events: list[str] = []

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

    def select(self, sql, params):
        if "WHERE delivery.status = %s" in sql:
            if (
                self.row["status"] == "processing"
                and self.row["lease_expires_at"] <= pendulum.now("UTC")
            ):
                return [
                    {
                        "job_id": self.row["job_id"],
                        "db_job_id": self.row["db_job_id"],
                        "lease_token": self.row["lease_token"],
                        "tracker_status": self.tracker_status,
                    }
                ]
            return []
        if "WHERE delivery.expires_at <= %s" in sql:
            return []
        if sql.startswith("SELECT job_id FROM queue_job_delivery"):
            if (
                self.row["status"] == "pending"
                and self.row["publish_status"] == "pending"
            ):
                return [{"job_id": self.row["job_id"]}]
            return []
        raise AssertionError(f"Unexpected SELECT: {sql}")

    def select_one(self, sql, params=None):
        if sql.startswith("SELECT pg_advisory_xact_lock"):
            return {"locked": None}
        if sql.startswith("WITH settings AS"):
            if (
                self.row["status"] == "pending"
                and self.row["publish_status"] == "pending"
            ):
                self.row["publish_status"] = "processing"
                self.row["publish_lease_token"] = params[4]
                self.row["publish_lease_expires_at"] = params[5]
                return dict(self.row)
            return None
        if sql.startswith(
            "UPDATE queue_job_delivery SET publish_status = %s"
        ):
            if (
                self.row["status"] == "pending"
                and self.row["publish_status"] == "pending"
            ):
                self.row["publish_status"] = "processing"
                self.row["publish_lease_token"] = params[1]
                return dict(self.row)
            return None
        if "WHERE job_id = %s FOR UPDATE" in sql:
            return dict(self.row)
        if sql.startswith(
            "SELECT publish_status, published_at FROM queue_job_delivery"
        ):
            return {
                "publish_status": self.row["publish_status"],
                "published_at": self.row.get("published_at"),
            }
        raise AssertionError(f"Unexpected SELECT ONE: {sql}")

    def statement(self, sql, params):
        if "SET status = %s, publish_status = %s" in sql:
            self.row.update(
                {
                    "status": params[0],
                    "publish_status": params[1],
                    "lease_token": None,
                    "lease_expires_at": None,
                }
            )
            return 1
        if sql.startswith("UPDATE job SET status = %s"):
            self.tracker_status = params[0]
            return 1
        if "SET publish_status = %s, published_at = %s" in sql:
            self.row["publish_status"] = params[0]
            self.row["published_at"] = params[1]
            self.row["publish_lease_token"] = None
            return 1
        if "published_at = COALESCE(published_at, %s)" in sql:
            self.row["publish_status"] = params[0]
            self.row["published_at"] = self.row.get("published_at") or params[1]
            self.row["publish_lease_token"] = None
            self.row["publish_lease_expires_at"] = None
            return 1
        if "SET status = %s, attempts = attempts + 1" in sql:
            self.row["status"] = params[0]
            self.row["lease_token"] = params[1]
            self.row["lease_expires_at"] = params[2]
            return 1
        raise AssertionError(f"Unexpected STATEMENT: {sql}")


class _Application:
    def __init__(self, db):
        self.db = db

    def has(self, key):
        return key == "DB"

    def make(self, key):
        assert key == "DB"
        return self.db


class _PublishingDriver:
    def __init__(self):
        self.published: list[tuple[bytes, dict]] = []

    def _publish_registered_envelope(self, body, payload, *, capability):
        assert capability is not None
        self.published.append((body, payload))


class _ConfirmDeliveryRaceDB(_CrashDB):
    def __init__(self, envelope: bytes, payload: dict):
        super().__init__(envelope, payload)
        self.row.update(
            {
                "status": "pending",
                "publish_status": "processing",
                "publish_lease_token": "relay-token",
                "publish_lease_expires_at": pendulum.now("UTC").add(minutes=5),
                "published_at": None,
                "lease_token": None,
                "lease_expires_at": None,
            }
        )

    def statement(self, sql, params):
        if "published_at = COALESCE(published_at, %s)" in sql:
            return super().statement(sql, params)
        if "SET status = %s, attempts = attempts + 1" in sql:
            return super().statement(sql, params)
        if "SET publish_status = %s, published_at = %s" in sql:
            if (
                self.row["publish_status"] == "processing"
                and self.row["publish_lease_token"] == params[5]
            ):
                return super().statement(sql, params)
            return 0
        return super().statement(sql, params)


class _ReceiptBeforeConfirmDriver(_PublishingDriver):
    def __init__(self):
        super().__init__()
        self.store = None
        self.claim = None

    def _publish_registered_envelope(self, body, payload, *, capability):
        super()._publish_registered_envelope(
            body,
            payload,
            capability=capability,
        )
        self.claim = self.store.claim_execution(body=body, payload=payload)


class _RetryTrackerModel:
    STATUS_PENDING = "pending"
    STATUS_RETRYING = "retrying"
    STATUS_PROCESSING = "processing"
    STATUS_COMPLETED = "completed"
    STATUS_SUCCESS = "success"
    STATUS_FAILED = "failed"
    STATUS_CANCELLED = "cancelled"

    def __init__(self, status: str):
        self.record = SimpleNamespace(id=91, status=status)
        self.filters: list[tuple[str, object]] = []
        self.update_count = 0

    def find(self, job_id: int):
        return self.record if job_id == self.record.id else None

    def where(self, field: str, value):
        self.filters.append((field, value))
        return self

    def update(self, values: dict):
        matches = all(
            getattr(self.record, field) == value
            for field, value in self.filters
        )
        self.filters.clear()
        if not matches:
            return 0
        self.record.status = values["status"]
        self.update_count += 1
        return 1


def _envelope() -> tuple[bytes, dict]:
    payload = {
        "obj": CrashRecoveryJob(7),
        "args": (),
        "callback": "handle",
        "created": pendulum.now("UTC"),
        "job_id": _JOB_ID,
        "db_job_id": 91,
        "timeout_seconds": 300,
        "attempts": 0,
        "_otel": {},
        "_tenant": 5,
        "_tenant_mode": "tenant",
        "queue": "sync",
        "priority": "default",
        "dispatched_at": pendulum.now("UTC").to_iso8601_string(),
        "replay_of": None,
    }
    body = SignedJsonJobSerializer.serialize(
        payload,
        signing_key_id=_KID,
        signing_keys={_KID: _KEY},
        allowed_prefixes=(__name__,),
    )
    inspected = SignedJsonJobSerializer.inspect_envelope(
        body,
        signing_keys={_KID: _KEY},
    )
    return body, inspected["payload"]


def test_stale_worker_lease_returns_to_outbox_and_republishes_same_envelope():
    body, payload = _envelope()
    db = _CrashDB(body, payload)
    driver = _PublishingDriver()
    store = QueueJobDeliveryStore(
        _Application(db),
        driver,
        {
            "allowed_job_prefixes": (__name__,),
            "canonical_queues": ("sync", "connector"),
            "signing_key_id": _KID,
            "signing_keys": {_KID: _KEY},
        },
    )

    result = store.publish_due()

    assert result["stale_requeued"] == 1
    assert result["published"] == 1
    assert db.tracker_status == "pending"
    assert db.row["publish_status"] == "published"
    assert driver.published == [(body, payload)]

    first = store.claim_execution(body=body, payload=payload)
    duplicate = store.claim_execution(body=body, payload=payload)

    assert first.outcome == "claimed"
    assert duplicate.outcome == "live_lease"


def test_worker_receipt_closes_confirm_to_ledger_cas_crash_window():
    body, payload = _envelope()
    db = _ConfirmDeliveryRaceDB(body, payload)
    driver = _ReceiptBeforeConfirmDriver()
    store = QueueJobDeliveryStore(
        _Application(db),
        driver,
        {
            "allowed_job_prefixes": (__name__,),
            "canonical_queues": ("sync", "connector"),
            "signing_key_id": _KID,
            "signing_keys": {_KID: _KEY},
        },
    )
    driver.store = store
    claimed_row = dict(db.row)

    settled = store._publish_claimed(claimed_row, "relay-token")

    assert settled is True
    assert driver.claim.outcome == "claimed"
    assert db.row["publish_status"] == "published"
    assert db.row["published_at"] is not None
    assert db.row["publish_lease_token"] is None


@pytest.mark.parametrize("claim_outcome", ["live_lease", "not_ready"])
def test_durable_recovery_outcomes_are_acked_without_quorum_redelivery_loop(
    monkeypatch,
    claim_outcome,
):
    module = importlib.import_module(
        "cara.commands.core.QueueWorkCommand"
    )
    payload = {
        "job_id": _JOB_ID,
        "db_job_id": 91,
        "queue": "sync",
    }
    delivery_store = SimpleNamespace(
        claim_execution=lambda **_kwargs: DeliveryClaim(claim_outcome)
    )
    monkeypatch.setattr(
        module.SignedJsonJobSerializer,
        "inspect_envelope",
        lambda *_args, **_kwargs: {"payload": payload},
    )
    queue_service = SimpleNamespace(
        driver=lambda *_args, **_kwargs: SimpleNamespace(
            delivery_store=delivery_store
        )
    )
    application = SimpleNamespace(
        has=lambda _key: False,
        make=lambda key: (
            queue_service
            if key == "queue"
            else (_ for _ in ()).throw(KeyError(key))
        ),
    )
    monkeypatch.setattr(
        builtins,
        "app",
        lambda: application,
        raising=False,
    )
    acks: list[int] = []
    channel = SimpleNamespace(
        basic_ack=lambda *, delivery_tag: acks.append(delivery_tag)
    )

    result = module.JobProcessor.process_message(
        channel,
        SimpleNamespace(delivery_tag=19),
        b"signed-envelope",
        queue_name="sync",
    )

    assert result is True
    assert acks == [19]


@pytest.mark.parametrize(
    ("initial_status", "expected_updates"),
    [
        ("pending", 1),
        ("processing", 0),
        ("completed", 0),
    ],
)
def test_retry_progress_repair_is_monotonic(
    initial_status,
    expected_updates,
):
    model = _RetryTrackerModel(initial_status)

    JobTracker(model).ensure_retry_progress_strict(91)

    assert model.record.status in {"retrying", "processing", "completed"}
    assert model.update_count == expected_updates


def test_retry_source_redelivery_repairs_tracker_and_acks_without_deserializing(
    monkeypatch,
):
    module = importlib.import_module("cara.commands.core.QueueWorkCommand")
    payload = {
        "job_id": _JOB_ID,
        "db_job_id": 91,
        "queue": "sync",
    }
    delivery_store = SimpleNamespace(
        claim_execution=lambda **_kwargs: DeliveryClaim("retry_scheduled")
    )
    model = _RetryTrackerModel("pending")
    tracker = JobTracker(model)
    monkeypatch.setattr(
        module.SignedJsonJobSerializer,
        "inspect_envelope",
        lambda *_args, **_kwargs: {"payload": payload},
    )
    monkeypatch.setattr(
        module.SignedJsonJobSerializer,
        "deserialize_verified",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("retry source must not deserialize or execute")
        ),
    )
    queue_service = SimpleNamespace(
        driver=lambda *_args, **_kwargs: SimpleNamespace(
            delivery_store=delivery_store
        )
    )
    application = SimpleNamespace(
        has=lambda key: key == "JobTracker",
        make=lambda key: (
            tracker
            if key == "JobTracker"
            else (
                queue_service
                if key == "queue"
                else (_ for _ in ()).throw(KeyError(key))
            )
        ),
    )
    monkeypatch.setattr(
        builtins,
        "app",
        lambda: application,
        raising=False,
    )
    acks: list[int] = []
    channel = SimpleNamespace(
        basic_ack=lambda *, delivery_tag: acks.append(delivery_tag)
    )

    result = module.JobProcessor.process_message(
        channel,
        SimpleNamespace(delivery_tag=23),
        b"signed-envelope",
        queue_name="sync",
    )

    assert result is True
    assert acks == [23]
    assert model.record.status == "retrying"
    assert model.update_count == 1
