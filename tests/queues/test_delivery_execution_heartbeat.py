"""The delivery execution lease is a heartbeat lease, and shutdown hands it back.

Live, 2026-10-30: a worker died mid-job and its ``queue_job_delivery`` row
stayed ``processing`` with ``lease_expires_at = claim + timeout + grace``
(65 minutes for a one-hour job). The relay's broker-window claim counts such
a row as held, so the orphan blocked a slot of its queue's window — fleet
wide — until the lease ran out. These pin the fix:

* a claim takes the short heartbeat lease, and ``timeout + grace`` is only
  its ceiling (``lease_deadline``);
* ``renew_execution`` is a compare-and-set on the lease token, capped by the
  deadline;
* the worker renews while the job runs — even a job that blocks its event
  loop — and stops renewing before it settles;
* shutdown cancellation releases the row to ``pending`` and the tracker with
  it, and a forced exit releases what cancellation could not.
"""

from __future__ import annotations

import asyncio
import builtins
import importlib
import time
from types import SimpleNamespace

import pendulum
import pytest

from cara.commands.core.JobProcessor import JobProcessor
from cara.configuration import Configuration
from cara.queues.delivery import DeliveryClaim
from cara.queues.LeaseHeartbeat import LeaseHeartbeat
from tests.queues.test_queue_delivery_crash_recovery import (
    _CrashDB,
    _envelope,
    _PublishingDriver,
    _store,
)

_JOB_ID = "55555555-5555-4555-8555-555555555555"


@pytest.fixture(autouse=True)
def _isolated_configuration(monkeypatch):
    monkeypatch.setattr(Configuration, "_instance", None)
    Configuration.empty()
    yield
    for heartbeat in LeaseHeartbeat.live():
        heartbeat.stop()


class _RenewingDB(_CrashDB):
    def statement(self, sql, params):
        if sql.startswith("UPDATE queue_job_delivery SET lease_expires_at = %s"):
            expiry, _now, job_id, status, token = params
            owned = (
                self.row["job_id"] == job_id
                and self.row["status"] == status
                and self.row["lease_token"] == token
            )
            if owned:
                self.row["lease_expires_at"] = expiry
            return int(owned)
        return super().statement(sql, params)


def _pending_store(**options):
    body, payload = _envelope()
    db = _RenewingDB(body, payload)
    db.row.update({"status": "pending", "lease_token": None, "lease_expires_at": None})
    return db, _store(db, _PublishingDriver(), **options), body, payload


# ── the store ────────────────────────────────────────────────────────────


def test_a_claim_takes_the_heartbeat_lease_and_keeps_the_budget_as_its_ceiling():
    db, store, body, payload = _pending_store()
    before = pendulum.now("UTC")

    claim = store.claim_execution(body=body, payload=payload)

    assert claim.outcome == "claimed"
    assert store.execution_heartbeat_seconds == 120
    lease = (db.row["lease_expires_at"] - before).total_seconds()
    assert 119 <= lease <= 121, "a claim must not take the whole timeout + grace"
    budget = (claim.lease_deadline - before).total_seconds()
    assert budget == pytest.approx(
        payload["timeout_seconds"] + store.execution_lease_grace_seconds, abs=2
    )


def test_renewal_extends_only_this_workers_live_lease():
    db, store, body, payload = _pending_store()
    claim = store.claim_execution(body=body, payload=payload)
    db.row["lease_expires_at"] = pendulum.now("UTC").add(seconds=5)

    assert store.renew_execution(
        db.row["job_id"], claim.lease_token, deadline=claim.lease_deadline
    )
    assert (db.row["lease_expires_at"] - pendulum.now("UTC")).total_seconds() > 100

    # Another worker's token, or a row the relay already recovered, is
    # never extended.
    assert not store.renew_execution(db.row["job_id"], "someone-else")
    db.row.update({"status": "pending", "lease_token": None})
    assert not store.renew_execution(db.row["job_id"], claim.lease_token)


def test_renewal_never_carries_the_lease_past_the_budget():
    db, store, body, payload = _pending_store()
    claim = store.claim_execution(body=body, payload=payload)
    near = pendulum.now("UTC").add(seconds=20)

    assert store.renew_execution(db.row["job_id"], claim.lease_token, deadline=near)
    assert db.row["lease_expires_at"] == near

    spent = pendulum.now("UTC").subtract(seconds=1)
    assert not store.renew_execution(db.row["job_id"], claim.lease_token, deadline=spent)


def test_a_short_job_budget_still_caps_the_first_lease():
    db, store, body, payload = _pending_store(
        delivery_execution_lease_seconds=200,
        delivery_execution_lease_grace_seconds=30,
        delivery_default_job_timeout_seconds=100,
        delivery_execution_heartbeat_seconds=600,
    )

    claim = store.claim_execution(body=body, payload=payload)

    assert claim.outcome == "claimed"
    assert db.row["lease_expires_at"] <= claim.lease_deadline


# ── the worker ───────────────────────────────────────────────────────────


class _BlockingPull:
    """A job whose body blocks its event loop, like a synchronous pull."""

    queue = "sync"
    seconds = 0.15

    async def handle(self):
        time.sleep(self.seconds)


class _CancelledPull:
    queue = "sync"

    async def handle(self):
        raise asyncio.CancelledError()


class _Tracker:
    def __init__(self):
        self.statuses: list[str] = []

    def update_job_status_strict(self, db_job_id, status):
        self.statuses.append(status)

    def is_job_completed(self, db_job_id):
        return False


class _DeliveryStore:
    execution_heartbeat_seconds = 30

    def __init__(self):
        self.events: list[str] = []
        self.renewals: list[str] = []

    def claim_execution(self, *, body, payload):
        self.events.append("claim")
        return DeliveryClaim(
            "claimed",
            "worker-lease",
            lease_deadline=pendulum.now("UTC").add(seconds=600),
        )

    def renew_execution(self, job_id, lease_token, *, deadline=None):
        assert (job_id, lease_token) == (_JOB_ID, "worker-lease")
        assert "complete" not in self.events, "a renewal raced the settlement"
        self.renewals.append(job_id)
        return True

    def execution_timeout_for(self, _job_class):
        return 30

    def complete_with_tracker(self, job_id, lease_token, *, db_job_id):
        self.events.append("complete")

    def abandon_execution(self, job_id, lease_token):
        self.events.append("abandon")


def _run_worker(monkeypatch, job, store, tracker):
    execution = importlib.import_module("cara.commands.core._JobExecution")
    payload = {
        "job_id": _JOB_ID,
        "db_job_id": 91,
        "queue": "sync",
        "timeout_seconds": 30,
        "attempts": 0,
        "_tenant": 5,
        "_tenant_mode": "tenant",
    }
    monkeypatch.setattr(
        execution.SignedJsonJobSerializer,
        "inspect_envelope",
        lambda *_args, **_kwargs: {"payload": payload},
    )
    monkeypatch.setattr(
        execution.SignedJsonJobSerializer,
        "deserialize_verified",
        lambda verified, **_kwargs: {**verified, "obj": job, "args": ()},
    )
    queue_service = SimpleNamespace(
        driver=lambda *_args, **_kwargs: SimpleNamespace(delivery_store=store)
    )
    logger = SimpleNamespace(
        debug=lambda *a, **k: None,
        info=lambda *a, **k: None,
        warning=lambda *a, **k: None,
        error=lambda *a, **k: None,
    )

    def _make(key):
        return {"queue": queue_service, "JobTracker": tracker, "logger": logger}[key]

    application = SimpleNamespace(
        has=lambda key: key == "JobTracker",
        make=_make,
        call=lambda method, *args: method(*args),
    )
    monkeypatch.setattr(builtins, "app", lambda: application, raising=False)
    settled: list[str] = []
    channel = SimpleNamespace(
        basic_ack=lambda *, delivery_tag: settled.append("ack"),
        basic_nack=lambda *, delivery_tag, requeue: settled.append("nack"),
    )
    outcome = JobProcessor.process_message(
        channel,
        SimpleNamespace(delivery_tag=7),
        b"signed-envelope",
        queue_name="sync",
    )
    return outcome, settled


def test_the_worker_renews_while_a_blocking_job_runs_and_stops_before_settling(
    monkeypatch,
):
    monkeypatch.setattr(LeaseHeartbeat, "RENEWALS_PER_LEASE", 1000)  # every 30 ms
    store = _DeliveryStore()
    tracker = _Tracker()

    outcome, settled = _run_worker(monkeypatch, _BlockingPull(), store, tracker)

    assert outcome == "success"
    assert settled == ["ack"]
    assert store.events == ["claim", "complete"]
    assert len(store.renewals) >= 2, "a blocked event loop starved the heartbeat"
    assert LeaseHeartbeat.live() == []


def test_shutdown_cancellation_releases_the_row_and_the_tracker(monkeypatch):
    store = _DeliveryStore()
    tracker = _Tracker()

    with pytest.raises(asyncio.CancelledError):
        _run_worker(monkeypatch, _CancelledPull(), store, tracker)

    assert store.events == ["claim", "abandon"]
    assert tracker.statuses == ["processing", "pending"]
    assert LeaseHeartbeat.live() == []


def test_a_forced_exit_releases_the_row_a_stuck_job_still_holds(monkeypatch):
    store = _DeliveryStore()
    tracker = _Tracker()
    released: dict[str, object] = {}

    class _StuckPull:
        queue = "sync"

        async def handle(self):
            # The worker's forced-exit step, while this job ignores
            # cancellation inside a blocking call.
            released["count"] = LeaseHeartbeat.release_all(timeout_seconds=2.0)
            released["events"] = list(store.events)
            released["tracker"] = list(tracker.statuses)

    _run_worker(monkeypatch, _StuckPull(), store, tracker)

    assert released == {
        "count": 1,
        "events": ["claim", "abandon"],
        "tracker": ["processing", "pending"],
    }
