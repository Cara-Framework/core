"""The relay leases a backlog in batches, not one claim statement per row.

Stress 2026-11-12: the tenant-fair claim walks every tenant with due work
(a few index probes per tenant), and the relay ran it once PER PUBLISHED ROW.
At 1,000 backlogged tenants one claim took 12-20 ms, so the fleet's single
publisher topped out at 35-65 rows/s however many workers or window slots
existed. The fair ranking (the plan) now runs once per relay iteration, the
cheap reserve leases from it as the free broker window allows, ONE broker
batch awaits its confirms together, and one statement settles every
broker-confirmed row (scratch stack 2026-11-13).
"""

from __future__ import annotations

from contextlib import contextmanager

import pendulum

from cara.queues.contracts import Queueable, ShouldQueue
from cara.queues.delivery import QueueJobDeliveryStore
from cara.queues.serializers import SignedJsonJobSerializer

_KID = "batch-claim-current"
_KEY = "batch-claim-signing-key-" * 3


class BatchClaimJob(ShouldQueue, Queueable):
    queue = "sync"

    def __init__(self, item_id: int):
        super().__init__()
        self.item_id = item_id

    async def handle(self):
        return None


def _envelope(index: int) -> tuple[bytes, dict]:
    job_id = f"00000000-0000-4000-8000-{index:012d}"
    payload = {
        "obj": BatchClaimJob(index),
        "args": (),
        "callback": "handle",
        "created": pendulum.now("UTC"),
        "job_id": job_id,
        "db_job_id": 1000 + index,
        "timeout_seconds": 300,
        "attempts": 0,
        "_otel": {},
        "_tenant": 9_000_000 + index,
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
    envelope = SignedJsonJobSerializer.inspect_envelope(body, signing_keys={_KID: _KEY})
    return body, envelope["payload"]


class _BacklogDB:
    """``count`` due rows, one per tenant; serves the plan and the reserve."""

    def __init__(self, count: int):
        self.due = []
        for index in range(count):
            body, payload = _envelope(index)
            self.due.append(
                {
                    "job_id": payload["job_id"],
                    "db_job_id": payload["db_job_id"],
                    "payload_sha256": SignedJsonJobSerializer.canonical_envelope_sha256(
                        body
                    ),
                    "signed_envelope": body,
                    "tenant_mode": "tenant",
                    "tenant_id": payload["_tenant"],
                    "queue": "sync",
                    "priority": "default",
                    "publish_attempts": 0,
                }
            )
        self.plans = 0
        self.reserves = 0
        self.settles = 0
        self.lock_keys: list[list[str]] = []
        self.leased: dict[str, str] = {}

    @contextmanager
    def transaction(self):
        yield self

    def advisory_xact_lock(self, *keys):
        self.lock_keys.append(list(keys))

    def select_one(self, sql, params=None):
        if sql.startswith("WITH RECURSIVE settings AS"):
            self.plans += 1
            per_queue = params[5]
            return {"planned": [[row["job_id"], "sync"] for row in self.due[:per_queue]]}
        if sql.startswith("WITH settings AS"):
            self.reserves += 1
            limit, planned, token = params[4], params[7], params[8]
            due = {row["job_id"]: row for row in self.due}
            rows = [due[job_id] for job_id in planned if job_id in due][:limit]
            taken = {row["job_id"] for row in rows}
            self.due = [row for row in self.due if row["job_id"] not in taken]
            for row in rows:
                self.leased[row["job_id"]] = token
            return {"claimed_rows": [dict(row) for row in rows]}
        if sql.startswith("WITH settled AS"):
            self.settles += 1
            ids = [job_id for job_id in params[3] if self.leased.get(job_id) == params[5]]
            return {"ids": ids}
        raise AssertionError(f"unexpected select_one: {sql[:80]}")


class _Driver:
    """Confirms every envelope; records each broker batch."""

    def __init__(self):
        self.published: list[str] = []
        self.batches: list[int] = []

    def _publish_registered_envelopes(self, envelopes, *, capability):
        self.batches.append(len(envelopes))
        self.published.extend(payload["job_id"] for _body, payload in envelopes)
        return [None] * len(envelopes)


class _Application:
    def __init__(self, db):
        self.db = db

    def has(self, key):
        return key == "DB"

    def make(self, key):
        return self.db


def _relay_store(db, driver, **options):
    store = QueueJobDeliveryStore(
        _Application(db),
        driver,
        {
            "canonical_queues": ("sync",),
            "delivery_claim_batch": 100,
            "delivery_broker_window_per_queue": 64,
            "allowed_job_prefixes": (__name__,),
            "signing_key_id": _KID,
            "signing_keys": {_KID: _KEY},
            **options,
        },
    )
    store.recover_stale_executions = lambda _limit: {
        "requeued": 0,
        "reconciled": 0,
        "unclaimed": 0,
    }
    store.expire_due = lambda _limit: 0
    return store


def test_a_backlog_is_leased_and_settled_in_batches_not_row_by_row():
    db = _BacklogDB(40)
    driver = _Driver()
    store = _relay_store(db, driver)

    result = store.publish_due()

    assert result["published"] == 40
    assert len(driver.published) == 40
    # One fair ranking per iteration, one lease, ONE broker batch whose
    # confirms are awaited together, one settlement. Row-by-row this was
    # 41 claim statements, 40 confirm waits and 40 settlements.
    assert db.plans == 1
    assert db.reserves == 1
    assert driver.batches == [40]
    assert db.settles == 1


def test_the_reserve_locks_only_the_planned_queues():
    db = _BacklogDB(3)
    store = _relay_store(db, _Driver(), canonical_queues=("bulk", "orders", "sync"))

    store.publish_due()

    # Per queue, never one lock for the whole fleet: a relay reserving
    # ``sync`` does not wait on one reserving ``bulk``.
    assert db.lock_keys == [["queue_job_delivery:publish-window:sync"]]


def test_the_relay_never_leases_more_than_its_claim_batch():
    db = _BacklogDB(30)
    store = _relay_store(db, _Driver(), delivery_claim_batch=7)

    assert store.publish_due()["published"] == 7
    assert len(db.due) == 23


def test_later_reserves_of_an_iteration_lease_from_the_same_plan():
    """A reserve that took less than the batch (a full window, a concurrent
    relay) is followed by another from the SAME plan: the expensive ranking
    is not repeated inside one iteration."""
    db = _BacklogDB(10)
    original = db.select_one
    calls = {"reserve": 0}

    def select_one(sql, params=None):
        if sql.startswith("WITH settings AS"):
            calls["reserve"] += 1
            # The first reserve finds room for 4 only.
            params = [*params[:4], 4 if calls["reserve"] == 1 else params[4], *params[5:]]
        return original(sql, params)

    db.select_one = select_one
    store = _relay_store(db, _Driver())

    assert store.publish_due()["published"] == 10
    assert db.plans == 1
    assert db.reserves == 2


class _RefusingDriver:
    """The broker confirms every envelope but refuses (nacks) ``refused``."""

    def __init__(self, refused: str):
        self.refused = refused
        self.published: list[str] = []

    def _publish_registered_envelopes(self, envelopes, *, capability):
        outcomes = []
        for _body, payload in envelopes:
            if payload["job_id"] == self.refused:
                outcomes.append(ConnectionError("nacked by the broker"))
            else:
                self.published.append(payload["job_id"])
                outcomes.append(None)
        return outcomes


def test_a_refused_row_is_retried_and_every_confirmed_row_settles():
    db = _BacklogDB(6)
    refused = "00000000-0000-4000-8000-000000000002"
    driver = _RefusingDriver(refused)
    store = _relay_store(db, driver)
    released: list[str] = []
    store._release_publish = lambda job_id, *_a: released.append(job_id) or True

    result = store.publish_due()

    assert result["published"] == 5
    assert released == [refused]
    assert result["retried"] == 1
    # A refusal stops the iteration: no second lease this round.
    assert db.reserves == 1


class _DownDriver:
    def __init__(self):
        self.attempts = 0

    def _publish_registered_envelopes(self, envelopes, *, capability):
        self.attempts += 1
        raise ConnectionError("broker went away")


def test_an_outage_costs_one_attempt_and_hands_the_rest_back_unpenalised():
    db = _BacklogDB(6)
    driver = _DownDriver()
    store = _relay_store(db, driver)
    released: list[str] = []
    unclaimed: list[str] = []
    store._release_publish = lambda job_id, *_a: released.append(job_id) or True
    store._unclaim_publish = lambda ids, _token: unclaimed.extend(ids)

    result = store.publish_due()

    assert driver.attempts == 1
    assert result["published"] == 0
    # The first row carries the outage (one publish attempt per outage, not
    # per row); the others go back to the outbox with no attempt counted.
    assert released == ["00000000-0000-4000-8000-000000000000"]
    assert unclaimed == [f"00000000-0000-4000-8000-{index:012d}" for index in range(1, 6)]
    assert result["retried"] == 1
    assert db.reserves == 1
