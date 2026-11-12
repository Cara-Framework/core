"""The relay leases a backlog in batches, not one claim statement per row.

Stress 2026-11-12: the tenant-fair claim walks every tenant with due work
(a few index probes per tenant), and the relay ran it once PER PUBLISHED ROW.
At 1,000 backlogged tenants one claim took 12-20 ms, so the fleet's single
publisher topped out at 35-65 rows/s however many workers or window slots
existed. One claim statement now leases the whole batch the free broker
window allows, and one statement settles every broker-confirmed row.
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
    """``count`` due rows, one per tenant; serves either claim shape."""

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
        self.claims = 0
        self.settles = 0
        self.leased: dict[str, str] = {}

    @contextmanager
    def transaction(self):
        yield self

    def select_one(self, sql, params=None):
        if sql.startswith("SELECT pg_advisory_xact_lock"):
            return {"locked": None}
        if sql.startswith("WITH RECURSIVE settings AS"):
            self.claims += 1
            limit = params[5] if isinstance(params[5], int) else 1
            token = next(p for p in params if isinstance(p, str) and len(p) == 32)
            rows, self.due = self.due[:limit], self.due[limit:]
            for row in rows:
                self.leased[row["job_id"]] = token
            if not rows:
                return None if not isinstance(params[5], int) else {"claimed_rows": []}
            return {**rows[0], "claimed_rows": [dict(row) for row in rows]}
        if sql.startswith("WITH settled AS"):
            self.settles += 1
            ids = [job_id for job_id in params[3] if self.leased.get(job_id) == params[5]]
            return {"ids": ids}
        raise AssertionError(f"unexpected select_one: {sql[:80]}")

    def statement(self, sql, params=None):
        if "SET publish_status = %s, published_at = %s" in sql:
            self.settles += 1
            return 1
        raise AssertionError(f"unexpected statement: {sql[:80]}")


class _Driver:
    def __init__(self):
        self.published: list[str] = []

    def _publish_registered_envelope(self, body, payload, *, capability):
        self.published.append(payload["job_id"])


class _Application:
    def __init__(self, db):
        self.db = db

    def has(self, key):
        return key == "DB"

    def make(self, key):
        return self.db


def test_a_backlog_is_leased_and_settled_in_batches_not_row_by_row():
    db = _BacklogDB(40)
    driver = _Driver()
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
        },
    )
    store.recover_stale_executions = lambda _limit: {
        "requeued": 0,
        "reconciled": 0,
        "unclaimed": 0,
    }
    store.expire_due = lambda _limit: 0

    result = store.publish_due()

    assert result["published"] == 40
    assert len(driver.published) == 40
    # Row-by-row this was 41 claim statements and 40 settlements.
    assert db.claims <= 2
    assert db.settles == 1


def test_the_relay_never_leases_more_than_its_claim_batch():
    db = _BacklogDB(30)
    store = QueueJobDeliveryStore(
        _Application(db),
        _Driver(),
        {
            "canonical_queues": ("sync",),
            "delivery_claim_batch": 7,
            "delivery_broker_window_per_queue": 64,
            "allowed_job_prefixes": (__name__,),
            "signing_key_id": _KID,
            "signing_keys": {_KID: _KEY},
        },
    )
    store.recover_stale_executions = lambda _limit: {
        "requeued": 0,
        "reconciled": 0,
        "unclaimed": 0,
    }
    store.expire_due = lambda _limit: 0

    assert store.publish_due()["published"] == 7
    assert len(db.due) == 23


class _FailingDriver:
    """Confirms every row except ``fail_on``, which the broker refuses."""

    def __init__(self, fail_on: str):
        self.fail_on = fail_on
        self.published: list[str] = []

    def _publish_registered_envelope(self, body, payload, *, capability):
        if payload["job_id"] == self.fail_on:
            raise ConnectionError("broker went away")
        self.published.append(payload["job_id"])


def test_an_outage_mid_batch_settles_the_confirmed_rows_and_hands_back_the_rest():
    db = _BacklogDB(6)
    failing = "00000000-0000-4000-8000-000000000002"
    driver = _FailingDriver(failing)
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
        },
    )
    store.recover_stale_executions = lambda _limit: {
        "requeued": 0,
        "reconciled": 0,
        "unclaimed": 0,
    }
    store.expire_due = lambda _limit: 0
    released: list[str] = []
    unclaimed: list[str] = []
    store._release_publish = lambda job_id, *_a: released.append(job_id) or True
    store._unclaim_publish = lambda ids, _token: unclaimed.extend(ids)

    result = store.publish_due()

    assert driver.published == [
        "00000000-0000-4000-8000-000000000000",
        "00000000-0000-4000-8000-000000000001",
    ]
    assert result["published"] == 2
    assert released == [failing]
    # Rows after the outage were never tried: back to the outbox, no
    # publish attempt counted against them.
    assert unclaimed == [f"00000000-0000-4000-8000-{index:012d}" for index in (3, 4, 5)]
    assert result["retried"] == 1
    # The relay stops after a systemic failure: no second claim this round.
    assert db.claims == 1
