"""A delayed job's clock, to the sub-second, through the signed envelope.

Time hunt, round 4 (2026-11-09). The ledger's ``available_at`` is rebuilt from
the envelope's ``not_before``, a whole epoch second, so what the envelope rounds
is what the worker's ``available_at > now`` gate reads.
"""

from __future__ import annotations

import datetime as dt
import uuid
from contextlib import contextmanager

import pendulum
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from cara.exceptions import QueueException
from cara.queues.contracts import Queueable, ShouldQueue
from cara.queues.delay import DurableDelayedJobStore
from cara.queues.serializers import SignedJsonJobSerializer

_KEY = "delayed-queue-signing-key-" * 3
_KID = "delayed-current"
_NOW = pendulum.datetime(2026, 11, 9, 12, 0, 0, 500_000, tz="UTC")


class HuntTimeJob(ShouldQueue, Queueable):
    def __init__(self, item_id: int):
        super().__init__()
        self.item_id = item_id
        self.queue = "sync"
        self.priority = "default"

    async def handle(self):
        return None


class _DB:
    @contextmanager
    def transaction(self):
        yield self

    def statement(self, _sql, _params):
        return 1


class _Application:
    def has(self, key):
        return key == "DB"

    def make(self, _key):
        return _DB()


class _Delivery:
    def __init__(self):
        self.registered = []

    def register(self, **kwargs):
        self.registered.append(kwargs)
        return True

    def mark_retry_scheduled(self, *_args, **_kwargs):
        return None

    def publish_after_commit(self, _job_id):
        return None

    @staticmethod
    def execution_timeout_for(_job):
        return 300


class _Driver:
    application = _Application()
    options = {
        "allowed_job_prefixes": [__name__],
        "envelope_max_age_seconds": 2_678_400,
        "envelope_ttl_seconds": 604_800,
        "signing_key_id": _KID,
        "signing_keys": {_KID: _KEY},
    }

    @staticmethod
    def _priority_name(_job, _options):
        return "default"

    @staticmethod
    def require_canonical_queue(queue_name):
        return queue_name

    @staticmethod
    def _tenant_payload(_job, _options):
        return {"_tenant_mode": "tenant", "_tenant": 5}

    @staticmethod
    def _create_job_record(_job, _job_id, _options):
        return 9


def _freeze(monkeypatch, moment):
    monkeypatch.setattr(pendulum, "now", lambda *_args, **_kwargs: moment)


def _schedule(when):
    ledger = _Delivery()
    store = DurableDelayedJobStore(
        _Driver.application, _Driver(), _Driver.options, delivery_store=ledger
    )
    store.schedule(
        HuntTimeJob(1),
        when,
        {
            "tenant_id": 5,
            "db_job_id": 9,
            "attempts": 1,
            "job_id": str(uuid.uuid4()),
            "source_delivery_job_id": "source",
            "source_delivery_lease_token": "lease",
        },
    )
    return ledger.registered[0]["envelope"]


def test_the_ledger_never_releases_a_delayed_job_before_the_instant_it_was_asked(
    monkeypatch,
):
    """Jobs 282644 and 282645 (backlog P4) ran about 0.4 s before their
    ``available_at``: ``_SignedJsonValues.epoch`` floors ``not_before``, and
    ``register`` rebuilds the ledger's ``available_at`` from that whole second."""
    _freeze(monkeypatch, _NOW)
    asked = _NOW.add(seconds=30)  # 12:00:30.5

    envelope = _schedule(asked)

    released = pendulum.from_timestamp(int(envelope["not_before"]), tz="UTC")
    assert released >= asked


def test_a_retry_due_within_the_second_boundary_it_crosses_is_accepted(monkeypatch):
    """A worker reads the clock for ``when``, the store reads it again for
    ``issued_at``. A due-now retry whose two reads straddle a second floors
    to ``not_before`` < ``issued_at`` and the envelope refuses itself."""
    _freeze(monkeypatch, pendulum.datetime(2026, 11, 9, 12, 0, 1, 40, tz="UTC"))
    asked = pendulum.datetime(2026, 11, 9, 12, 0, 0, 999_990, tz="UTC")

    envelope = _schedule(asked)

    assert envelope["not_before"] >= envelope["issued_at"]


@given(microsecond=st.integers(0, 999_999), delay=st.integers(1, 86_400))
@settings(deadline=None, max_examples=200)
def test_not_before_never_precedes_the_instant_it_encodes(microsecond, delay):
    issued = pendulum.datetime(2026, 3, 8, 9, 59, 59, tz="UTC")
    asked = issued.add(seconds=delay, microseconds=microsecond)

    body = SignedJsonJobSerializer.serialize(
        _payload(asked),
        signing_key_id=_KID,
        signing_keys={_KID: _KEY},
        allowed_prefixes=[__name__],
        issued_at=issued,
        not_before=asked,
        ttl_seconds=604_800,
        max_age_seconds=2_678_400,
    )
    envelope = SignedJsonJobSerializer.inspect_envelope(
        body,
        signing_keys={_KID: _KEY},
        now=issued,
        clock_skew_seconds=30,
        max_age_seconds=2_678_400,
        allow_not_before=True,
    )

    assert envelope["not_before"] >= asked.timestamp()


def _payload(asked):
    return {
        "obj": HuntTimeJob(1),
        "args": (),
        "callback": "handle",
        "created": asked.to_iso8601_string(),
        "job_id": str(uuid.uuid4()),
        "db_job_id": 9,
        "timeout_seconds": 300,
        "attempts": 0,
        "throttle_attempts": 0,
        "_otel": {},
        "_tenant_mode": "tenant",
        "_tenant": 5,
        "queue": "sync",
        "priority": "default",
        "dispatched_at": asked.to_iso8601_string(),
        "replay_of": None,
        "unique_key": None,
    }


def test_a_naive_instant_never_becomes_a_not_before():
    """A wall time with no zone is refused, never guessed as UTC or local."""
    naive = dt.datetime(2026, 11, 9, 12, 0, 0)

    with pytest.raises(QueueException):
        SignedJsonJobSerializer.serialize(
            _payload(pendulum.instance(naive)),
            signing_key_id=_KID,
            signing_keys={_KID: _KEY},
            allowed_prefixes=[__name__],
            issued_at=dt.datetime(2026, 11, 9, 11, 0, tzinfo=dt.UTC),
            not_before=naive,
            ttl_seconds=604_800,
            max_age_seconds=2_678_400,
        )


@pytest.mark.parametrize(
    "spelling",
    [
        "2026-11-09T12:00:30+00:00",
        "2026-11-09T07:00:30-05:00",
        "2026-11-09T15:00:30+03:00",
        "2026-11-09T12:00:30Z",
    ],
)
def test_every_offset_spelling_of_a_delay_is_one_instant(monkeypatch, spelling):
    _freeze(monkeypatch, pendulum.datetime(2026, 11, 9, 12, 0, 0, tz="UTC"))

    envelope = _schedule(spelling)

    assert envelope["not_before"] == int(
        pendulum.datetime(2026, 11, 9, 12, 0, 30, tz="UTC").timestamp()
    )
