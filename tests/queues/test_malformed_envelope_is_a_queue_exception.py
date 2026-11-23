"""A malformed envelope fails as ``QueueException``, and the relay quarantines it.

Live defect: untrusted envelope bytes escaped inspection as bare built-in
errors: a 64-character non-ASCII signature reached ``hmac.compare_digest``
(``TypeError``), an integer past the interpreter's digit limit failed
``json.loads`` with a plain ``ValueError``, deep nesting recursed
(``RecursionError``), and a ledger ``signed_envelope`` that is no JSON object
raised before the relay's per-row guard. Every caller treats only
``QueueException`` as "this envelope is invalid", so in the relay one such row
failed the whole publish iteration and its batch stayed leased with it.

Before the fix (``git show HEAD:`` of ``SignedJsonJobSerializer.py``,
``_SignedJsonValues.py`` and ``_DeliveryPublishing.py``) every case below
raised something other than ``QueueException``, and ``_publish_rows`` raised
instead of publishing the good row.
"""

from __future__ import annotations

import json
from collections import Counter
from types import SimpleNamespace

import pytest

from cara.exceptions import QueueException
from cara.queues.contracts import Queueable, ShouldQueue
from cara.queues.delivery import DeliveryEnvelopeMismatch, _DeliveryPublishing
from cara.queues.delivery._DeliveryDiagnostics import (
    _delivery_envelope_bytes,
    _delivery_row_value,
)
from cara.queues.serializers import SignedJsonJobSerializer

_KEYS = {"current": "malformed-envelope-test-key-" * 2}
_JOB_IDS = iter(f"2222222{n}-2222-4222-8222-222222222222" for n in range(10))


class ValueJob(ShouldQueue, Queueable):
    def __init__(self, x):
        self.x = x
        super().__init__()
        self.queue = "sync"
        self.priority = "default"

    async def handle(self):
        return None


def _signed() -> tuple[bytes, str]:
    job_id = next(_JOB_IDS)
    body = SignedJsonJobSerializer.serialize(
        {
            "obj": ValueJob(1),
            "args": (),
            "callback": "handle",
            "created": "2026-11-23T00:00:00Z",
            "job_id": job_id,
            "db_job_id": 12,
            "timeout_seconds": 300,
            "attempts": 0,
            "throttle_attempts": 0,
            "_tenant": 7,
            "_tenant_mode": "tenant",
            "queue": "sync",
            "priority": "default",
            "dispatched_at": "2026-11-23T00:00:00Z",
            "replay_of": None,
        },
        signing_key_id="current",
        signing_keys=_KEYS,
        allowed_prefixes=("tests.queues",),
    )
    return body, job_id


def _with_signature(signature: str) -> bytes:
    body, _job_id = _signed()
    envelope = json.loads(body)
    return json.dumps({**envelope, "signature": signature}, ensure_ascii=False).encode()


def _deeply_nested(depth: int) -> list:
    nested: list = []
    for _ in range(depth):
        nested = [nested]
    return nested


@pytest.mark.parametrize(
    "body",
    [
        pytest.param(_with_signature("é" * 64), id="non-ascii-64-char-signature"),
        pytest.param(b'{"expires_at": ' + b"1" * 4400 + b"}", id="4400-digit-number"),
        pytest.param(b"[" * 120_000 + b"]" * 120_000, id="deeply-nested-bytes"),
        pytest.param({"payload": _deeply_nested(120_000)}, id="deeply-nested-object"),
    ],
)
def test_untrusted_bytes_fail_inspection_as_a_queue_exception(body) -> None:
    with pytest.raises(QueueException):
        SignedJsonJobSerializer.inspect_envelope(
            body, signing_keys=_KEYS, allow_not_before=True
        )


def test_the_genuine_envelope_still_verifies() -> None:
    body, job_id = _signed()

    envelope = SignedJsonJobSerializer.inspect_envelope(body, signing_keys=_KEYS)

    assert envelope["payload"]["job_id"] == job_id


class _Relay:
    """The relay's publish path over claimed rows, with a recording broker."""

    options = {"signing_keys": _KEYS}
    _row_value = staticmethod(_delivery_row_value)
    _envelope_bytes = staticmethod(_delivery_envelope_bytes)
    _verified_envelope = _DeliveryPublishing._delivery_verified_envelope
    _publish_rows = _DeliveryPublishing._delivery_publish_rows
    _publish_batch = _DeliveryPublishing._delivery_publish_batch

    def __init__(self) -> None:
        self.sent: list[str] = []
        self.quarantined: list[str] = []
        self.driver = SimpleNamespace(_publish_registered_envelopes=self._broker)

    def _broker(self, envelopes, *, capability):
        self.sent.extend(payload["job_id"] for _body, payload in envelopes)
        return [None] * len(envelopes)

    def _quarantine_publish(self, job_id, _token, _error) -> None:
        self.quarantined.append(job_id)

    def _mark_published(self, job_ids, _token) -> set[str]:
        return set(job_ids)


def _row(body: bytes, job_id: str, **override) -> dict:
    return {
        "job_id": job_id,
        "db_job_id": 12,
        "payload_sha256": SignedJsonJobSerializer.canonical_envelope_sha256(body),
        "tenant_mode": "tenant",
        "tenant_id": 7,
        "queue": "sync",
        "priority": "default",
        "publish_attempts": 0,
        "signed_envelope": json.loads(body),
        **override,
    }


def _batch() -> list[dict]:
    good_body, good_id = _signed()
    bad_body, bad_id = _signed()
    not_object_body, not_object_id = _signed()
    bad_envelope = {**json.loads(bad_body), "signature": "é" * 64}
    return [
        _row(bad_body, bad_id, signed_envelope=bad_envelope),
        _row(not_object_body, not_object_id, signed_envelope=[1, 2]),
        _row(good_body, good_id),
    ]


def test_the_relay_marks_each_malformed_row_and_still_publishes_the_rest() -> None:
    relay = _Relay()
    rows = _batch()

    outcomes = relay._publish_rows(rows)

    assert isinstance(outcomes[0], DeliveryEnvelopeMismatch)
    assert isinstance(outcomes[1], DeliveryEnvelopeMismatch)
    assert outcomes[2] is None
    assert relay.sent == [rows[2]["job_id"]]


def test_the_relay_quarantines_the_malformed_rows_of_a_batch() -> None:
    relay = _Relay()
    rows = _batch()
    result: Counter = Counter()

    stop = relay._publish_batch(rows, "lease", result)

    assert stop is False
    assert relay.quarantined == [rows[0]["job_id"], rows[1]["job_id"]]
    assert result["quarantined"] == 2 and result["published"] == 1
