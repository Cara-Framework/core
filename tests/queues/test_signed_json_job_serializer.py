import json

import pytest

from cara.exceptions import QueueException
from cara.queues import JobClassResolver
from cara.queues.contracts import Queueable, ShouldQueue
from cara.queues.serializers import SignedJsonJobSerializer

_KEY = "signed-queue-test-key-" * 3
_OLD_KEY = "old-signed-queue-test-key-" * 3
_KID = "current"
_KEYS = {_KID: _KEY}
_PREFIXES = ("tests.queues",)
_JOB_ID = "11111111-1111-4111-8111-111111111111"


class ExampleJob(ShouldQueue, Queueable):
    def __init__(self, item_id: int, *, priority: str = "default"):
        self.item_id = int(item_id)
        super().__init__()
        self.queue = "sync"
        self.priority = priority

    async def handle(self):
        return None


class SyncJob(ShouldQueue, Queueable):
    def __init__(self):
        super().__init__()
        self.queue = "sync"
        self.priority = "default"

    def handle(self):
        return None


def _payload(job):
    return {
        "obj": job,
        "args": (),
        "callback": "handle",
        "created": "2026-07-16T00:00:00Z",
        "job_id": _JOB_ID,
        "db_job_id": 12,
        "timeout_seconds": 300,
        "attempts": 0,
        "_otel": {"traceparent": "00-" + "a" * 32 + "-" + "b" * 16 + "-01"},
        "_tenant": 7,
        "_tenant_mode": "tenant",
        "queue": job.queue,
        "priority": job.priority,
        "dispatched_at": "2026-07-16T00:00:00Z",
        "replay_of": None,
    }


def test_signed_json_round_trip_preserves_constructor_and_queue_metadata():
    body = SignedJsonJobSerializer.serialize(
        _payload(ExampleJob(42, priority="high")),
        signing_key_id=_KID,
        signing_keys=_KEYS,
        allowed_prefixes=_PREFIXES,
        issued_at=1_752_643_200,
    )

    decoded = SignedJsonJobSerializer.deserialize(
        body,
        signing_keys=_KEYS,
        allowed_prefixes=_PREFIXES,
        now=1_752_643_201,
    )

    assert decoded["obj"] is ExampleJob
    assert decoded["init_kwargs"]["item_id"] == 42
    assert decoded["init_kwargs"]["priority"] == "high"
    assert decoded["queue"] == "sync"
    assert decoded["priority"] == "high"
    assert decoded["_tenant"] == 7
    assert decoded["_tenant_mode"] == "tenant"


def test_serializer_rejects_oversized_body_before_transport(monkeypatch):
    job = ExampleJob(42)
    job.large_argument = "x" * 1024
    monkeypatch.setattr(SignedJsonJobSerializer, "MAX_PAYLOAD_BYTES", 512)

    with pytest.raises(QueueException, match="maximum wire size"):
        SignedJsonJobSerializer.serialize(
            _payload(job),
            signing_key_id=_KID,
            signing_keys=_KEYS,
            allowed_prefixes=_PREFIXES,
            issued_at=1_752_643_200,
        )


def test_serializer_rejects_excessive_json_depth():
    nested: dict = {}
    cursor = nested
    for _ in range(SignedJsonJobSerializer.MAX_JSON_DEPTH + 2):
        cursor["next"] = {}
        cursor = cursor["next"]
    job = ExampleJob(7)
    job.payload = nested

    with pytest.raises(QueueException, match="maximum JSON depth"):
        SignedJsonJobSerializer.serialize(
            _payload(job),
            signing_key_id=_KID,
            signing_keys={_KID: _KEY},
            allowed_prefixes=_PREFIXES,
        )


def test_signature_is_verified_before_job_class_resolution(monkeypatch):
    body = SignedJsonJobSerializer.serialize(
        _payload(ExampleJob(42)),
        signing_key_id=_KID,
        signing_keys=_KEYS,
        allowed_prefixes=_PREFIXES,
        issued_at=1_752_643_200,
    )
    envelope = json.loads(body)
    envelope["payload"]["job"]["module"] = "os"
    tampered = json.dumps(envelope).encode()

    def must_not_resolve(*_args, **_kwargs):
        raise AssertionError("dynamic import ran before signature verification")

    monkeypatch.setattr(JobClassResolver, "resolve", must_not_resolve)

    with pytest.raises(QueueException, match="signature verification failed"):
        SignedJsonJobSerializer.deserialize(
            tampered,
            signing_keys=_KEYS,
            allowed_prefixes=_PREFIXES,
            now=1_752_643_201,
        )


def test_job_class_resolver_rejects_modules_outside_allowlist():
    with pytest.raises(QueueException, match="outside the configured allowlist"):
        JobClassResolver.resolve(
            "os",
            "PathLike",
            allowed_prefixes=_PREFIXES,
        )


def test_job_class_resolver_uses_segment_aware_prefix_matching():
    with pytest.raises(QueueException, match="outside the configured allowlist"):
        JobClassResolver.resolve(
            f"{__name__}_evil",
            "ExampleJob",
            allowed_prefixes=(__name__,),
        )


def test_signed_json_rejects_non_primitive_constructor_state():
    job = ExampleJob(42)
    job.unsafe = object()

    with pytest.raises(QueueException, match="JSON primitives"):
        SignedJsonJobSerializer.serialize(
            _payload(job),
            signing_key_id=_KID,
            signing_keys=_KEYS,
            allowed_prefixes=_PREFIXES,
            issued_at=1_752_643_200,
        )


@pytest.mark.parametrize(
    "forbidden_key",
    [
        "password",
        "access_token",
        "refresh_token",
        "api_key",
        "client_secret",
        "authorization",
        "session_cookie",
    ],
)
def test_signed_json_rejects_secret_bearing_constructor_keys(
    forbidden_key,
):
    job = ExampleJob(42)
    job.payload = {"nested": {forbidden_key: "sensitive"}}

    with pytest.raises(QueueException, match="forbidden secret-bearing key"):
        SignedJsonJobSerializer.serialize(
            _payload(job),
            signing_key_id=_KID,
            signing_keys=_KEYS,
            allowed_prefixes=_PREFIXES,
        )


def test_signing_key_must_have_cryptographic_length():
    with pytest.raises(QueueException, match="at least 32 bytes"):
        SignedJsonJobSerializer.serialize(
            _payload(ExampleJob(42)),
            signing_key_id=_KID,
            signing_keys={_KID: "short"},
            allowed_prefixes=_PREFIXES,
            issued_at=1_752_643_200,
        )


def test_rotation_accepts_previous_kid_and_unknown_kid_is_rejected():
    body = SignedJsonJobSerializer.serialize(
        _payload(ExampleJob(42)),
        signing_key_id="old",
        signing_keys={"old": _OLD_KEY},
        allowed_prefixes=_PREFIXES,
        issued_at=1_752_643_200,
    )
    assert (
        SignedJsonJobSerializer.inspect(
            body,
            signing_keys={_KID: _KEY, "old": _OLD_KEY},
            now=1_752_643_201,
        )["job_id"]
        == _JOB_ID
    )
    with pytest.raises(QueueException, match="Unknown AMQP signing key id"):
        SignedJsonJobSerializer.inspect(
            body,
            signing_keys=_KEYS,
            now=1_752_643_201,
        )


@pytest.mark.parametrize(
    ("issued_at", "not_before", "expires_at", "now", "message"),
    [
        (200, 200, 800, 100, "issued in the future"),
        (100, 200, 800, 100, "not yet executable"),
        (100, 100, 200, 300, "has expired"),
    ],
)
def test_temporal_guards_reject_future_early_and_expired_envelopes(
    issued_at,
    not_before,
    expires_at,
    now,
    message,
):
    body = SignedJsonJobSerializer.serialize(
        _payload(ExampleJob(42)),
        signing_key_id=_KID,
        signing_keys=_KEYS,
        allowed_prefixes=_PREFIXES,
        issued_at=issued_at,
        not_before=not_before,
        expires_at=expires_at,
        ttl_seconds=300,
    )
    with pytest.raises(QueueException, match=message):
        SignedJsonJobSerializer.inspect(
            body,
            signing_keys=_KEYS,
            now=now,
            clock_skew_seconds=0,
        )


def test_canonical_hash_accepts_jsonb_reserialize_but_rejects_mutation():
    body = SignedJsonJobSerializer.serialize(
        _payload(ExampleJob(42)),
        signing_key_id=_KID,
        signing_keys=_KEYS,
        allowed_prefixes=_PREFIXES,
        issued_at=1_752_643_200,
    )
    reserialized = json.dumps(json.loads(body), indent=2)
    assert SignedJsonJobSerializer.canonical_envelope_sha256(
        body
    ) == SignedJsonJobSerializer.canonical_envelope_sha256(reserialized)

    mutated = json.loads(body)
    mutated["payload"]["queue"] = "connector"
    assert SignedJsonJobSerializer.canonical_envelope_sha256(
        body
    ) != SignedJsonJobSerializer.canonical_envelope_sha256(mutated)


def test_wire_identifiers_and_db_ids_are_bounded_and_canonical():
    invalid = _payload(ExampleJob(42))
    invalid["job_id"] = "job-1"
    with pytest.raises(QueueException, match="canonical UUID"):
        SignedJsonJobSerializer.serialize(
            invalid,
            signing_key_id=_KID,
            signing_keys=_KEYS,
            allowed_prefixes=_PREFIXES,
        )

    invalid = _payload(ExampleJob(42))
    invalid["queue"] = "q" * 101
    with pytest.raises(QueueException, match="exceeds 100"):
        SignedJsonJobSerializer.serialize(
            invalid,
            signing_key_id=_KID,
            signing_keys=_KEYS,
            allowed_prefixes=_PREFIXES,
        )

    for field in ("db_job_id", "_tenant"):
        invalid = _payload(ExampleJob(42))
        invalid[field] = 0
        with pytest.raises(QueueException, match="positive integer"):
            SignedJsonJobSerializer.serialize(
                invalid,
                signing_key_id=_KID,
                signing_keys=_KEYS,
                allowed_prefixes=_PREFIXES,
            )


def test_sync_handle_is_rejected_by_amqp_contract():
    payload = _payload(SyncJob())
    with pytest.raises(QueueException, match="handle must be async"):
        SignedJsonJobSerializer.serialize(
            payload,
            signing_key_id=_KID,
            signing_keys=_KEYS,
            allowed_prefixes=_PREFIXES,
        )
