"""Ambient app context rides the signed envelope beside ``_tenant``.

A fact the dispatcher held (who acted: a person, a token, a rule) must reach
the worker, or a rule's write lands as "system" on the far side of the queue.
"""

from __future__ import annotations

import asyncio
from contextvars import ContextVar

from cara.context import AMBIENT_KEY, Ambient
from cara.queues.contracts import Queueable, ShouldQueue
from cara.queues.middleware import run_through_middleware_async
from cara.queues.serializers import SignedJsonJobSerializer

_KEY = "ambient-queue-test-key-" * 3
_KID = "current"
_ACTOR: ContextVar[dict | None] = ContextVar("test_ambient_actor", default=None)

Ambient.register(
    "test_actor",
    _ACTOR,
    encode=lambda value: {"kind": str(value["kind"]), "id": int(value["id"])},
    decode=lambda raw: {"kind": str(raw["kind"]), "id": int(raw["id"])},
)


class AmbientJob(ShouldQueue, Queueable):
    seen: list = []

    def __init__(self, item_id: int):
        self.item_id = int(item_id)
        super().__init__()
        self.queue = "sync"
        self.priority = "default"

    async def handle(self):
        AmbientJob.seen.append(_ACTOR.get())


def _payload(job, ambient):
    return {
        "obj": job,
        "args": (),
        "callback": "handle",
        "created": "2026-11-14T00:00:00Z",
        "job_id": "33333333-3333-4333-8333-333333333333",
        "db_job_id": 12,
        "timeout_seconds": 300,
        "attempts": 0,
        "throttle_attempts": 0,
        "_otel": {},
        AMBIENT_KEY: ambient,
        "_tenant": 7,
        "_tenant_mode": "tenant",
        "queue": job.queue,
        "priority": job.priority,
        "dispatched_at": "2026-11-14T00:00:00Z",
        "replay_of": None,
    }


def test_capture_reads_only_set_vars() -> None:
    assert "test_actor" not in Ambient.capture()
    token = _ACTOR.set({"kind": "rule", "id": 5})
    try:
        assert Ambient.capture()["test_actor"] == {"kind": "rule", "id": 5}
    finally:
        _ACTOR.reset(token)


def test_the_envelope_signs_and_returns_the_ambient_block() -> None:
    body = SignedJsonJobSerializer.serialize(
        _payload(AmbientJob(1), {"test_actor": {"kind": "token", "id": 9}}),
        signing_key_id=_KID,
        signing_keys={_KID: _KEY},
        allowed_prefixes=("tests.queues",),
        issued_at=1_763_078_400,
    )
    decoded = SignedJsonJobSerializer.deserialize(
        body,
        signing_keys={_KID: _KEY},
        allowed_prefixes=("tests.queues",),
        now=1_763_078_401,
    )
    assert decoded[AMBIENT_KEY] == {"test_actor": {"kind": "token", "id": 9}}


def test_the_worker_pipeline_restores_it_around_the_body() -> None:
    AmbientJob.seen = []
    job = AmbientJob(2)
    job._tenant_id = 7
    job._tenant_mode = "tenant"
    job._ambient = {"test_actor": {"kind": "rule", "id": 41}, "unknown": 1}

    async def handler(j):
        await j.handle()

    asyncio.run(run_through_middleware_async(job, handler))
    assert AmbientJob.seen == [{"kind": "rule", "id": 41}]
    assert _ACTOR.get() is None


def test_an_undecodable_value_is_skipped_not_fatal() -> None:
    with Ambient.restored({"test_actor": {"kind": "rule"}}):
        assert _ACTOR.get() is None
