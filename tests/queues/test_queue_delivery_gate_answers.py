"""A gate that could not answer never says "already done" or "full".

The cooldown take-over and the concurrency cap each had one word for two
different facts. Only the real answer may settle or hold a job.
"""

from __future__ import annotations

import asyncio
import sys

import pendulum
import pytest
from redis.exceptions import ConnectionError as RedisConnectionError

from cara.exceptions import CacheConfigurationException, IdempotencyGateFaultException
from cara.queues.contracts import Queueable, ShouldQueue
from cara.queues.idempotency import MakesIdempotentBase
from cara.queues.middleware import ConcurrencyLimited
from cara.queues.middleware.ConcurrencyExceeded import ConcurrencyExceeded
from cara.queues.serializers import SignedJsonJobSerializer
from cara.testing.fakes.CacheFake import CacheFake

_cooldown = sys.modules["cara.queues.idempotency.ClaimsSourceCooldown"]
_orchestrator = sys.modules["cara.queues.idempotency.MakesIdempotentBase"]

_WINDOW = "collection_cooldown:orders"


class _UnreadableWindow(CacheFake):
    def __init__(self, error: Exception) -> None:
        super().__init__()
        self.error = error

    def get(self, *_args, **_kwargs):
        raise self.error


class _RedeliveredPoll(MakesIdempotentBase):
    source_cooldown_minutes = {"orders": 5}
    idempotency_cache_results = False

    def __init__(self) -> None:
        super().__init__()
        self.source = "orders"
        self._db_job_id = 42

    def should_execute_based_on_lifecycle(self) -> bool:
        return self._claim_source_cooldown()


def _run(monkeypatch, cache) -> list[str]:
    monkeypatch.setattr(_cooldown, "Cache", cache)
    monkeypatch.setattr(_orchestrator, "Cache", cache)
    monkeypatch.setattr(_cooldown, "config", lambda _key, default=None: default)
    cache.add(_WINDOW, "2026-11-09T08:00:00+00:00|42", 300)
    ran: list[str] = []

    async def body():
        ran.append("body")

    asyncio.run(_RedeliveredPoll().wrap_with_idempotency(body))
    return ran


def test_a_window_nobody_can_read_comes_back_instead_of_settling(monkeypatch):
    cache = _UnreadableWindow(CacheConfigurationException("Unsigned cache value"))

    with pytest.raises(IdempotencyGateFaultException) as raised:
        _run(monkeypatch, cache)

    assert str(raised.value).startswith(IdempotencyGateFaultException.REASON)
    assert raised.value.retry_after == MakesIdempotentBase.GATE_FAULT_RETRY_SECONDS


def test_a_bug_in_the_take_over_is_a_failure_not_a_skip(monkeypatch):
    with pytest.raises(TypeError):
        _run(monkeypatch, _UnreadableWindow(TypeError("bad stamp")))


class _Full:
    def eval(self, *_args, **_kwargs):
        return 0


class _Down:
    def eval(self, *_args, **_kwargs):
        raise RedisConnectionError("Error 61 connecting to redis.")


def _limiter(client):
    class _Driver:
        @staticmethod
        def connection():
            return client

    class _Cache:
        @staticmethod
        def driver():
            return _Driver()

    class _Limited(ConcurrencyLimited):
        _resolve_cache = staticmethod(_Cache)

    return _Limited(max_concurrent=8, key="provider", retry_delay=0)


def _refusal(client) -> str:
    async def body(_job):
        raise AssertionError("no slot, no body")

    with pytest.raises(ConcurrencyExceeded) as refused:
        asyncio.run(_limiter(client).handle(object(), body))
    return str(refused.value)


def test_a_real_full_ceiling_keeps_its_own_words():
    assert _refusal(_Full()) == "Concurrency limit (8) reached for provider"


def test_an_outage_names_itself_with_the_fixed_prefix():
    reason = _refusal(_Down())

    assert reason.startswith(f"{ConcurrencyLimited.BACKEND_FAULT_REASON} for provider")


_KID = "clock-current"
_KEY = "clock-signing-key-------" * 3


class QueueDeliveryClockJob(ShouldQueue, Queueable):
    async def handle(self):
        return None


def test_a_release_time_already_past_is_released_when_it_is_issued():
    """A retry due now, whose transaction outlasted a second before signing."""
    issued = pendulum.datetime(2026, 11, 9, 12, 0, 3, tz="UTC")
    asked = issued.subtract(seconds=2, microseconds=500_000)

    body = SignedJsonJobSerializer.serialize(
        {
            "obj": QueueDeliveryClockJob(),
            "args": (),
            "callback": "handle",
            "created": issued.to_iso8601_string(),
            "job_id": "6f1f4bb7-5c0f-4d3a-9d43-2a3f1a5e3f10",
            "db_job_id": 9,
            "timeout_seconds": 300,
            "attempts": 1,
            "throttle_attempts": 0,
            "_otel": {},
            "_tenant_mode": "tenant",
            "_tenant": 5,
            "queue": "sync",
            "priority": "default",
            "dispatched_at": issued.to_iso8601_string(),
            "replay_of": None,
            "unique_key": None,
        },
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

    assert envelope["not_before"] == envelope["issued_at"]


def test_a_duration_delay_never_rounds_the_release_down(monkeypatch):
    from cara.queues.drivers import _AMQPDelivery as delivery

    frozen = pendulum.datetime(2026, 11, 9, 12, 0, 0, tz="UTC")
    monkeypatch.setattr(delivery.pendulum_module, "now", lambda **_k: frozen)
    asked: list = []

    class _Driver:
        options: dict = {}

        @staticmethod
        def schedule(_job, when, _options):
            asked.append(when)
            return "id"

    delivery._amqp_delivery_later(_Driver(), pendulum.duration(seconds=1.5), object())

    assert asked == [frozen.add(seconds=2)]
