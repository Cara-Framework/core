"""MakesRetryable — retry classification and backoff behavior.

Pins the contract the products lean on: transient database-driver
exceptions (psycopg2 ``OperationalError`` / ``InterfaceError``) are in the
DEFAULT retryable set when the driver is installed, non-retryable
exceptions surface immediately, and a missing driver degrades to the base
tuple instead of erroring.

Also pins the two defects the resolver used to hide: a subclass could only
ever RAISE its attempt budget (``max(self.MAX_RETRY_ATTEMPTS,
config)``), so a job that deliberately narrowed to one attempt because its
body is a non-idempotent external call issued that call three times; and
the in-job backoff carried no jitter, so a fleet that failed on the same
downstream blip retried in lockstep and recreated the spike.
"""

from __future__ import annotations

import asyncio
import builtins
import importlib

import pytest

from cara.queues.retry.MakesRetryable import MakesRetryable


class _Job(MakesRetryable):
    pass


def _failing_then_ok(exc: Exception, failures: int):
    calls = {"n": 0}

    async def _body():
        calls["n"] += 1
        if calls["n"] <= failures:
            raise exc
        return "ok"

    return _body, calls


@pytest.mark.asyncio
async def test_psycopg2_transients_are_retryable_by_default(monkeypatch) -> None:
    psycopg2 = pytest.importorskip("psycopg2")

    assert psycopg2.OperationalError in _Job.RETRYABLE_EXCEPTIONS
    assert psycopg2.InterfaceError in _Job.RETRYABLE_EXCEPTIONS

    # A dropped connection retries and then succeeds — without sleeping
    # for real.
    async def _no_sleep(_secs: float) -> None:
        return None

    monkeypatch.setattr(asyncio, "sleep", _no_sleep)
    body, calls = _failing_then_ok(psycopg2.OperationalError("gone away"), failures=1)
    assert await _Job().wrap_with_retry(body, max_attempts=3, base_delay=0.0) == "ok"
    assert calls["n"] == 2


@pytest.mark.asyncio
async def test_non_retryable_exception_raises_immediately(monkeypatch) -> None:
    slept = {"n": 0}

    async def _count_sleep(_secs: float) -> None:
        slept["n"] += 1

    monkeypatch.setattr(asyncio, "sleep", _count_sleep)
    body, calls = _failing_then_ok(ValueError("bad input"), failures=1)

    with pytest.raises(ValueError):
        await _Job().wrap_with_retry(body, max_attempts=3, base_delay=0.0)

    assert calls["n"] == 1, "non-retryable must not re-run the body"
    assert slept["n"] == 0, "non-retryable must not back off"


class _NarrowJob(MakesRetryable):
    """A body that must NOT be re-run: one attempt, deliberately."""

    MAX_RETRY_ATTEMPTS = 1


@pytest.mark.asyncio
async def test_a_narrowing_subclass_override_is_honoured(monkeypatch) -> None:
    """``MAX_RETRY_ATTEMPTS = 1`` means one call, not ``max(1, config)``.

    Pre-fix the resolver recognised an override by comparing the attribute
    against a hand-written copy of its own default and then took the
    MAXIMUM of the two, so a narrowed budget silently widened back to the
    configured 3 — three calls to a non-idempotent external API.
    """

    async def _no_sleep(_secs: float) -> None:
        return None

    monkeypatch.setattr(asyncio, "sleep", _no_sleep)
    body, calls = _failing_then_ok(ConnectionError("upstream refused"), failures=5)

    with pytest.raises(ConnectionError):
        await _NarrowJob().wrap_with_retry(body)

    assert calls["n"] == 1, "a narrowed attempt budget must narrow"


class _FlatBackoffJob(MakesRetryable):
    """Flat schedule so every sleep would be identical without jitter."""

    MAX_RETRY_ATTEMPTS = 5
    BASE_RETRY_DELAY = 1.0
    RETRY_BACKOFF_MULTIPLIER = 1.0


@pytest.mark.asyncio
async def test_the_backoff_is_jittered(monkeypatch) -> None:
    """Identical geometric delays must not produce identical sleeps.

    Policy.py owns ``DEFAULT_RETRY_JITTER_FRACTION`` and the reason for it;
    the queue-republish path applied it and this in-job path did not. With
    a flat multiplier every sleep was exactly ``BASE_RETRY_DELAY`` — N
    workers marching back into the recovering dependency on the same
    second.
    """
    slept: list[float] = []

    async def _record(secs: float) -> None:
        slept.append(secs)

    monkeypatch.setattr(asyncio, "sleep", _record)
    body, _calls = _failing_then_ok(ConnectionError("blip"), failures=10)

    with pytest.raises(ConnectionError):
        await _FlatBackoffJob().wrap_with_retry(body)

    assert len(slept) == 4, "four gaps between five attempts"
    assert len(set(slept)) > 1, "an unjittered schedule retries in lockstep"
    assert all(0.75 <= value <= 1.25 for value in slept), (
        "jitter must stay inside the schedule's intent (±25%)"
    )


@pytest.mark.asyncio
async def test_a_zero_jitter_fraction_restores_the_exact_schedule(monkeypatch) -> None:
    """``retry_jitter_fraction = 0`` is the documented opt-out."""

    class _Exact(_FlatBackoffJob):
        retry_jitter_fraction = 0

    slept: list[float] = []

    async def _record(secs: float) -> None:
        slept.append(secs)

    monkeypatch.setattr(asyncio, "sleep", _record)
    body, _calls = _failing_then_ok(ConnectionError("blip"), failures=10)

    with pytest.raises(ConnectionError):
        await _Exact().wrap_with_retry(body)

    assert slept == [1.0, 1.0, 1.0, 1.0]


def test_missing_driver_degrades_to_the_base_tuple(monkeypatch) -> None:
    """With psycopg2 unimportable the module still imports, minus the
    driver classes — a non-Postgres install is legitimate, not an error."""
    real_import = builtins.__import__

    def _no_psycopg2(name, *args, **kwargs):
        if name == "psycopg2" or name.startswith("psycopg2."):
            raise ImportError("no driver in this install")
        return real_import(name, *args, **kwargs)

    monkeypatch.delitem(importlib.sys.modules, "psycopg2", raising=False)
    monkeypatch.setattr(builtins, "__import__", _no_psycopg2)

    module = importlib.import_module("cara.queues.retry.MakesRetryable")
    try:
        reloaded = importlib.reload(module)
        assert (
            ConnectionError,
            TimeoutError,
            asyncio.TimeoutError,
            OSError,
        ) == reloaded.MakesRetryable.RETRYABLE_EXCEPTIONS
    finally:
        monkeypatch.undo()
        importlib.reload(module)
