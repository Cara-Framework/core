"""MakesRetryable — retry classification and backoff behavior.

Pins the contract the products lean on: transient database-driver
exceptions (psycopg2 ``OperationalError`` / ``InterfaceError``) are in the
DEFAULT retryable set when the driver is installed, non-retryable
exceptions surface immediately, and a missing driver degrades to the base
tuple instead of erroring.
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
        assert reloaded.MakesRetryable.RETRYABLE_EXCEPTIONS == (
            ConnectionError,
            TimeoutError,
            asyncio.TimeoutError,
            OSError,
        )
    finally:
        monkeypatch.undo()
        importlib.reload(module)
