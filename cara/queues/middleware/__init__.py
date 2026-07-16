"""Queue job middleware system.

Jobs can declare a ``middleware()`` method returning a list of middleware
instances. Each middleware has an async ``handle(job, next_fn)`` method that
can short-circuit execution (return ``None``), record metrics, or wrap the
call with a lock / rate limit / exception throttle.

Wire-up lives in :mod:`cara.queues.Bus`, which runs jobs through
:func:`run_through_middleware_async` during sync-context dispatch.
"""

from __future__ import annotations

import asyncio
from typing import Any
from collections.abc import Callable

from .ConcurrencyLimited import ConcurrencyLimited
from .RateLimited import RateLimited, WithoutOverlapping
from .ThrottlesExceptions import ThrottlesExceptions

__all__ = [
    "ConcurrencyLimited",
    "RateLimited",
    "ThrottlesExceptions",
    "WithoutOverlapping",
    "run_through_middleware",
    "run_through_middleware_async",
]


def _build_chain(job, handler: Callable) -> Callable:
    """Build a callable chain from the middleware list + innermost handler.

    Middleware ordering: the first entry in ``job.middleware()`` runs
    outermost (i.e. first), the handler runs innermost.
    """
    middleware_list = job.middleware() if hasattr(job, "middleware") else []

    chain = handler
    for mw in reversed(middleware_list):
        prev = chain

        def _link(j: Any, m: Any = mw, p: Callable = prev) -> Any:
            """One link in the middleware chain — invokes ``m.handle(job, next)``.

            ``m`` and ``p`` are pinned via default args so each loop
            iteration captures the current ``mw`` / ``prev`` rather than
            the loop's last-assigned values (the closure-over-loop-var bug
            ``lambda`` invites here).
            """
            return m.handle(j, p)

        chain = _link
    return chain


_TENANT_UNSET = object()


async def run_through_middleware_async(job, handler: Callable) -> Any:
    """Run an async handler through the job's middleware pipeline.

    ``handler`` must be an async callable ``(job) -> coroutine``. Each
    middleware's ``handle`` is expected to be async as well; middleware may
    short-circuit by returning ``None`` without invoking ``next_fn``.

    Tenancy: a job consumed off the queue carries the dispatcher's
    tenant scope as ``_tenant_id`` (stamped by the worker from the
    payload's ``_tenant``); the whole pipeline runs under
    ``Tenancy.as_tenant`` so TenantScope filters exactly as it did at
    dispatch time. An inline (sync-mode) dispatch never sets the attr —
    the job simply inherits the caller's live context.
    """
    from cara.context import Tenancy

    tenant_id = getattr(job, "_tenant_id", _TENANT_UNSET)
    tenant_mode = getattr(job, "_tenant_mode", _TENANT_UNSET)
    is_central_job = bool(getattr(job, "central_job", False))

    if tenant_mode is _TENANT_UNSET and tenant_id is _TENANT_UNSET:
        if is_central_job:
            if not Tenancy.is_central():
                raise RuntimeError(
                    f"Central job {job.__class__.__name__} requires "
                    "Tenancy.central()."
                )
            scope = Tenancy.central()
        else:
            if not Tenancy.is_tenant():
                raise RuntimeError(
                    f"Ordinary job {job.__class__.__name__} requires "
                    "an active tenant."
                )
            scope = Tenancy.as_tenant(Tenancy.id())
    elif tenant_mode == "central":
        if not is_central_job or tenant_id is not None:
            raise RuntimeError("Signed central queue mode does not match job marker.")
        scope = Tenancy.central()
    elif tenant_mode == "tenant":
        if is_central_job or tenant_id is None or tenant_id is _TENANT_UNSET:
            raise RuntimeError("Signed tenant queue mode does not match job marker.")
        scope = Tenancy.as_tenant(tenant_id)
    else:
        raise RuntimeError("Queue job has incomplete or invalid tenant metadata.")

    with scope:
        chain = _build_chain(job, handler)
        result = chain(job)
        if asyncio.iscoroutine(result):
            return await result
        return result


def run_through_middleware(job, handler: Callable) -> Any:
    """Synchronous wrapper — runs the async pipeline on a fresh event loop.

    Use only when the surrounding code is sync and ``handler`` is sync too.
    When in an async context, call :func:`run_through_middleware_async`
    directly instead.
    """

    async def async_handler(j):
        res = handler(j)
        if asyncio.iscoroutine(res):
            return await res
        return res

    return asyncio.run(run_through_middleware_async(job, async_handler))
