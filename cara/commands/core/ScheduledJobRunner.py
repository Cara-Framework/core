"""Run one scheduled job inline exactly as the scheduler does.

The scheduler (``_ScheduleRegistration``) and every operator command that
runs a schedule entry by hand (``maintenance:reconcile``) must agree on four
things, and each product used to restate them — one of them wrongly:

* **instantiation** — the entry's ``kwargs`` reach ``__init__``; a
  ``TypeError`` (an inherited signature) falls back to the container;
* **the idempotency opt-out** — a recurring no-arg job hashes to one stable
  key, so the 24h result cache would replay its first tick for a day;
* **``handle()`` arguments** — entry ``kwargs`` first, then DI by annotation;
* **the middleware chain** under the job's tenancy scope — overlap locks,
  timeouts, metrics. A hand-run that called ``handle()`` directly skipped
  every one of them and could overlap the scheduled tick of the same job.

The scheduler adds only what is scheduler-specific on top (tick tracking, the
throttle retry, the APScheduler wrapper).
"""

from __future__ import annotations

import contextlib
import inspect
from typing import Any

from cara.context import Tenancy
from cara.queues.middleware import run_through_middleware_async

#: The narrow failures tolerated around optional setup (the idempotency
#: opt-out, a DI resolution) — a real programming error still surfaces.
OPTIONAL_SETUP_ERRORS = (OSError, RuntimeError, AttributeError, ConnectionError)


def instantiate_scheduled_job(
    application: Any, job_class: type, kwargs: dict[str, Any]
) -> Any:
    """Build the job the way the scheduler does and opt it out of the result cache."""
    try:
        instance = job_class(**kwargs) if kwargs else application.make(job_class)
    except TypeError:
        instance = application.make(job_class)
    with contextlib.suppress(*OPTIONAL_SETUP_ERRORS):
        instance.idempotency_cache_results = False
    return instance


def scheduled_handle_kwargs(
    application: Any, handle: Any, kwargs: dict[str, Any]
) -> dict[str, Any]:
    """``handle()``'s arguments: the entry's ``kwargs`` first, then DI."""
    resolved: dict[str, Any] = {}
    for name, parameter in inspect.signature(handle).parameters.items():
        if name in kwargs:
            resolved[name] = kwargs[name]
        elif parameter.annotation is not inspect.Parameter.empty:
            with contextlib.suppress(*OPTIONAL_SETUP_ERRORS):
                resolved[name] = application.make(parameter.annotation)
    return resolved


async def run_scheduled_job_inline(
    application: Any,
    job_class: type,
    kwargs: dict[str, Any] | None = None,
    *,
    tenant_id: Any = None,
) -> bool:
    """Run one schedule entry through the job's middleware; ``False`` when the
    job has no ``handle()``. Exceptions propagate to the caller."""
    entry_kwargs = dict(kwargs or {})
    instance = instantiate_scheduled_job(application, job_class, entry_kwargs)
    handle = getattr(instance, "handle", None)
    if handle is None:
        return False
    handle_kwargs = scheduled_handle_kwargs(application, handle, entry_kwargs)

    async def _invoke(_job: Any) -> Any:
        result = handle(**handle_kwargs)
        if inspect.isawaitable(result):
            return await result
        return result

    central = bool(getattr(job_class, "central_job", False))
    scope = Tenancy.central() if central else Tenancy.as_tenant(tenant_id)
    with scope:
        await run_through_middleware_async(instance, _invoke)
    return True


__all__ = [
    "OPTIONAL_SETUP_ERRORS",
    "instantiate_scheduled_job",
    "run_scheduled_job_inline",
    "scheduled_handle_kwargs",
]
