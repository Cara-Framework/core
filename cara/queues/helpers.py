"""Reliability helpers for queue-side dispatch.

These wrap ``Bus.dispatch`` with concerns every app eventually
re-implements: retry on transient failures (network blips, broker
reconnects), structured logging on each attempt, and a single
``raise`` on persistent failure that fits the queue runner's retry
contract.

Generic, domain-free — apps bind their own jobs and routing keys.
"""

import asyncio
from typing import Any, Optional

from cara.facades import Log


async def safe_dispatch(
    job: Any,
    routing_key: Optional[str] = None,
    delay: Optional[float] = None,
    max_retries: int = 3,
) -> bool:
    """Dispatch a job to the queue with retry on transient failures.

    Mirrors Laravel's ``Bus::dispatch`` semantics but adds explicit
    retry handling for broker-level transients (a dropped AMQP
    channel, a Redis blip during failover, etc.). Each attempt is
    logged via ``cara.facades.Log`` so operators see backoff in
    real time; the final attempt re-raises so the calling job/listener
    can decide whether to swallow or escalate.

    Args:
        job: The job instance to dispatch (any ``ShouldQueue``).
        routing_key: Optional topic-exchange routing key.
        delay: Optional delay in seconds before the worker picks the job.
        max_retries: Maximum retry attempts before the last error
            is re-raised. Default 3 — good fit for a 1s/2s/3s
            linear-backoff window covering most transient broker hiccups.

    Returns:
        ``True`` if dispatch succeeded on any attempt.

    Raises:
        Exception: Re-raised from the final attempt when every
            retry failed. The caller decides whether to swallow
            (best-effort dispatch) or propagate (retryable job).
    """
    # Lazy import: cara.queues.Bus is part of the same package and
    # importing it at module top would force ``cara.queues.__init__``
    # callers to pay for the queue runtime even if they only ever use
    # ``safe_dispatch``.  This pattern matches the original app helper.
    from cara.queues.Bus import Bus

    last_exc: Optional[Exception] = None
    for attempt in range(max_retries):
        try:
            kwargs: dict = {}
            if routing_key:
                kwargs["routing_key"] = routing_key
            if delay:
                kwargs["delay"] = delay

            await Bus.dispatch(job, **kwargs)
            return True

        except Exception as e:
            last_exc = e
            if attempt < max_retries - 1:
                Log.warning(
                    f"Dispatch attempt {attempt + 1} failed for "
                    f"{job.__class__.__name__}: {e}. Retrying…"
                )
                # Linear backoff — keeps the total tail bounded for the
                # default 3-attempt window (1s + 2s = 3s max wait).
                await asyncio.sleep(1 * (attempt + 1))
            else:
                Log.error(
                    f"Failed to dispatch {job.__class__.__name__} after "
                    f"{max_retries} attempts: {e}"
                )
                raise

    # Defensive: the loop only exits via ``return True`` or ``raise``,
    # but mypy + linters appreciate the explicit terminal path.
    if last_exc is not None:
        raise last_exc
    return False


__all__ = ["safe_dispatch"]
