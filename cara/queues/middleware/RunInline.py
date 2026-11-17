"""Run a job inside the current process THROUGH its middleware chain.

``job.handle()`` alone runs the body and skips ``middleware()`` — and with it
every rate limit, concurrency ceiling, budget shed and overlap lock the job
declares. A command's ``--sync`` run, or a job driving another job inline,
then competes uncapped with every queue-delivered run of the same work (both
products had such call sites). :func:`run_inline` is the one in-process
entry: the same chain the worker runs, inside the caller's established
tenancy scope (it does not restore a serialized one — see
:func:`run_middleware_chain_async`).
"""

from __future__ import annotations

import inspect
from typing import Any

from cara.queues.middleware import run_middleware_chain_async


async def run_inline(job: Any) -> Any:
    """Run ``job.handle()`` through ``job.middleware()``; returns its result.

    A middleware refusal (``JobThrottledException``, ``ConcurrencyExceeded``)
    propagates to the caller, who decides whether to wait or give up.
    """

    async def _handle(concrete_job: Any) -> Any:
        result = concrete_job.handle()
        if inspect.isawaitable(result):
            return await result
        return result

    return await run_middleware_chain_async(job, _handle)


__all__ = ["run_inline"]
