"""``ExecutionContext.scope_cache`` — one memo per running job or request.

Products hand-rolled this (Synkronus ``BundleCost``'s recipe memo) after an
id-keyed module dict served a retried job an earlier run's bundle recipe: a
stale cost, so a wrong margin floor. The framework now owns the primitive.
"""

from __future__ import annotations

import asyncio
from contextvars import copy_context

from cara.context import ExecutionContext


def test_no_scope_means_no_memo():
    assert ExecutionContext.scope_cache("recipes") is None


def test_one_scope_shares_one_memo_per_name():
    with ExecutionContext.queue(job_id="job-1"):
        memo = ExecutionContext.scope_cache("recipes")
        memo["k"] = 1
        assert ExecutionContext.scope_cache("recipes") is memo
        assert ExecutionContext.scope_cache("other") == {}


def test_a_retry_with_the_same_job_id_starts_empty():
    def run_job():
        with ExecutionContext.queue(job_id="job-1"):
            memo = ExecutionContext.scope_cache("recipes")
            seen = dict(memo)
            memo["k"] = "first run"
            return seen

    # Each delivery runs in its own context copy, as a worker runs it.
    assert copy_context().run(run_job) == {}
    assert copy_context().run(run_job) == {}


def test_a_new_scope_id_in_the_same_context_starts_empty():
    with ExecutionContext.queue(job_id="job-1"):
        ExecutionContext.scope_cache("recipes")["k"] = 1
    with ExecutionContext.queue(job_id="job-2"):
        assert ExecutionContext.scope_cache("recipes") == {}


def test_concurrent_requests_never_share_a_memo():
    async def request(correlation_id: str) -> dict:
        ExecutionContext.set_correlation_id(correlation_id)
        memo = ExecutionContext.scope_cache("recipes")
        memo[correlation_id] = True
        await asyncio.sleep(0)
        return dict(ExecutionContext.scope_cache("recipes"))

    async def both():
        return await asyncio.gather(request("same-header"), request("same-header"))

    first, second = asyncio.run(both())
    assert first == {"same-header": True}
    assert second == {"same-header": True}


def test_limit_starts_the_memo_over():
    with ExecutionContext.queue(job_id="sweep"):
        memo = ExecutionContext.scope_cache("recipes", limit=2)
        memo.update(a=1, b=2)
        again = ExecutionContext.scope_cache("recipes", limit=2)
        assert again is memo and again == {}
