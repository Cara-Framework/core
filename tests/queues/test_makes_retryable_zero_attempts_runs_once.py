"""A zero retry budget still runs the job body once.

Live defect: ``wrap_with_retry`` looped ``range(attempts)``, so
``JOBS_RETRY_MAX_ATTEMPTS=0`` (an operator turning retries off) — or a
``MAX_RETRY_ATTEMPTS = 0`` / ``max_attempts=0`` override — never called the
body and raised "Unexpected exit from retry loop" for every job.
"""

from __future__ import annotations

import pytest

from cara.queues.retry.MakesRetryable import MakesRetryable


class _ZeroBudgetJob(MakesRetryable):
    MAX_RETRY_ATTEMPTS = 0


@pytest.mark.asyncio
async def test_zero_budget_runs_the_body_once_and_returns_its_result() -> None:
    calls = []

    async def _body():
        calls.append(1)
        return "done"

    assert await _ZeroBudgetJob().wrap_with_retry(_body) == "done"
    assert calls == [1]


@pytest.mark.asyncio
async def test_zero_budget_does_not_retry_a_transient_failure() -> None:
    calls = []

    async def _body():
        calls.append(1)
        raise ConnectionError("blip")

    with pytest.raises(ConnectionError):
        await _ZeroBudgetJob().wrap_with_retry(_body, max_attempts=0)
    assert calls == [1]
