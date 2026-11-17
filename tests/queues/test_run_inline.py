"""An in-process job run goes through the job's own middleware."""

from __future__ import annotations

import pytest

from cara.queues.middleware import run_inline


class _Refuse:
    async def handle(self, job, next_fn):
        raise RuntimeError("budget shed")


class _Record:
    def __init__(self, seen: list[str]) -> None:
        self.seen = seen

    async def handle(self, job, next_fn):
        self.seen.append("middleware")
        return await next_fn(job)


class _Job:
    def __init__(self, middleware: list) -> None:
        self._middleware = middleware
        self.ran = False

    def middleware(self) -> list:
        return self._middleware

    async def handle(self) -> str:
        self.ran = True
        return "done"


class _SyncJob:
    def handle(self) -> str:
        return "sync"


@pytest.mark.asyncio
async def test_the_chain_runs_before_the_body() -> None:
    seen: list[str] = []
    job = _Job([_Record(seen)])
    assert await run_inline(job) == "done"
    assert seen == ["middleware"] and job.ran


@pytest.mark.asyncio
async def test_a_refusing_middleware_keeps_the_body_from_running() -> None:
    job = _Job([_Refuse()])
    with pytest.raises(RuntimeError, match="budget shed"):
        await run_inline(job)
    assert job.ran is False


@pytest.mark.asyncio
async def test_a_job_without_middleware_or_with_a_sync_handle_runs() -> None:
    assert await run_inline(_SyncJob()) == "sync"
