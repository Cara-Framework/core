"""Tenancy context — the single source TenantScope and the queue rail read.

Pins the three contracts:
1. ContextVar semantics — scoped ``as_tenant``/``central`` blocks nest and
   restore, and the value crosses ``run_in_thread`` (copy_context).
2. TenantScope resolves through Tenancy (thread-local storage is gone).
3. The queue rail: a job carrying ``_tenant_id`` runs its middleware
   pipeline under that tenant; a job without the attr (inline dispatch)
   inherits the caller's live context.
"""

from __future__ import annotations

import asyncio

from cara.context import ExecutionContext, Tenancy
from cara.eloquent.scopes.TenantScope import TenantScope
from cara.queues.middleware import run_through_middleware_async


class TestTenancyContext:
    def test_default_is_none(self):
        assert Tenancy.id() is None

    def test_as_tenant_scopes_and_restores(self):
        with Tenancy.as_tenant(7):
            assert Tenancy.id() == 7
            with Tenancy.as_tenant(9):
                assert Tenancy.id() == 9
            assert Tenancy.id() == 7
        assert Tenancy.id() is None

    def test_central_clears_inside_tenant_scope(self):
        with Tenancy.as_tenant(7):
            with Tenancy.central():
                assert Tenancy.id() is None
            assert Tenancy.id() == 7

    def test_crosses_run_in_thread(self):
        async def main():
            with Tenancy.as_tenant(42):
                return await ExecutionContext.run_in_thread(Tenancy.id)

        assert asyncio.run(main()) == 42


class TestTenantScopeResolution:
    def test_scope_reads_tenancy(self):
        scope = TenantScope()
        with Tenancy.as_tenant(13):
            assert scope._get_current_tenant_id() == 13
        assert scope._get_current_tenant_id() is None


class _Job:
    """Minimal job — no middleware, records the tenant its body saw."""

    def __init__(self):
        self.seen = "unset"

    def middleware(self):
        return []


class TestQueueRail:
    def test_consumed_job_runs_under_dispatch_tenant(self):
        job = _Job()
        job._tenant_id = 21  # what the worker stamps from payload["_tenant"]

        async def handler(j):
            j.seen = Tenancy.id()

        asyncio.run(run_through_middleware_async(job, handler))
        assert job.seen == 21

    def test_consumed_job_with_none_tenant_clears_scope(self):
        job = _Job()
        job._tenant_id = None  # scheduler/CLI dispatch — no tenant scope

        async def handler(j):
            j.seen = Tenancy.id()

        async def main():
            with Tenancy.as_tenant(99):  # worker thread must not leak in
                await run_through_middleware_async(job, handler)

        asyncio.run(main())
        assert job.seen is None

    def test_inline_dispatch_inherits_caller_context(self):
        job = _Job()  # no _tenant_id attr — sync-mode inline dispatch

        async def handler(j):
            j.seen = Tenancy.id()

        async def main():
            with Tenancy.as_tenant(55):
                await run_through_middleware_async(job, handler)

        asyncio.run(main())
        assert job.seen == 55
