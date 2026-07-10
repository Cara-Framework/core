"""Current-tenant execution context.

The single source of truth for "which tenant is this unit of work
running for" — the tenancy analogue of the trace ids on
:class:`ExecutionContext`. HTTP middleware sets it once per request
after resolving the caller's workspace; the queue rail restores it
around a job dispatched from tenant context (``AMQPDriver`` stamps
``_tenant`` into the payload on the same rail as ``_otel``); and
``TenantScope`` reads it to arm fail-closed per-tenant query filtering.

ContextVar-backed on purpose: it propagates through
``ExecutionContext.run_in_thread`` (which ``copy_context()``s into the
worker thread) and stays isolated per request/task — unlike
thread-local storage, which leaks across requests on reused
executor-pool threads.
"""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any

_tenant_id: ContextVar[Any | None] = ContextVar("cara_tenant_id", default=None)


class Tenancy:
    """Request/job-scoped current tenant."""

    @staticmethod
    def set(tenant_id: Any | None):
        """Set the current tenant id. Returns the reset token."""
        return _tenant_id.set(tenant_id)

    @staticmethod
    def reset(token) -> None:
        """Restore the value active before the matching :meth:`set`."""
        _tenant_id.reset(token)

    @staticmethod
    def id() -> Any | None:
        """The current tenant id, or ``None`` outside tenant scope."""
        return _tenant_id.get()

    @staticmethod
    def clear() -> None:
        """Drop tenant scope for the current context."""
        _tenant_id.set(None)

    @staticmethod
    @contextmanager
    def as_tenant(tenant_id: Any | None):
        """Run a block as ``tenant_id`` (``None`` = no tenant), restoring
        the previous scope on exit."""
        token = _tenant_id.set(tenant_id)
        try:
            yield
        finally:
            _tenant_id.reset(token)

    @staticmethod
    @contextmanager
    def central():
        """Run a block with NO tenant — the escape for deliberate
        multi-tenant sweeps inside otherwise tenant-scoped work."""
        token = _tenant_id.set(None)
        try:
            yield
        finally:
            _tenant_id.reset(token)
