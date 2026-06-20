from __future__ import annotations

from .TenantScope import TenantScope


class MakesTenantScope:
    """Mixin to add automatic tenant scoping to models."""

    def boot_MakesTenantScope(self, builder):
        """Boot the tenant scope mixin."""
        builder.set_global_scope(TenantScope())

    def scope_without_tenant(self, query):
        """Query scope to remove tenant filtering."""
        return query.remove_global_scope("_tenant_filter", action="select")
