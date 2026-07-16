from __future__ import annotations

from .TenantScope import TenantScope


class MakesTenantScope:
    """Mixin to add automatic tenant scoping to models."""

    def boot_MakesTenantScope(self, builder):
        """Boot the tenant scope mixin."""
        builder.set_global_scope(TenantScope())

    def scope_without_tenant(self, query):
        """Remove tenant filtering only inside an explicit central scope."""
        from cara.context import Tenancy

        if not Tenancy.is_central():
            raise RuntimeError(
                "without_tenant() requires an explicit Tenancy.central() scope."
            )
        return query.remove_global_scope("_tenant_filter", action="select")
