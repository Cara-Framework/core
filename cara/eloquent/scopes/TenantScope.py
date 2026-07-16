from __future__ import annotations

import logging

from .BaseScope import BaseScope

_logger = logging.getLogger("cara.tenant")


class TenantScope(BaseScope):
    """Global scope class to add automatic tenant filtering and tenant_id injection to models."""

    def __init__(self, tenant_column="tenant_id"):
        self.tenant_column = tenant_column

    def on_boot(self, builder):
        """Apply tenant scoping to all queries and auto-inject tenant_id on creates."""
        # Apply tenant filtering to select queries
        builder.set_global_scope(
            "_tenant_filter", self._apply_tenant_filter, action="select"
        )
        builder.set_global_scope(
            "_tenant_filter_update",
            self._apply_tenant_filter,
            action="update",
        )
        builder.set_global_scope(
            "_tenant_filter_delete",
            self._apply_tenant_filter,
            action="delete",
        )

        # Apply tenant_id injection to create/insert queries
        builder.set_global_scope(
            "_tenant_injector", self._inject_tenant_id, action="insert"
        )

        # bulk_create runs its own scope action (same registration split
        # UUIDPrimaryKeyScope / TimeStampsScope use) — without it, bulk
        # inserts silently skipped tenant injection and produced rows
        # invisible to every scoped query.
        builder.set_global_scope(
            "_tenant_injector_bulk", self._inject_tenant_id_bulk, action="bulk_create"
        )

    def on_remove(self, builder):
        """Remove tenant scoping."""
        builder.remove_global_scope("_tenant_filter", action="select")
        builder.remove_global_scope("_tenant_filter_update", action="update")
        builder.remove_global_scope("_tenant_filter_delete", action="delete")
        builder.remove_global_scope("_tenant_injector", action="insert")
        builder.remove_global_scope("_tenant_injector_bulk", action="bulk_create")

    def _apply_tenant_filter(self, builder):
        """Apply tenant filtering based on current request context.

        Fails CLOSED: if tenant resolution raises, we apply an impossible
        filter (1=0) that returns zero rows. This prevents cross-tenant
        data exposure during transient failures. The previous behaviour
        of skipping the filter was a data-leak vector.
        """
        try:
            from cara.context import Tenancy

            state = Tenancy.state()
            if state is Tenancy.UNSET:
                return builder.where_raw("1 = 0")
            if state is Tenancy.CENTRAL:
                return builder
            table_name = builder.get_table_name()
            return builder.where(f"{table_name}.{self.tenant_column}", state)

        except Exception:
            _logger.error(
                "Tenant filter failed — failing CLOSED (returning empty set)",
                exc_info=True,
            )
            return builder.where_raw("1 = 0")

    def _inject_tenant_id(self, builder):
        """Automatically inject tenant_id into create/insert operations.

        Fails CLOSED: if tenant resolution raises, we raise rather than
        inserting a row without a tenant_id (which would be invisible to
        subsequent scoped queries and constitute a data integrity failure).
        """
        try:
            from cara.context import Tenancy

            state = Tenancy.state()
            creates = builder._creates
            if state is Tenancy.UNSET:
                raise RuntimeError(
                    "Cannot insert a tenant-scoped model with UNSET tenancy."
                )
            if state is Tenancy.CENTRAL:
                if self.tenant_column not in creates:
                    raise RuntimeError(
                        "Central tenant-scoped inserts must explicitly include "
                        f"{self.tenant_column} (None is allowed)."
                    )
                return builder

            explicit = creates.get(self.tenant_column)
            if explicit is None:
                creates[self.tenant_column] = state
            elif explicit != state:
                raise RuntimeError(
                    "Tenant-scoped insert attempted a cross-tenant write."
                )

            return builder

        except Exception:
            _logger.error(
                "Tenant ID injection failed — aborting insert to prevent orphan row",
                exc_info=True,
            )
            raise RuntimeError(
                "Cannot insert without tenant_id: tenant resolution failed"
            )

    def _inject_tenant_id_bulk(self, builder):
        """Bulk-insert variant of :meth:`_inject_tenant_id`.

        ``_creates`` is a list of canonicalized rows here. Same semantics:
        explicitly provided tenant_id values are respected, absent/None
        ones are stamped from the active tenant context; a tenant
        resolution failure fails CLOSED.
        """
        try:
            from cara.context import Tenancy

            state = Tenancy.state()
            if state is Tenancy.UNSET:
                raise RuntimeError(
                    "Cannot bulk-insert tenant-scoped models with UNSET tenancy."
                )
            for row in builder._creates:
                if state is Tenancy.CENTRAL:
                    if self.tenant_column not in row:
                        raise RuntimeError(
                            "Central tenant-scoped bulk inserts must explicitly "
                            f"include {self.tenant_column} in every row."
                        )
                    continue
                explicit = row.get(self.tenant_column)
                if explicit is None:
                    row[self.tenant_column] = state
                elif explicit != state:
                    raise RuntimeError(
                        "Tenant-scoped bulk insert attempted a cross-tenant write."
                    )

            return builder

        except Exception:
            _logger.error(
                "Tenant ID injection failed — aborting bulk insert to prevent orphan rows",
                exc_info=True,
            )
            raise RuntimeError(
                "Cannot bulk-insert without tenant_id: tenant resolution failed"
            )

    def _get_current_tenant_id(self):
        """The current tenant from :class:`cara.context.Tenancy` — the
        single source HTTP middleware, the queue rail and CLI code all
        write to. ContextVar-backed, so it survives the
        ``run_in_thread`` hop and stays isolated per request/job
        (thread-local storage leaked across requests on reused
        executor-pool threads and was removed)."""
        from cara.context import Tenancy

        return Tenancy.id()
