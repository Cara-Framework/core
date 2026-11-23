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

        # bulk_create runs its own scope action; without it, bulk inserts
        # silently skip tenant injection and produce invisible rows.
        builder.set_global_scope(
            "_tenant_injector_bulk", self._inject_tenant_id_bulk, action="bulk_create"
        )

        # upsert runs its own action too: without it an upsert wrote another
        # tenant's tenant_id, and its ON CONFLICT DO UPDATE rewrote a
        # conflicting row of ANY tenant.
        builder.set_global_scope(
            "_tenant_injector_upsert", self._inject_tenant_id_upsert, action="upsert"
        )
        # An update may never move a row to another tenant.
        builder.set_global_scope(
            "_tenant_update_guard", self._refuse_tenant_move, action="update"
        )

    def on_remove(self, builder):
        """Remove tenant scoping."""
        builder.remove_global_scope("_tenant_filter", action="select")
        builder.remove_global_scope("_tenant_filter_update", action="update")
        builder.remove_global_scope("_tenant_filter_delete", action="delete")
        builder.remove_global_scope("_tenant_injector", action="insert")
        builder.remove_global_scope("_tenant_injector_bulk", action="bulk_create")
        builder.remove_global_scope("_tenant_injector_upsert", action="upsert")
        builder.remove_global_scope("_tenant_update_guard", action="update")

    def _inject_tenant_id_upsert(self, builder):
        """:meth:`_inject_tenant_id_bulk` for ``upsert`` rows, plus the
        conflict guard: unless the conflict key names the tenant column, the
        ``DO UPDATE`` only rewrites a conflicting row of the SAME tenant
        (``WHERE t.tenant_id = EXCLUDED.tenant_id``) — another tenant's row
        is left as it is."""
        from cara.context import Tenancy  # local: cycle with cara.context

        state = Tenancy.state()
        if state is Tenancy.UNSET:
            raise RuntimeError("Cannot upsert tenant-scoped models with UNSET tenancy.")
        rows = []
        for row in builder._upsert_values:
            self._refuse_aliased_tenant_key(row)
            if state is Tenancy.CENTRAL:
                if self.tenant_column not in row:
                    raise RuntimeError(
                        "Central tenant-scoped upserts must explicitly include "
                        f"{self.tenant_column} in every row."
                    )
            elif row.get(self.tenant_column) is None:
                row = {**row, self.tenant_column: state}
            elif row[self.tenant_column] != state:
                raise RuntimeError("Tenant-scoped upsert attempted a cross-tenant write.")
            rows.append(dict(sorted(row.items())))
        builder._upsert_values = rows
        if self.tenant_column not in (builder._upsert_unique_by or []):
            builder._upsert_conflict_guard = self.tenant_column
        return builder

    def _refuse_tenant_move(self, builder):
        """Refuse an UPDATE that sets the tenant column to another tenant
        (the insert path refuses the same write; central work may move)."""
        from cara.context import Tenancy  # local: cycle with cara.context

        # CENTRAL may move a row; UNSET already matches no row (``1 = 0``).
        if not Tenancy.is_tenant():
            return builder
        state = Tenancy.state()
        for update in getattr(builder, "_updates", ()) or ():
            target = getattr(update, "column", None)
            if isinstance(target, dict):
                moved = any(
                    self._names_tenant_column(column) and str(value) != str(state)
                    for column, value in target.items()
                )
            else:
                # ``increment`` / ``decrement`` name their column as a string:
                # arithmetic on the tenant column is a move whatever the amount.
                moved = self._names_tenant_column(target)
            if moved:
                raise RuntimeError("Tenant-scoped update attempted a cross-tenant move.")
        return builder

    def _names_tenant_column(self, column) -> bool:
        """Whether an update's column name is the tenant column, however it
        is spelled. The guard compared the bare name only, so a qualified key
        (``update({"listing.tenant_id": 9})``) and the counter verbs
        (``increment("tenant_id")``) moved the caller's rows to another
        tenant; the grammar writes every one of these to the same column."""
        if not isinstance(column, str):
            return False
        name = column.rsplit(".", 1)[-1].strip().strip('"`[]').strip()
        return name.casefold() == self.tenant_column.casefold()

    def _refuse_aliased_tenant_key(self, row) -> None:
        """Refuse an insert row that names the tenant column under any
        spelling but the bare one. The injectors read the bare key only: under
        ``{"listing.tenant_id": 9}`` the caller's value went unseen, the armed
        tenant was stamped beside it, and the statement carried the column
        twice — a database that takes the first value (SQLite) stored the row
        in the caller's chosen tenant."""
        for column in row:
            if column != self.tenant_column and self._names_tenant_column(column):
                raise RuntimeError(
                    "Tenant-scoped insert attempted a cross-tenant write: "
                    f"{column!r} is another spelling of {self.tenant_column}."
                )

    def _apply_tenant_filter(self, builder):
        """Apply tenant filtering based on current request context.

        Fails CLOSED: if tenant resolution raises, we apply an impossible
        filter (1=0) that returns zero rows. This prevents cross-tenant
        data exposure during transient failures. The previous behaviour
        of skipping the filter was a data-leak vector.
        """
        try:
            from cara.context import Tenancy  # local: cycle with cara.context

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
            from cara.context import Tenancy  # local: cycle with cara.context

            state = Tenancy.state()
            creates = builder._creates
            self._refuse_aliased_tenant_key(creates)
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
                raise RuntimeError("Tenant-scoped insert attempted a cross-tenant write.")

            return builder

        except RuntimeError:
            # The deliberate refusals above (UNSET tenancy, a missing central
            # tenant_id, a cross-tenant write) already say exactly what went
            # wrong. Swallowing them into the generic message below reported a
            # cross-tenant write attempt as a resolution failure.
            raise
        except Exception as exc:
            _logger.error(
                "Tenant ID injection failed — aborting insert to prevent orphan row",
                exc_info=True,
            )
            raise RuntimeError(
                "Cannot insert without tenant_id: tenant resolution failed"
            ) from exc

    def _inject_tenant_id_bulk(self, builder):
        """Bulk-insert variant of :meth:`_inject_tenant_id`.

        ``_creates`` is a list of canonicalized rows here. Same semantics:
        explicitly provided tenant_id values are respected, absent/None
        ones are stamped from the active tenant context; a tenant
        resolution failure fails CLOSED.
        """
        try:
            from cara.context import Tenancy  # local: cycle with cara.context

            state = Tenancy.state()
            if state is Tenancy.UNSET:
                raise RuntimeError(
                    "Cannot bulk-insert tenant-scoped models with UNSET tenancy."
                )
            for row in builder._creates:
                self._refuse_aliased_tenant_key(row)
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

        except RuntimeError:
            # Same as the single-row path: the deliberate refusals above carry
            # the precise reason and must reach the caller unchanged.
            raise
        except Exception as exc:
            _logger.error(
                "Tenant ID injection failed — aborting bulk insert to prevent orphan rows",
                exc_info=True,
            )
            raise RuntimeError(
                "Cannot bulk-insert without tenant_id: tenant resolution failed"
            ) from exc

    def _get_current_tenant_id(self):
        """The current tenant from :class:`cara.context.Tenancy` — the
        single source HTTP middleware, the queue rail and CLI code all
        write to. ContextVar-backed, so it survives the
        ``run_in_thread`` hop and stays isolated per request/job
        (thread-local storage leaked across requests on reused
        executor-pool threads and was removed)."""
        from cara.context import Tenancy  # local: cycle with cara.context

        return Tenancy.id()
