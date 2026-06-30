from __future__ import annotations

from .BaseScope import BaseScope


class SoftDeleteScope(BaseScope):
    """
    Global scope class to add soft deleting to models.

    Soft delete scopes queries to exclude soft-deleted records by default,
    while providing methods to include or view only trashed records.
    Similar to Laravel's SoftDeletes trait.
    """

    def __init__(self, deleted_at_column: str = "deleted_at"):
        """
        Initialize the soft delete scope.

        Args:
            deleted_at_column: The column name for soft delete timestamp
        """
        self.deleted_at_column = deleted_at_column

    def on_boot(self, builder) -> None:
        """Register global scopes and macros when scope boots."""
        # Add default scope to exclude soft-deleted records from SELECT queries
        builder.set_global_scope("_soft_delete", self._where_not_deleted, action="select")

        # Override DELETE to soft delete (update deleted_at instead)
        builder.set_global_scope(
            "_soft_delete_delete",
            self._soft_delete_query,
            action="delete",
        )

        # Register convenience methods
        builder.macro("with_trashed", self._with_trashed)
        builder.macro("only_trashed", self._only_trashed)
        builder.macro("restore", self._restore)
        builder.macro("force_delete", self._force_delete)
        builder.macro("force_delete_query", self._force_delete_query)

    def on_remove(self, builder) -> None:
        """Remove soft delete scopes when disabled."""
        builder.remove_global_scope("_soft_delete", action="select")
        builder.remove_global_scope("_soft_delete_delete", action="delete")

    def _where_not_deleted(self, builder):
        """
        Apply the default scope to exclude soft-deleted records.
        Ensures only non-deleted records are returned by default.
        """
        table = builder.get_table_name()
        return builder.where_null(f"{table}.{self.deleted_at_column}")

    def _with_trashed(self, model, builder):
        """
        Include soft-deleted records in results.
        Removes the soft delete scope from SELECT.

        Macro signature: ``(model, builder)`` — the QueryBuilder macro
        dispatcher (`QueryBuilder.__getattr__`) calls registered macros
        with ``(self._model, self, *args)``, so every macro on this scope
        receives the model + builder pair even when it doesn't use the
        model. Pre-fix the signature was ``(self, builder)`` and every
        ``Model.with_trashed()`` call raised
        ``TypeError: takes 2 positional arguments but 3 were given``.
        """
        builder.remove_global_scope("_soft_delete", action="select")
        return builder

    def _only_trashed(self, model, builder):
        """
        Return only soft-deleted records.
        Removes the soft delete scope and adds a filter for non-null deleted_at.

        See ``_with_trashed`` for the ``(model, builder)`` signature reason.
        """
        builder.remove_global_scope("_soft_delete", action="select")
        table = builder.get_table_name()
        return builder.where_not_null(f"{table}.{self.deleted_at_column}")

    def _restore(self, model, builder):
        """
        Restore soft-deleted records by clearing the deleted_at timestamp.
        Must remove the soft delete scope to allow restoring deleted records.

        ``ignore_mass_assignment=True`` because ``deleted_at`` is framework-
        managed and intentionally absent from ``__fillable__``; without it the
        mass-assignment filter strips the column and the restore UPDATE
        collapses to an empty no-op (the same failure mode documented on
        ``_soft_delete_query``).

        See ``_with_trashed`` for the ``(model, builder)`` signature reason.
        """
        builder.remove_global_scope("_soft_delete", action="select")
        return builder.update({self.deleted_at_column: None}, ignore_mass_assignment=True)

    def _soft_delete_query(self, builder):
        """
        Convert a DELETE into a soft delete by mutating the builder in place
        so ``to_qmark()`` compiles ``UPDATE <table> SET deleted_at = <ts>``
        against the SAME ``WHERE`` clause instead of emitting a ``DELETE``.

        This scope runs INSIDE ``QueryBuilder.to_qmark()`` (via
        ``run_scopes()``) while the delete is being compiled, so it MUST be a
        pure builder mutation — it must not execute a query or reset state.

        ROOT CAUSE (2026-06-27): the previous implementation returned
        ``builder.update({deleted_at: ts})``, which silently degraded every
        ``.delete()`` to a hard ``DELETE``. Two independent reasons, each
        fatal on its own:

          1. ``update()`` runs its payload through the model's mass-assignment
             filter. ``deleted_at`` is framework-managed (like ``created_at`` /
             ``updated_at``) and deliberately absent from every model's
             ``__fillable__``, so the filter STRIPPED it. ``update()`` then saw
             an empty change-set and returned a no-op WITHOUT setting
             ``_action = "update"`` — leaving the builder on ``"delete"`` so it
             fell straight through to a hard ``DELETE``. With ``ON DELETE
             CASCADE`` children (``listing.product_id`` → product) a single
             ``Product.delete()`` obliterated the product AND its listings.
          2. ``update()`` SELF-EXECUTES and ``reset()``s the builder, both of
             which corrupt the surrounding compile when run mid-``to_qmark()``.

        Assigning ``_updates`` + ``set_action("update")`` directly is the
        correct primitive: it bypasses mass-assignment (this is a framework
        write, not user input) and leaves the builder in a compile-ready
        UPDATE state for the outer ``to_qmark()`` to render and execute.
        Pinned by ``tests/integration/test_soft_delete_contract.py``.
        """
        if hasattr(builder, "_model") and builder._model:
            timestamp = builder._model.get_new_datetime_string()
        else:
            import pendulum

            timestamp = pendulum.now("UTC").to_datetime_string()

        builder._updates = ()
        builder.set_updates({self.deleted_at_column: timestamp})
        builder.set_action("update")
        return builder

    def _force_delete(self, model, builder):
        """
        Permanently delete a record, bypassing soft delete.

        See ``_with_trashed`` for the ``(model, builder)`` signature reason.
        """
        self._strip_soft_delete_scopes(builder)
        return builder.delete()

    def _force_delete_query(self, model, builder):
        """
        Get a query builder for force delete without executing.
        Useful for batch delete operations.

        See ``_with_trashed`` for the ``(model, builder)`` signature reason.
        """
        self._strip_soft_delete_scopes(builder)
        return builder

    @staticmethod
    def _strip_soft_delete_scopes(builder) -> None:
        """Drop the soft-delete select + delete scopes from ``builder`` so a
        force-delete compiles to a real ``DELETE`` — WITHOUT mutating the
        existing scope dicts in place.

        ``remove_global_scope`` does ``del scopes[name]`` on the live
        action-dict. We instead rebuild ``_global_scopes`` from fresh dicts:
        that guarantees we never disturb a dict that could be aliased
        elsewhere, and it keeps a force-delete from leaving the builder
        unable to soft-delete on any later reuse. (Each top-level query
        already gets its own builder, so this is defence in depth — but it
        permanently retires the shared-dict mutation hazard rather than
        relying on the per-builder copy in ``QueryBuilder.__init__``.)
        """
        drop = {"_soft_delete", "_soft_delete_delete"}
        builder._global_scopes = {
            action: {name: fn for name, fn in scopes.items() if name not in drop}
            for action, scopes in builder._global_scopes.items()
        }
