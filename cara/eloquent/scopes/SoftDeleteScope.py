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

    def _with_trashed(self, builder):
        """
        Include soft-deleted records in results.
        Removes the soft delete scope from SELECT.
        """
        builder.remove_global_scope("_soft_delete", action="select")
        return builder

    def _only_trashed(self, builder):
        """
        Return only soft-deleted records.
        Removes the soft delete scope and adds a filter for non-null deleted_at.
        """
        builder.remove_global_scope("_soft_delete", action="select")
        table = builder.get_table_name()
        return builder.where_not_null(f"{table}.{self.deleted_at_column}")

    def _restore(self, builder):
        """
        Restore soft-deleted records by clearing the deleted_at timestamp.
        Must remove the soft delete scope to allow restoring deleted records.
        """
        builder.remove_global_scope("_soft_delete", action="select")
        return builder.update({self.deleted_at_column: None})

    def _soft_delete_query(self, builder):
        """
        Convert a DELETE query to a soft delete (UPDATE).
        Sets deleted_at timestamp instead of deleting the record.
        """
        if hasattr(builder, "_model") and builder._model:
            timestamp = builder._model.get_new_datetime_string()
        else:
            import pendulum
            timestamp = pendulum.now("UTC").to_datetime_string()

        # Must remove the delete scope to avoid infinite recursion
        builder.remove_global_scope("_soft_delete_delete", action="delete")
        return builder.update({self.deleted_at_column: timestamp})

    def _force_delete(self, builder):
        """
        Permanently delete a record, bypassing soft delete.
        """
        # Remove soft delete scope to allow actual deletion
        builder.remove_global_scope("_soft_delete", action="select")
        builder.remove_global_scope("_soft_delete_delete", action="delete")
        return builder.delete()

    def _force_delete_query(self, builder):
        """
        Get a query builder for force delete without executing.
        Useful for batch delete operations.
        """
        builder.remove_global_scope("_soft_delete", action="select")
        builder.remove_global_scope("_soft_delete_delete", action="delete")
        return builder
