from .BaseScope import BaseScope


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

        # Apply tenant_id injection to create/insert queries
        builder.set_global_scope(
            "_tenant_injector", self._inject_tenant_id, action="insert"
        )

    def on_remove(self, builder):
        """Remove tenant scoping."""
        builder.remove_global_scope("_tenant_filter", action="select")
        builder.remove_global_scope("_tenant_injector", action="insert")

    def _apply_tenant_filter(self, builder):
        """Apply tenant filtering based on current request context."""
        try:
            tenant_id = self._get_current_tenant_id()

            # Only apply filter if tenant_id is available
            if tenant_id is not None:
                table_name = builder.get_table_name()
                return builder.where(f"{table_name}.{self.tenant_column}", tenant_id)

            return builder

        except Exception:
            # If anything goes wrong, don't break the query - just skip filtering
            return builder

    def _inject_tenant_id(self, builder):
        """Automatically inject tenant_id into create/insert operations."""
        try:
            tenant_id = self._get_current_tenant_id()

            if tenant_id is not None and self.tenant_column not in builder._creates:
                # Inject tenant_id into the creates dictionary (like TimeStampsScope does)
                builder._creates.update({self.tenant_column: tenant_id})

            return builder

        except Exception:
            # If anything goes wrong, don't break the query - just skip injection
            return builder

    def _get_current_tenant_id(self):
        """Get current tenant_id from request context or thread-local storage."""
        try:
            # First try to get from thread-local storage (for CLI/job contexts)
            thread_tenant_id = TenantScope.get_thread_tenant_id()
            if thread_tenant_id is not None:
                return thread_tenant_id

            # Then try request context using Cara's context system
            from cara.http.request.context import current_request

            try:
                request = current_request.get()
                return getattr(request, "tenant_id", None)
            except:
                # If no request context (e.g., in CLI, jobs, etc.), return None
                return None

        except Exception:
            return None

    @classmethod
    def set_tenant_id(cls, tenant_id):
        """Set tenant_id for current thread (useful for CLI/job contexts)."""
        import threading

        if not hasattr(cls, "_tenant_storage"):
            cls._tenant_storage = threading.local()
        cls._tenant_storage.tenant_id = tenant_id

    @classmethod
    def get_thread_tenant_id(cls):
        """Get tenant_id from thread-local storage."""
        import threading

        if not hasattr(cls, "_tenant_storage"):
            cls._tenant_storage = threading.local()
        return getattr(cls._tenant_storage, "tenant_id", None)
