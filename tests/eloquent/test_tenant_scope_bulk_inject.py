"""Regression pins for TenantScope bulk-insert tenant injection.

``bulk_create`` runs its own scope action; TenantScope only registered
an ``insert`` injector, so bulk-inserted rows silently skipped tenant
stamping and became invisible to every tenant-scoped query. The bulk
injector stamps absent/None tenant columns per row and respects
explicitly provided values.
"""

from __future__ import annotations

import pytest

from cara.eloquent.scopes.TenantScope import TenantScope


class _FakeBuilder:
    def __init__(self, creates):
        self._creates = creates
        self.scopes: dict[str, dict] = {}

    def set_global_scope(self, name, callback, action="select"):
        self.scopes.setdefault(action, {})[name] = callback


@pytest.fixture
def tenant_context():
    from cara.context import Tenancy

    token = Tenancy.set(42)
    yield 42
    Tenancy.reset(token)


class TestBulkInjection:
    def test_bulk_rows_get_tenant_stamped(self, tenant_context):
        scope = TenantScope()
        builder = _FakeBuilder(
            [
                {"name": "a", "tenant_id": None},
                {"name": "b", "tenant_id": None},
            ]
        )

        scope._inject_tenant_id_bulk(builder)

        assert [row["tenant_id"] for row in builder._creates] == [42, 42]

    def test_explicit_tenant_values_are_respected(self, tenant_context):
        scope = TenantScope()
        builder = _FakeBuilder(
            [
                {"name": "a", "tenant_id": 7},
                {"name": "b", "tenant_id": None},
            ]
        )

        scope._inject_tenant_id_bulk(builder)

        assert [row["tenant_id"] for row in builder._creates] == [7, 42]

    def test_no_tenant_context_leaves_rows_untouched(self):
        scope = TenantScope()
        builder = _FakeBuilder([{"name": "a"}])

        scope._inject_tenant_id_bulk(builder)

        assert "tenant_id" not in builder._creates[0]

    def test_on_boot_registers_bulk_action(self):
        scope = TenantScope()
        builder = _FakeBuilder([])

        scope.on_boot(builder)

        assert "_tenant_injector_bulk" in builder.scopes.get("bulk_create", {})
        assert "_tenant_injector" in builder.scopes.get("insert", {})
