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

    def get_table_name(self):
        return "example"

    def where(self, column, value):
        self.where_call = (column, value)
        return self

    def where_raw(self, expression):
        self.where_raw_call = expression
        return self


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

    def test_explicit_cross_tenant_value_is_rejected(self, tenant_context):
        scope = TenantScope()
        builder = _FakeBuilder(
            [
                {"name": "a", "tenant_id": 7},
                {"name": "b", "tenant_id": None},
            ]
        )

        with pytest.raises(RuntimeError):
            scope._inject_tenant_id_bulk(builder)

    def test_unset_context_rejects_bulk_insert(self):
        scope = TenantScope()
        builder = _FakeBuilder([{"name": "a"}])

        with pytest.raises(RuntimeError):
            scope._inject_tenant_id_bulk(builder)

    def test_central_requires_explicit_column_but_allows_null(self):
        from cara.context import Tenancy

        scope = TenantScope()
        with Tenancy.central():
            allowed = _FakeBuilder([{"name": "platform", "tenant_id": None}])
            scope._inject_tenant_id_bulk(allowed)
            assert allowed._creates[0]["tenant_id"] is None

            with pytest.raises(RuntimeError):
                scope._inject_tenant_id_bulk(_FakeBuilder([{"name": "missing"}]))

    def test_on_boot_registers_bulk_action(self):
        scope = TenantScope()
        builder = _FakeBuilder([])

        scope.on_boot(builder)

        assert "_tenant_injector_bulk" in builder.scopes.get("bulk_create", {})
        assert "_tenant_injector" in builder.scopes.get("insert", {})
        assert "_tenant_filter_update" in builder.scopes.get("update", {})
        assert "_tenant_filter_delete" in builder.scopes.get("delete", {})

    @pytest.mark.parametrize("action", ["select", "update", "delete"])
    def test_filter_actions_are_empty_when_unset(self, action):
        scope = TenantScope()
        builder = _FakeBuilder([])
        scope.on_boot(builder)

        builder.scopes[action][
            "_tenant_filter" if action == "select" else f"_tenant_filter_{action}"
        ](builder)

        assert builder.where_raw_call == "1 = 0"

    @pytest.mark.parametrize("action", ["select", "update", "delete"])
    def test_filter_actions_pin_active_tenant(self, action, tenant_context):
        scope = TenantScope()
        builder = _FakeBuilder([])
        scope.on_boot(builder)

        builder.scopes[action][
            "_tenant_filter" if action == "select" else f"_tenant_filter_{action}"
        ](builder)

        assert builder.where_call == ("example.tenant_id", 42)
