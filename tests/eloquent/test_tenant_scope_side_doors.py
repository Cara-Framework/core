"""Builder doors that went around ``TenantScope`` (framework seam sweep, 2026-11-19).

* ``upsert`` ran its own action with no tenant scope: a tenant-armed upsert
  could write another tenant's ``tenant_id``, and its ``ON CONFLICT DO
  UPDATE`` rewrote a conflicting row of ANY tenant (``unique_by`` on global
  ids such as ``listing_id``).
* ``update`` filtered rows by tenant but let the SET move a row to another
  tenant (``update({"tenant_id": 9})``) — the insert path refuses that write.
* ``without_global_scopes()`` / ``remove_global_scope("_tenant_filter")``
  dropped the scope outside ``Tenancy.central()``, which ``without_tenant()``
  already refuses.
* ``truncate()`` emptied every workspace's rows from a tenant scope.
"""

from __future__ import annotations

import pytest

from cara.context import Tenancy
from cara.eloquent import DatabaseManager
from cara.eloquent.query import QueryBuilder
from cara.eloquent.query.grammars import PostgresGrammar
from cara.eloquent.scopes import TenantScope
from cara.testing.FacadeSwap import swap


@pytest.fixture(autouse=True)
def _connection():
    manager = DatabaseManager(
        "test_side_doors",
        {"test_side_doors": {"driver": "sqlite", "database": ":memory:"}},
    )
    with swap("DB", manager):
        yield


def _attributes() -> QueryBuilder:
    builder = QueryBuilder(
        grammar=PostgresGrammar,
        connection="test_side_doors",
        table="listing_attribute",
        dry=True,
    )
    TenantScope().on_boot(builder)
    return builder


def _upsert_sql(rows: list[dict]) -> str:
    builder = _attributes()
    builder.upsert(rows, unique_by=["listing_id", "name"], update=["value"])
    return builder.to_sql()


def test_an_upsert_stamps_the_armed_tenant():
    with Tenancy.as_tenant(5):
        sql = _upsert_sql([{"listing_id": 1, "name": "Color", "value": "Red"}])

    assert '"tenant_id"' in sql.split("VALUES")[0]
    assert "'5'" in sql.split("VALUES")[1]


def test_an_upsert_never_rewrites_another_tenants_conflicting_row():
    with Tenancy.as_tenant(5):
        sql = _upsert_sql([{"listing_id": 1, "name": "Color", "value": "Red"}])

    assert sql.endswith(
        '"value" = EXCLUDED."value" WHERE "listing_attribute"."tenant_id" = '
        'EXCLUDED."tenant_id" RETURNING *'
    )


def test_an_upsert_for_another_tenant_is_refused():
    with Tenancy.as_tenant(5), pytest.raises(RuntimeError, match="cross-tenant"):
        _upsert_sql([{"tenant_id": 9, "listing_id": 1, "name": "C", "value": "R"}])


def test_an_unset_upsert_is_refused():
    with pytest.raises(RuntimeError, match="UNSET"):
        _upsert_sql([{"listing_id": 1, "name": "C", "value": "R"}])


def test_an_update_cannot_move_a_row_to_another_tenant():
    with Tenancy.as_tenant(5), pytest.raises(RuntimeError, match="cross-tenant"):
        _attributes().where("id", 1).update({"tenant_id": 9}, dry=True).to_sql()


def test_an_update_of_other_columns_still_compiles():
    with Tenancy.as_tenant(5):
        sql = _attributes().where("id", 1).update({"value": "x"}, dry=True).to_sql()

    assert sql.endswith("AND \"tenant_id\" = '5'")


def test_central_work_may_move_a_row():
    with Tenancy.central():
        _attributes().where("id", 1).update({"tenant_id": 9}, dry=True).to_sql()


@pytest.mark.parametrize(
    "drop",
    [
        lambda builder: builder.without_global_scopes(),
        lambda builder: builder.remove_global_scope("_tenant_filter", action="select"),
        lambda builder: builder.truncate(dry=True),
    ],
)
def test_the_scope_is_dropped_only_inside_central(drop):
    with Tenancy.as_tenant(5), pytest.raises(RuntimeError, match="central"):
        drop(_attributes())
    with pytest.raises(RuntimeError, match="central"):
        drop(_attributes())  # UNSET
    with Tenancy.central():
        drop(_attributes())
