"""A tenant-armed INSERT may never carry the tenant key under another spelling.

The injectors read the bare ``tenant_id`` key only. Under ``{"listing.tenant_id": 2}``
or ``{"TENANT_ID": 2}`` the caller's value went unseen, the armed tenant was
stamped beside it, and the statement carried the column twice — SQLite took the
caller's value and the row was planted in another workspace. ``create``,
``bulk_create`` and ``upsert`` all went through.
"""

from __future__ import annotations

import sqlite3

import pytest

from cara.context import Tenancy
from cara.eloquent import DatabaseManager
from cara.eloquent.models import Model
from cara.eloquent.scopes import MakesTenantScope
from cara.testing.FacadeSwap import swap


class _Listing(Model, MakesTenantScope):
    __table__ = "listing"
    __connection__ = "tenant_insert"
    __fillable__ = ["*"]
    __timestamps__ = False


@pytest.fixture
def database(tmp_path):
    path = str(tmp_path / "tenant_insert.db")
    con = sqlite3.connect(path)
    con.execute(
        "create table listing (id integer primary key, tenant_id integer, sku text unique)"
    )
    con.commit()
    con.close()
    manager = DatabaseManager(
        "tenant_insert", {"tenant_insert": {"driver": "sqlite", "database": path}}
    )
    with swap("DB", manager):
        yield path


def _tenants(path):
    con = sqlite3.connect(path)
    try:
        return [row[0] for row in con.execute("select tenant_id from listing")]
    finally:
        con.close()


@pytest.mark.parametrize(
    "plant",
    [
        lambda: _Listing.create({"listing.tenant_id": 2, "sku": "q"}),
        lambda: _Listing.create({"TENANT_ID": 2, "sku": "q"}),
        lambda: _Listing.bulk_create([{"listing.tenant_id": 2, "sku": "q"}]),
        lambda: _Listing.upsert(
            [{"listing.tenant_id": 2, "sku": "q"}], unique_by=["sku"], update=["sku"]
        ),
    ],
    ids=["create-qualified", "create-upper-case", "bulk-create", "upsert"],
)
def test_an_aliased_tenant_key_is_refused(database, plant):
    with Tenancy.as_tenant(1), pytest.raises(RuntimeError, match="cross-tenant"):
        plant()

    assert _tenants(database) == []


def test_an_aliased_key_naming_the_own_tenant_is_refused_too(database):
    # The refusal is about the duplicate column, not the value.
    with Tenancy.as_tenant(1), pytest.raises(RuntimeError, match="another spelling"):
        _Listing.create({"listing.tenant_id": 1, "sku": "q"})


@pytest.mark.parametrize(
    "row",
    [{"sku": "q"}, {"tenant_id": 1, "sku": "q"}],
    ids=["stamped", "own-tenant-named"],
)
def test_a_plain_insert_is_stamped_with_the_armed_tenant(database, row):
    with Tenancy.as_tenant(1):
        _Listing.create(row)

    assert _tenants(database) == [1]
