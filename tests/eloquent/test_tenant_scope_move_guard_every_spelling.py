"""A tenant-armed UPDATE may never move a row to another tenant, however the
tenant column is spelled.

The move guard compared the bare ``tenant_id`` key of a dict update only. A
table-qualified key (``update({"listing.tenant_id": 2})``), another letter case
or quoting, and the counter verbs (``increment("tenant_id")`` /
``decrement("tenant_id")``, whose update names its column as a string) all went
through and rewrote ``tenant_id`` on the caller's own rows — handing them to
another workspace. Central (un-tenanted) work may still move a row.
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
    __connection__ = "tenant_move"
    __fillable__ = ["*"]
    __timestamps__ = False


@pytest.fixture
def database(tmp_path):
    path = str(tmp_path / "tenant_move.db")
    con = sqlite3.connect(path)
    con.executescript(
        """
        create table listing (id integer primary key, tenant_id integer,
                              sku text, qty integer default 0);
        insert into listing (id, tenant_id, sku, qty) values (1, 1, 'a', 1), (2, 2, 'b', 1);
        """
    )
    con.commit()
    con.close()
    manager = DatabaseManager(
        "tenant_move", {"tenant_move": {"driver": "sqlite", "database": path}}
    )
    with swap("DB", manager):
        yield path


def _row(path, row_id=1):
    con = sqlite3.connect(path)
    try:
        return con.execute(
            "select tenant_id, sku, qty from listing where id = ?", (row_id,)
        ).fetchone()
    finally:
        con.close()


@pytest.mark.parametrize(
    "move",
    [
        lambda: _Listing.where("id", 1).update({"listing.tenant_id": 2}),
        lambda: _Listing.where("id", 1).update({"TENANT_ID": 2}),
        lambda: _Listing.where("id", 1).update({'"tenant_id"': 2}),
        lambda: _Listing.where("id", 1).increment("tenant_id", 1),
        lambda: _Listing.where("id", 1).decrement("tenant_id", -1),
        lambda: _Listing.where("id", 1).increment("listing.tenant_id", 1),
    ],
    ids=[
        "qualified",
        "upper-case",
        "quoted",
        "increment",
        "decrement",
        "qualified-increment",
    ],
)
def test_no_spelling_of_the_tenant_column_moves_a_row(database, move):
    with Tenancy.as_tenant(1), pytest.raises(RuntimeError, match="cross-tenant"):
        move()

    assert _row(database)[0] == 1


def test_ordinary_writes_and_naming_the_own_tenant_still_run(database):
    with Tenancy.as_tenant(1):
        assert _Listing.where("id", 1).update({"sku": "z"}) == 1
        _Listing.where("id", 1).increment("qty", 4)
        _Listing.where("id", 1).update({"listing.tenant_id": 1, "sku": "y"})

    assert _row(database) == (1, "y", 5)


def test_central_work_may_still_move_a_row(database):
    with Tenancy.central():
        _Listing.where("id", 1).update({"listing.tenant_id": 2})

    assert _row(database)[0] == 2
