"""Cycle-2 Cara hunt: ``QueryBuilder.bulk_update`` against real PostgreSQL.

``bulk_update`` builds its own ``UPDATE … FROM (VALUES …)`` statement. It
must keep the two promises every other write in the builder keeps:

* A row that does not name a column leaves that column alone. Today the
  missing key is bound as NULL, so a partial record ERASES the stored value
  (``upsert`` already refuses mixed row shapes for this reason).
* The builder's own constraints bound the write. Today ``where(...)`` (and
  so any scope a caller narrowed with) is dropped: a ``tenant_id = 1``
  builder rewrites tenant 2's row with the same id.

Needs a scratch database::

    DB_DATABASE=synk_scratch_<label> CARA_RUN_INTEGRATION=1 pytest <this file>
"""

from __future__ import annotations

import os

import pytest

from cara.eloquent.DatabaseManager import DatabaseManager
from cara.eloquent.query import QueryBuilder
from cara.eloquent.query.grammars import PostgresGrammar
from cara.exceptions import QueryException
from cara.testing.FacadeSwap import swap

pytestmark = pytest.mark.skipif(
    os.getenv("CARA_RUN_INTEGRATION") != "1"
    or not os.getenv("DB_DATABASE", "").startswith("synk_scratch_"),
    reason="set CARA_RUN_INTEGRATION=1 and DB_DATABASE named synk_scratch_*",
)

TABLE = "c2_bulk_update_listing"


@pytest.fixture
def db():
    manager = DatabaseManager(
        "default",
        {
            "default": {
                "driver": "postgres",
                "host": os.getenv("DB_HOST", "localhost"),
                "database": os.environ["DB_DATABASE"],
                "user": os.getenv("DB_USERNAME") or os.getenv("USER"),
                "password": os.getenv("DB_PASSWORD", ""),
                "port": int(os.getenv("DB_PORT", "5432")),
                "prefix": "",
            }
        },
    )
    with swap("DB", manager):
        manager.statement(f"DROP TABLE IF EXISTS {TABLE}")
        manager.statement(
            f"CREATE TABLE {TABLE} (id int, tenant_id int, price numeric(12,2),"
            " quantity int, synced_at timestamptz, PRIMARY KEY (tenant_id, id))"
        )
        manager.statement(
            f"INSERT INTO {TABLE} VALUES (1, 1, 10.00, 4), (2, 1, 20.00, 7),"
            " (1, 2, 99.00, 3)"
        )
        try:
            yield manager
        finally:
            manager.statement(f"DROP TABLE IF EXISTS {TABLE}")


def _listings() -> QueryBuilder:
    return QueryBuilder(grammar=PostgresGrammar, connection="default", table=TABLE)


def _rows(manager) -> dict[tuple[int, int], dict]:
    return {
        (int(r["tenant_id"]), int(r["id"])): r
        for r in manager.select(f"SELECT * FROM {TABLE}")
    }


def test_a_record_without_a_column_keeps_its_stored_value(db):
    _listings().where("tenant_id", 1).bulk_update(
        [{"id": 1, "price": 11.00, "quantity": 5}, {"id": 2, "price": 21.00}],
        key="id",
    )

    rows = _rows(db)
    assert rows[(1, 2)]["quantity"] == 7, "quantity was erased by a partial record"


def test_the_builders_where_bounds_the_write(db):
    _listings().where("tenant_id", 1).bulk_update(
        [{"id": 1, "price": 12.00}], key="id", update_columns=["price"]
    )

    rows = _rows(db)
    assert str(rows[(1, 1)]["price"]) == "12.00"
    assert str(rows[(2, 1)]["price"]) == "99.00", "another tenant's row was rewritten"


def test_an_update_global_scope_bounds_the_write(db):
    # The tenant scope reaches every model write as an ``update`` global
    # scope, not as a caller ``where``; bulk_update must run it too.
    builder = _listings().set_global_scope(
        "_tenant", lambda b: b.where("tenant_id", 1), action="update"
    )
    builder.bulk_update([{"id": 1, "price": 13.00}], key="id")

    rows = _rows(db)
    assert str(rows[(1, 1)]["price"]) == "13.00"
    assert str(rows[(2, 1)]["price"]) == "99.00", "the update scope was skipped"


def test_two_records_for_one_row_are_refused(db):
    # PostgreSQL would apply an arbitrary one of the two prices.
    with pytest.raises(QueryException):
        _listings().where("tenant_id", 1).bulk_update(
            [{"id": 1, "price": 1.00}, {"id": 1, "price": 50.00}], key="id"
        )

    assert str(_rows(db)[(1, 1)]["price"]) == "10.00"


def test_an_explicit_none_clears_a_price_another_record_leaves_alone(db):
    # Clearing a sale price is a ``None``; the VALUES column it travels in
    # must carry the stored column's type, not ``text``.
    _listings().where("tenant_id", 1).bulk_update(
        [{"id": 1, "price": None}, {"id": 2, "quantity": 8}], key="id"
    )

    rows = _rows(db)
    assert rows[(1, 1)]["price"] is None
    assert str(rows[(1, 2)]["price"]) == "20.00"
    assert rows[(1, 2)]["quantity"] == 8


def test_a_timestamp_string_writes_to_a_timestamptz_column(db):
    # ``DateTimeCast`` renders an aware instant as a string; a VALUES list
    # typed it ``text`` and the write failed.
    _listings().where("tenant_id", 1).bulk_update(
        [{"id": 1, "synced_at": "2026-11-14T08:00:00+00:00"}, {"id": 2, "quantity": 9}],
        key="id",
    )

    synced = _rows(db)[(1, 1)]["synced_at"]
    assert synced.utcoffset() is not None
    assert int(synced.timestamp()) == 1794643200
