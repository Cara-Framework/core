"""A query soft delete stamps live rows only (Laravel's scoped builder).

``Model.where(...).delete()`` on a soft-deleting model compiles to an UPDATE
of ``deleted_at``. It used to carry no ``deleted_at IS NULL``: every row
already in the trash matching the WHERE was stamped again, losing when it was
really deleted and inflating the returned count (found 2026-11-13 batching a
channel removal's listing retire). ``with_trashed()`` opts back into every
row; a loaded instance's own delete stays keyed to its primary key.
"""

from __future__ import annotations

import pytest

from cara.eloquent import DatabaseManager
from cara.eloquent.query import QueryBuilder
from cara.eloquent.query.grammars import PostgresGrammar
from cara.eloquent.scopes import SoftDeleteScope
from cara.testing.FacadeSwap import swap


@pytest.fixture(autouse=True)
def _connection():
    manager = DatabaseManager(
        "test_soft", {"test_soft": {"driver": "sqlite", "database": ":memory:"}}
    )
    with swap("DB", manager):
        yield


def _listing() -> QueryBuilder:
    builder = QueryBuilder(
        grammar=PostgresGrammar, connection="test_soft", table="listing"
    )
    SoftDeleteScope().on_boot(builder)
    return builder


def test_a_query_soft_delete_leaves_the_trash_alone():
    sql = _listing().where("channel_id", 7).delete(query=True).to_sql()

    assert sql.startswith('UPDATE "listing" SET "deleted_at" = ')
    assert sql.endswith('WHERE "channel_id" = \'7\' AND "deleted_at" IS NULL')


def test_with_trashed_restamps_every_matching_row():
    sql = _listing().with_trashed().where("channel_id", 7).delete(query=True).to_sql()

    assert sql.startswith('UPDATE "listing" SET "deleted_at" = ')
    assert sql.endswith("WHERE \"channel_id\" = '7'")


def test_a_loaded_instance_delete_stays_keyed_to_its_primary_key():
    """Laravel's ``runSoftDelete``: the instance's own delete is keyed by
    its id alone, so it gains no ``deleted_at IS NULL``."""

    class _Loaded:
        def is_loaded(self):
            return True

        def get_new_datetime_string(self):
            return "2026-11-13T00:00:00+00:00"

        def get_selects(self):
            return []

    builder = _listing()
    builder._model = _Loaded()
    sql = builder.where("id", 9).delete(query=True).to_sql()

    assert sql.startswith('UPDATE "listing" SET "deleted_at" = ')
    assert sql.endswith("WHERE \"id\" = '9'")


def test_a_force_delete_is_still_a_real_delete():
    sql = (
        _listing().where("channel_id", 7).force_delete_query().delete(query=True).to_sql()
    )

    assert sql == 'DELETE FROM "listing" WHERE "listing"."channel_id" = \'7\''
