"""A counter update on a column that merely CONTAINS the updated-at name is stamped.

``set_timestamp_update`` skips the stamp when the write already sets the
updated-at column. A dict update names its columns as keys, but a counter
update (``increment`` / ``decrement``) names one column as a string, and
``in`` on a string is a substring test: incrementing ``updated_count`` on a
model whose updated-at column is ``updated`` read as "already stamped", so the
row kept its old timestamp.
"""

from __future__ import annotations

from types import SimpleNamespace

import pendulum

from cara.eloquent.scopes.TimeStampsScope import TimeStampsScope


class _Model:
    __timestamps__ = True
    __casts__: dict = {}
    date_created_at = "created"
    date_updated_at = "updated"

    @staticmethod
    def get_new_date():
        return pendulum.datetime(2026, 11, 23, 12, 0, tz="UTC")

    @staticmethod
    def get_casts():
        return {}


def _builder(*columns):
    return SimpleNamespace(
        _model=_Model(),
        _updates=tuple(SimpleNamespace(column=column) for column in columns),
    )


def _stamped(builder) -> bool:
    before = len(builder._updates)
    TimeStampsScope().set_timestamp_update(builder)
    return len(builder._updates) == before + 1


def test_a_counter_on_a_lookalike_column_is_still_stamped():
    assert _stamped(_builder("updated_count")) is True


def test_a_counter_on_the_updated_column_itself_is_left_alone():
    assert _stamped(_builder("updated")) is False


def test_a_dict_update_that_sets_the_column_is_left_alone():
    assert _stamped(_builder({"name": "x", "updated": "2026-01-01"})) is False


def test_a_dict_update_of_other_columns_is_stamped():
    assert _stamped(_builder({"updated_count": 3})) is True
