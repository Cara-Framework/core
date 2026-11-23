"""``__guarded__`` must refuse a column under every spelling the grammar writes.

The deny-list compared exact key names, while the grammar's
``_table_column_string`` drops a ``table.`` qualifier (and the quotes around
a name), so ``User.update({"users.is_admin": 1})`` wrote ``is_admin`` past
``__guarded__ = ["is_admin"]``.
"""

from __future__ import annotations

from cara.eloquent.models import Model


class _User(Model):
    __table__ = "users"
    __guarded__ = ["is_admin"]


def test_a_table_qualified_key_is_guarded():
    assert _User.filter_mass_assignment({"users.is_admin": 1, "name": "x"}) == {
        "name": "x"
    }


def test_quoted_and_differently_cased_keys_are_guarded():
    assert _User.filter_guarded({'"is_admin"': 1}) == {}
    assert _User.filter_guarded({'"users"."IS_ADMIN"': 1}) == {}
    assert _User.filter_guarded({"`Is_Admin`": 1}) == {}


def test_a_guarded_entry_spelled_qualified_still_matches_the_bare_key():
    class _Qualified(Model):
        __table__ = "users"
        __guarded__ = ["users.is_admin"]

    assert _Qualified.filter_guarded({"is_admin": 1, "name": "x"}) == {"name": "x"}


def test_unguarded_keys_pass_through_untouched():
    data = {"users.name": "x", "email": "a@b.c"}
    assert _User.filter_guarded(data) == data
