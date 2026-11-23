"""Building a model must not push its own stores through the column write door.

``Model.__setattr__`` is the write door for COLUMN values: it looks up a
mutator, consults the cast registry and applies the date rule. The
constructor used to assign its eight internal stores (``__attributes__``,
``_relations``, ...) and ``boot()`` three more through that same door, so
every instance paid eleven mutator/cast/date passes that could only ever end
in a plain ``__dict__`` write. A model is built for every hydrated row and
for every ``Model.query()``; measured on this machine an instance went from
10.2us to 0.9us and hydrating 2000 rows from 23.9ms to 5.0ms.

Pins the call count, not the wall clock: constructing or hydrating a model
calls ``__setattr__`` only for the caller's own keyword attributes.
"""

from __future__ import annotations

import importlib

import pytest

from cara.eloquent.models import Model
from cara.eloquent.scopes import MakesSoftDeletes

qb_module = importlib.import_module("cara.eloquent.query.QueryBuilder")
persistence = importlib.import_module("cara.eloquent.models._ModelPersistence")


class Gadget(Model, MakesSoftDeletes):
    __table__ = "gadgets"
    __fillable__ = ["name"]


class _Manager:
    """The DB facade a model-created builder resolves (no app booted here)."""

    def get_connection_details(self):
        return {"default": "stub"}

    def get_default_connection(self):
        return "stub"

    def validate_connection(self, _name):
        return None

    def get_connection_class(self, _name):
        return object

    def get_grammar(self, _name):
        return object


@pytest.fixture(autouse=True)
def _db_manager(monkeypatch):
    manager = _Manager()
    monkeypatch.setattr(qb_module, "DB", manager)
    monkeypatch.setattr(persistence, "DB", manager)


@pytest.fixture
def writes(monkeypatch):
    Gadget.hydrate({"id": 0, "name": "warm"})  # first boot of the class
    seen: list[str] = []
    original = Model.__setattr__

    def counting(self, attribute, value):
        seen.append(attribute)
        original(self, attribute, value)

    monkeypatch.setattr(Model, "__setattr__", counting)
    return seen


def test_constructing_a_model_writes_no_internal_store_through_setattr(writes):
    gadget = Gadget()

    assert writes == []
    assert gadget.__attributes__ == {}
    assert gadget.__original_attributes__ == {}
    assert gadget.__dirty_attributes__ == {}
    assert gadget._relations == {}
    assert gadget._relationships == {}
    assert gadget._model_events is None
    assert gadget._booted is True
    # The boot snapshot still reaches the instance: scopes and macros.
    assert "with_trashed" in gadget._macros
    assert any(gadget._global_scopes.values())


def test_hydrating_rows_calls_setattr_for_no_row(writes):
    rows = Gadget.hydrate([{"id": i, "name": f"g{i}"} for i in range(50)])

    assert len(rows) == 50
    assert writes == []
    assert rows[7].name == "g7"
    assert rows[7].is_dirty() is False


def test_keyword_attributes_still_go_through_the_write_door(writes):
    gadget = Gadget(name="widget")

    assert writes == ["name"]
    assert gadget.get_dirty_attributes() == {"name": "widget"}
    assert gadget.name == "widget"


def test_instances_do_not_share_their_stores():
    first, second = Gadget(), Gadget()

    first.__attributes__["id"] = 1
    first._relations["owner"] = object()
    first._global_scopes.clear()

    assert second.__attributes__ == {}
    assert second._relations == {}
    assert any(second._global_scopes.values())
