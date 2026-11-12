"""Hydrating N rows must not build N query builders.

Stress run 2026-11-12 (synk api, ``stress/api``): a 10k-product bulk-edit
preview hydrated ~55k models and spent most of its 5 s (43 s under a
profiler) in ``Model.__init__`` -> ``boot()``: every instance walked its MRO,
re-ran the ``boot_Makes*`` hooks and constructed a ``QueryBuilder`` (with a
container lookup for its connection details) just to copy the same global
scopes and macros back onto itself. The boot outcome is per CLASS, so it is
computed once per class and an instance gets a copy; its builder is built
lazily, seeded from that copy, the first time it is asked for.
"""

from __future__ import annotations

import importlib

import pytest

from cara.eloquent.models import Model
from cara.eloquent.scopes import MakesSoftDeletes

qb_module = importlib.import_module("cara.eloquent.query.QueryBuilder")
persistence = importlib.import_module("cara.eloquent.models._ModelPersistence")


class Widget(Model, MakesSoftDeletes):
    __table__ = "widgets"
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


def _shape(scopes: dict) -> dict:
    return {action: sorted(names) for action, names in scopes.items() if names}


def _count_builders(monkeypatch) -> list[int]:
    built = [0]
    original = qb_module.QueryBuilder.__init__

    def counting(self, *args, **kwargs):
        built[0] += 1
        original(self, *args, **kwargs)

    monkeypatch.setattr(qb_module.QueryBuilder, "__init__", counting)
    return built


def test_hydrating_many_rows_builds_no_builder_per_row(monkeypatch):
    Widget.hydrate({"id": 0, "name": "warm"})  # first boot of the class
    built = _count_builders(monkeypatch)
    rows = Widget.hydrate([{"id": i, "name": f"w{i}"} for i in range(200)])
    assert len(rows) == 200
    assert built[0] < 10, built[0]


def test_a_lazily_built_builder_still_carries_the_boot_scopes():
    Widget.hydrate({"id": 0, "name": "warm"})
    widget = Widget.hydrate({"id": 7, "name": "x"})
    assert widget._booted
    scopes = widget.get_builder()._global_scopes
    assert any(scopes.get(action) for action in scopes), scopes
    fresh = Widget()
    assert _shape(fresh.get_builder()._global_scopes) == _shape(scopes)
    assert "_soft_delete" in str(_shape(scopes)) or len(_shape(scopes)) >= 2


def test_instances_do_not_share_scope_dicts():
    a = Widget.hydrate({"id": 1, "name": "a"})
    b = Widget.hydrate({"id": 2, "name": "b"})
    assert _shape(a._global_scopes) == _shape(b._global_scopes)
    assert a._global_scopes is not b._global_scopes
    for action in a._global_scopes:
        assert a._global_scopes[action] is not b._global_scopes[action]
