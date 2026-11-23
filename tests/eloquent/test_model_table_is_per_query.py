"""``Model.table("x")`` must point ONE query at ``x``, never the class.

It assigned ``cls.__table__`` and returned the class, so after a single
``Invoice.table("invoice_archive")`` every later ``Invoice`` query in the
process — other requests and other tenants included — read and wrote the
archive table.
"""

from __future__ import annotations

import importlib

import pytest

from cara.eloquent.models import Model

qb_module = importlib.import_module("cara.eloquent.query.QueryBuilder")
persistence = importlib.import_module("cara.eloquent.models._ModelPersistence")


class _Invoice(Model):
    __table__ = "invoice"
    __fillable__ = ["number"]


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


def test_the_returned_builder_reads_the_named_table():
    builder = _Invoice.table("invoice_archive")

    assert isinstance(builder, qb_module.QueryBuilder)
    assert builder.get_table_name() == "invoice_archive"


def test_the_model_class_keeps_its_own_table():
    _Invoice.table("invoice_archive")

    assert _Invoice.__table__ == "invoice"
    assert _Invoice.get_table_name() == "invoice"
    assert _Invoice().get_builder().get_table_name() == "invoice"


def test_two_calls_do_not_share_a_builder():
    first = _Invoice.table("invoice_archive")
    second = _Invoice.table("invoice_2025")

    assert first is not second
    assert first.get_table_name() == "invoice_archive"
    assert second.get_table_name() == "invoice_2025"
