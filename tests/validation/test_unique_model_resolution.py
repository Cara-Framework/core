"""The ``unique`` rule queries through the table's model, scopes included.

It used to run ``SELECT 1 FROM "<table>" WHERE "<col>" = %s`` through
``DB.select``: neither the fail-closed tenant scope nor the soft-delete scope
applied, so tenant B was refused a value only tenant A held (and learned that
it exists), a value held only by a trashed row read as taken, and a database
outage answered every request "has already been taken" — a 422 instead of a
5xx. ``exists`` had already moved to model resolution for the same reason.
"""

from __future__ import annotations

import sys
from types import ModuleType

import pytest

from cara.context import Tenancy
from cara.eloquent import DatabaseManager
from cara.eloquent.query import QueryBuilder
from cara.eloquent.query.grammars import PostgresGrammar
from cara.eloquent.scopes import SoftDeleteScope, TenantScope
from cara.exceptions import ConfigurationException
from cara.support import ModuleManager
from cara.testing.FacadeSwap import swap
from cara.validation.rules.UniqueRule import UniqueRule


@pytest.fixture(autouse=True)
def _tenant_five():
    """A throwaway connection (the builder validates one even when it only
    compiles) and tenant 5 in context."""
    manager = DatabaseManager(
        "test_unique", {"test_unique": {"driver": "sqlite", "database": ":memory:"}}
    )
    token = Tenancy.set(5)
    with swap("DB", manager):
        yield
    Tenancy.reset(token)


def _install_product(monkeypatch, *, first) -> list[str]:
    """A ``product`` model whose builder carries the real tenant and
    soft-delete scopes; ``first`` answers for the compiled query."""
    compiled: list[str] = []

    class Product:
        __table__ = "product"

        @classmethod
        def where(cls, column, value):
            builder = QueryBuilder(
                grammar=PostgresGrammar, connection="test_unique", table="product"
            )
            TenantScope().on_boot(builder)
            SoftDeleteScope().on_boot(builder)
            return builder.where(column, value)

    def _first(builder):
        compiled.append(builder.to_sql())
        return first()

    module = ModuleType("tests.fake_models_unique")
    module.Product = Product
    monkeypatch.setitem(sys.modules, module.__name__, module)
    monkeypatch.setattr(ModuleManager, "models_module", lambda: module.__name__)
    monkeypatch.setattr(QueryBuilder, "first", _first)
    return compiled


def test_the_lookup_runs_inside_the_tenant_and_excludes_trashed_rows(monkeypatch):
    compiled = _install_product(monkeypatch, first=lambda: None)

    assert UniqueRule().validate("sku", "SKU-1", {"unique": "product,sku"}) is True
    assert compiled == [
        'SELECT * FROM "product" WHERE "product"."sku" = \'SKU-1\' AND '
        '"product"."tenant_id" = \'5\' AND "product"."deleted_at" IS NULL'
    ]


def test_a_visible_row_holding_the_value_fails(monkeypatch):
    _install_product(monkeypatch, first=object)

    assert UniqueRule().validate("sku", "SKU-1", {"unique": "product,sku"}) is False


def test_the_ignore_clause_excludes_the_row_being_updated(monkeypatch):
    compiled = _install_product(monkeypatch, first=lambda: None)

    assert UniqueRule().validate("sku", "SKU-1", {"unique": "product,sku,9"}) is True
    assert '"product"."id" != \'9\'' in compiled[0]


def test_a_database_error_propagates_instead_of_reading_as_taken(monkeypatch):
    def _outage():
        raise RuntimeError("database unavailable")

    _install_product(monkeypatch, first=_outage)

    with pytest.raises(RuntimeError, match="database unavailable"):
        UniqueRule().validate("sku", "SKU-1", {"unique": "product,sku"})


def test_an_unsafe_identifier_is_a_configuration_error(monkeypatch):
    _install_product(monkeypatch, first=lambda: None)

    with pytest.raises(ConfigurationException, match="not a safe SQL identifier"):
        UniqueRule().validate("sku", "SKU-1", {"unique": 'product,sku"; --'})
