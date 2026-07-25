"""DB-backed rules are skipped once a shape rule already rejected the value.

``exists``/``unique`` answer their question by BINDING THE RAW VALUE into a
typed column comparison. When an earlier rule in the same chain already
rejected the value, running them is meaningless — and on a strict engine it is
a crash:
Postgres answers ``invalid input syntax for type bigint`` for
``exists:product,id`` with a public id like ``PRD01K…``, and that
``QueryException`` escapes the validator as a **500** where validation owes
the caller a **422**.

This was live: ``GET /api/clicks/redirect/{product_id}/{marketplace_id}``
returned 500 for any non-numeric ``product_id`` even though its FormRequest
chain (``required|integer|exists:product,id``) was already correct. The fix
belongs in the validator, not in every FormRequest — otherwise one forgotten
``bail`` re-opens the hole.

Guards the two halves of the contract:
  * a failed shape rule stops the DB rule from ever running (no query, 422),
  * a PASSING shape rule still lets the DB rule run (``exists`` keeps gating).
"""

from __future__ import annotations

import sys
from types import ModuleType

import pytest

from cara.support import ModuleManager
from cara.validation import Validation


class _ExplodingQuery:
    """Stands in for a typed-column comparison the DB refuses to run.

    Mirrors psycopg2 raising on ``WHERE id = 'PRD01K…'`` against a bigint
    column: reaching this at all is the bug under test.
    """

    def where(self, _column: str, _value):
        return self

    def first(self):
        raise AssertionError(
            "exists/unique ran after a shape rule already failed — "
            "this is the 500-instead-of-422 regression"
        )


def _install_models(monkeypatch: pytest.MonkeyPatch, *models: type) -> None:
    module_name = "tests.fake_models_db_backed"
    module = ModuleType(module_name)
    for model in models:
        setattr(module, model.__name__, model)
    monkeypatch.setitem(sys.modules, module_name, module)
    monkeypatch.setattr(ModuleManager, "models_module", lambda: module_name)


def _install_exploding_product(monkeypatch: pytest.MonkeyPatch) -> None:
    class Product:
        __table__ = "product"

        @classmethod
        def where(cls, _column: str, _value):
            return _ExplodingQuery()

    _install_models(monkeypatch, Product)


def test_exists_is_skipped_when_integer_rule_already_failed(monkeypatch) -> None:
    _install_exploding_product(monkeypatch)

    validator = Validation.make(
        {"product_id": "PRD01KWYDX779KDQSZRV1A008MHKC"},
        {"product_id": "required|integer|exists:product,id"},
    )

    # No AssertionError from _ExplodingQuery => the DB rule never ran.
    assert validator.fails() is True
    assert "integer" in validator.errors().first("product_id").lower()


def test_unique_is_skipped_when_shape_rule_already_failed(monkeypatch) -> None:
    _install_exploding_product(monkeypatch)

    validator = Validation.make(
        {"product_id": "not-a-number"},
        {"product_id": "required|integer|unique:product,id"},
    )

    assert validator.fails() is True


def test_exists_still_runs_when_the_shape_rule_passes(monkeypatch) -> None:
    """The skip must be scoped to ALREADY-FAILED fields — a valid-shaped
    value must still be gated by the database lookup."""
    seen: list[tuple[str, object]] = []

    class _MissingQuery:
        def where(self, column: str, value):
            seen.append((column, value))
            return self

        def first(self):
            return None  # row not found -> exists must fail the field

    class Product:
        __table__ = "product"

        @classmethod
        def where(cls, column: str, value):
            return _MissingQuery().where(column, value)

    _install_models(monkeypatch, Product)

    validator = Validation.make(
        {"product_id": 999_999_999},
        {"product_id": "required|integer|exists:product,id"},
    )

    assert validator.fails() is True
    assert seen == [("id", 999_999_999)]
