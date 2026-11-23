"""A reused builder runs each statement on the context's CURRENT connection.

``QueryBuilder.new_connection()`` kept the first wrapper it resolved for the
life of the builder, and a model instance keeps its builder between calls
(``model.update()``, ``model.refresh()``, ``model.where(...)``). The wrapper a
builder resolves OUTSIDE a transaction is a plain autocommit one, so a later
write through the same instance INSIDE ``DB.transaction()`` went out on a
sibling autocommit session: the rollback could not undo it.

Runs the real builder against a file-backed SQLite database (an ``:memory:``
database would be a fresh, empty database on every connection).

Pins:
* a model written once outside a transaction, then written inside one that
  rolls back, holds the value from before the transaction;
* the same through a builder object reused across the boundary;
* a wrapper resolved inside one transaction is not carried into the next;
* a builder's OWN transaction (``builder.begin()``) keeps its wrapper for
  every statement until it ends.
"""

from __future__ import annotations

import sqlite3

import pytest

from cara.eloquent import DatabaseManager
from cara.eloquent.models.Model import Model
from cara.testing.FacadeSwap import swap


class _Account(Model):
    __table__ = "account"
    __fillable__ = ["*"]
    __timestamps__ = False


@pytest.fixture
def manager(tmp_path):
    path = tmp_path / "pin.sqlite"
    raw = sqlite3.connect(path)
    raw.executescript(
        """
        CREATE TABLE account (id INTEGER PRIMARY KEY, label TEXT);
        INSERT INTO account (id, label) VALUES (1, 'start');
        """
    )
    raw.commit()
    raw.close()

    dm = DatabaseManager("app", {"app": {"driver": "sqlite", "database": str(path)}})
    with swap("DB", dm):
        yield dm


def _stored_label() -> str:
    return _Account.query().where("id", 1).first().label


def test_model_written_before_a_transaction_rolls_back_inside_it(manager):
    account = _Account.find(1)
    account.update({"label": "outside"})

    with pytest.raises(RuntimeError), manager.transaction():
        account.update({"label": "inside"})
        raise RuntimeError("abort")

    assert _stored_label() == "outside"


def test_builder_reused_across_the_boundary_joins_the_transaction(manager):
    builder = manager.table("account")
    builder.where("id", 1).update({"label": "outside"})

    with pytest.raises(RuntimeError), manager.transaction():
        builder.where("id", 1).update({"label": "inside"})
        raise RuntimeError("abort")

    assert _stored_label() == "outside"


def test_wrapper_of_one_transaction_is_not_carried_into_the_next(manager):
    account = _Account.find(1)
    with manager.transaction():
        account.update({"label": "first"})

    with pytest.raises(RuntimeError), manager.transaction():
        account.update({"label": "second"})
        raise RuntimeError("abort")

    assert _stored_label() == "first"


def test_builder_owned_transaction_keeps_its_wrapper(manager):
    builder = manager.query().table("account")
    builder.begin()
    own = builder.get_connection()

    builder.where("id", 1).update({"label": "pending"})
    assert builder.new_connection() is own
    builder.rollback()

    assert _stored_label() == "start"
