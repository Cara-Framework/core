"""``Model.save()`` / ``Model.delete()`` raise database errors (Laravel parity).

They used to catch every exception, log it and return ``False``. No caller
read the bool, so a pool timeout or a lock timeout lost the write behind a
success response; inside a transaction the swallowed error aborted it, the
NEXT statement failed with a misleading 25P02, ``atomic(attempts=N)`` never
saw the 40001 it retries on, and a ``save()`` that was the block's last
statement had its aborted transaction silently rolled back by COMMIT.
``False`` now means one thing: an event listener cancelled the write (or no
row matched).
"""

from __future__ import annotations

import importlib
from unittest.mock import MagicMock

import pytest

from cara.decorators.Events import saving
from cara.eloquent.Atomic import atomic
from cara.eloquent.connections.ConnectionResolver import (
    _get_after_commit_registry,
    _get_registry,
)
from cara.eloquent.DatabaseManager import DatabaseManager
from cara.eloquent.Integrity import sqlstate_of
from cara.eloquent.models import Model
from cara.exceptions import DatabaseUnavailableException, QueryException
from cara.testing.FacadeSwap import swap

_ATOMIC_MODULE = importlib.import_module("cara.eloquent.Atomic")


class _Widget(Model):
    __table__ = "widget"
    __fillable__ = ["name"]


class _Guarded(Model):
    __table__ = "guarded_widget"
    __fillable__ = ["name"]

    @saving
    def refuse(self):
        return False


class _DriverError(Exception):
    def __init__(self, pgcode):
        super().__init__(pgcode)
        self.pgcode = pgcode


def _query_error(pgcode):
    try:
        raise QueryException("query failed") from _DriverError(pgcode)
    except QueryException as exc:
        return exc


def _builder(monkeypatch, model, **behaviour):
    builder = MagicMock()
    builder.where.return_value = builder
    for method, effect in behaviour.items():
        getattr(builder, method).side_effect = effect
    monkeypatch.setattr(model, "get_builder", lambda _self: builder)
    return builder


def test_an_update_error_reaches_the_caller_with_its_sqlstate(monkeypatch):
    _builder(monkeypatch, _Widget, update=_query_error("23505"))
    widget = _Widget.hydrate({"id": 7, "name": "old"})
    widget.name = "new"

    with pytest.raises(QueryException) as raised:
        widget.save()

    assert sqlstate_of(raised.value) == "23505"
    # Nothing was merged as if it had been written.
    assert widget.get_dirty_attributes() == {"name": "new"}


def test_an_insert_error_reaches_the_caller(monkeypatch):
    def unavailable(cls, *args, **kwargs):
        raise DatabaseUnavailableException("pool exhausted")

    # Constructing a model boots its builder; keep that off the DB facade.
    _builder(monkeypatch, _Widget)
    monkeypatch.setattr(_Widget, "create", classmethod(unavailable))

    with pytest.raises(DatabaseUnavailableException):
        _Widget(name="new").save()


def test_a_delete_error_reaches_the_caller(monkeypatch):
    _builder(monkeypatch, _Widget, delete=_query_error("55P03"))

    with pytest.raises(QueryException):
        _Widget.hydrate({"id": 7, "name": "old"}).delete()


def test_false_still_means_a_listener_cancelled_the_write(monkeypatch):
    builder = _builder(monkeypatch, _Guarded)
    guarded = _Guarded.hydrate({"id": 7, "name": "old"})
    guarded.name = "new"

    assert guarded.save() is False
    builder.update.assert_not_called()


def test_a_failed_quiet_save_switches_the_observers_back_on(monkeypatch):
    """``without_events`` flips a CLASS flag; a raising save must not leave
    every later write on the model silent for the rest of the process."""
    _builder(monkeypatch, _Widget, update=_query_error("57014"))
    widget = _Widget.hydrate({"id": 7, "name": "old"})
    widget.name = "new"

    with pytest.raises(QueryException):
        widget.save_quietly()

    assert _Widget.__has_events__ is True


class _FakeConnection:
    """``PostgresConnection``'s transaction-level math, nothing else."""

    def __init__(self):
        self.transaction_level = 0

    def begin(self):
        self.transaction_level += 1
        return self

    def commit(self):
        self.transaction_level = max(0, self.transaction_level - 1)
        return self

    def rollback(self):
        self.transaction_level = max(0, self.transaction_level - 1)
        return self

    def close_connection(self):
        pass


def test_atomic_retries_a_serialization_failure_raised_inside_save(monkeypatch):
    _get_registry().clear()
    _get_after_commit_registry().clear()
    connection = _FakeConnection()
    manager = DatabaseManager("app", {"app": {"driver": "sqlite"}})
    monkeypatch.setattr(
        manager._ensure_resolver(),
        "_create_connection_instance",
        lambda name: connection,
        raising=False,
    )
    monkeypatch.setattr(_ATOMIC_MODULE.time, "sleep", lambda _seconds: None)

    outcomes = iter([_query_error("40001"), 1])

    def update(values, **options):
        outcome = next(outcomes)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    _builder(monkeypatch, _Widget, update=update)
    runs: list[int] = []

    @atomic(attempts=2)
    def rename():
        runs.append(1)
        widget = _Widget.hydrate({"id": 7, "name": "old"})
        widget.name = "new"
        return widget.save()

    try:
        with swap("DB", manager):
            assert rename() is True
    finally:
        _get_registry().clear()
        _get_after_commit_registry().clear()

    assert len(runs) == 2
