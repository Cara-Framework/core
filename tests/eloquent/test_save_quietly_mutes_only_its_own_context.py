"""``save_quietly`` mutes observers for its own call only (ORM audit).

It used to flip the CLASS flag ``__has_events__``, so while one thread or task
saved quietly, every concurrent save of that model class lost its observer
events. The mute is now a ``ContextVar`` scoped to the quiet call.
"""

from __future__ import annotations

import sqlite3
import threading

import pytest

from cara.eloquent import DatabaseManager
from cara.eloquent.models.Model import Model
from cara.testing.FacadeSwap import swap


class _Note(Model):
    __table__ = "note"
    __fillable__ = ["*"]
    __timestamps__ = False

    def save(self, **kwargs):
        # A quiet save parks here (inside its mute) until the loud one is done.
        gate = self.__dict__.get("_gate")
        if gate is not None:
            entered, release = gate
            entered.set()
            assert release.wait(5)
        return super().save(**kwargs)


class _Recorder:
    def __init__(self):
        self.seen: list[tuple[str, int]] = []

    def updating(self, model):
        self.seen.append(("updating", model.id))

    def updated(self, model):
        self.seen.append(("updated", model.id))


@pytest.fixture
def recorder(tmp_path):
    path = tmp_path / "quiet.sqlite"
    raw = sqlite3.connect(path)
    raw.executescript(
        """
        CREATE TABLE note (id INTEGER PRIMARY KEY, body TEXT);
        INSERT INTO note (id, body) VALUES (1, 'a'), (2, 'b');
        """
    )
    raw.commit()
    raw.close()

    observer = _Recorder()
    _Note.observe(observer)
    dm = DatabaseManager("app", {"app": {"driver": "sqlite", "database": str(path)}})
    try:
        with swap("DB", dm):
            yield observer
    finally:
        _Note.__observers__.pop(_Note, None)


def test_a_quiet_save_fires_no_observer_event(recorder):
    note = _Note.hydrate({"id": 1, "body": "a"})
    note.body = "quiet"

    assert note.save_quietly() is True
    assert recorder.seen == []

    note.body = "loud"
    assert note.save() is True
    assert recorder.seen == [("updating", 1), ("updated", 1)]


def test_a_concurrent_save_keeps_its_events_while_another_saves_quietly(recorder):
    entered, release = threading.Event(), threading.Event()
    quiet = _Note.hydrate({"id": 1, "body": "a"})
    quiet.__dict__["_gate"] = (entered, release)
    quiet.body = "quiet"
    errors: list[BaseException] = []

    def run_quiet():
        try:
            quiet.save_quietly()
        except BaseException as exc:  # surfaced by the assertion below
            errors.append(exc)

    worker = threading.Thread(target=run_quiet)
    worker.start()
    try:
        assert entered.wait(5)
        # The quiet save is now inside its mute on the other thread.
        loud = _Note.hydrate({"id": 2, "body": "b"})
        loud.body = "loud"
        assert loud.save() is True
    finally:
        release.set()
        worker.join(5)

    assert errors == []
    assert recorder.seen == [("updating", 2), ("updated", 2)]
    assert _Note.__has_events__ is True


def test_the_mute_is_lifted_when_the_quiet_save_raises(recorder, monkeypatch):
    note = _Note.hydrate({"id": 1, "body": "a"})
    note.body = "x"

    def boom(self, **kwargs):
        raise RuntimeError("write failed")

    monkeypatch.setattr(Model, "save", boom)
    with pytest.raises(RuntimeError):
        note.save_quietly()
    monkeypatch.undo()

    other = _Note.hydrate({"id": 2, "body": "b"})
    other.body = "y"
    other.save()
    assert recorder.seen == [("updating", 2), ("updated", 2)]
