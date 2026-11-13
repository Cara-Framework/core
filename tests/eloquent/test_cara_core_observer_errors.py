"""An observer bug is logged, not silently swallowed.

``observe_events`` used to wrap the handler CALL in ``except AttributeError:
pass`` to skip observers that do not handle an event. That also swallowed,
with no log line, every ``AttributeError`` raised inside a real handler (a
typo'd attribute, a None dereference). Missing handlers are now skipped by a
lookup before the call; errors inside a handler reach the logged branch.
"""

from __future__ import annotations

import logging

from cara.eloquent.observers.ObservesEvents import ObservesEvents


class _Observer:
    def __init__(self):
        self.seen = []

    def saved(self, model):
        self.seen.append("saved")

    def updated(self, model):
        raise AttributeError("'NoneType' object has no attribute 'id'")


class _Model(ObservesEvents):
    __has_events__ = True
    __observers__: dict = {}


def _model_with(observer):
    _Model.__observers__ = {_Model: [observer]}
    return _Model()


def test_event_without_handler_is_skipped_quietly(caplog):
    observer = _Observer()
    model = _model_with(observer)

    with caplog.at_level(logging.ERROR):
        model.observe_events(model, "creating")
        model.observe_events(model, "saved")

    assert observer.seen == ["saved"]
    assert not [r for r in caplog.records if r.levelno >= logging.ERROR]


def test_attribute_error_inside_handler_is_logged(caplog):
    model = _model_with(_Observer())

    with caplog.at_level(logging.ERROR):
        model.observe_events(model, "updated")

    assert any(
        "Observer _Observer.updated failed" in r.getMessage() for r in caplog.records
    )
