"""An ambient carrier that fails to encode or decode is said, by type only.

The live defect: ``Ambient.capture`` / ``Ambient.restored`` swallowed a
carrier's exception with a bare ``except Exception: continue`` — the job ran
without the fact and nothing recorded why. The fix logs a warning naming the
error TYPE; the value itself (which may be anything the var held) must never
reach the log.
"""

from __future__ import annotations

import logging
from contextvars import ContextVar

import pytest

from cara.context.Ambient import Ambient

_SECRET = "tenant-secret-4242"


class _Records(logging.Handler):
    def __init__(self) -> None:
        super().__init__(logging.DEBUG)
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


@pytest.fixture
def records(monkeypatch):
    monkeypatch.setattr(Ambient, "_carriers", {})
    logger = logging.getLogger("cara.context")
    handler = _Records()
    logger.addHandler(handler)
    previous = logger.level
    logger.setLevel(logging.DEBUG)
    yield handler.records
    logger.removeHandler(handler)
    logger.setLevel(previous)


def _refuse(value):
    raise ValueError(f"cannot carry {value}")


def test_an_encode_failure_is_a_warning_naming_the_type_not_the_value(records):
    var: ContextVar = ContextVar("ambient_test_encode", default=None)
    Ambient.register("audit_lane", var, encode=_refuse, decode=str)
    token = var.set(_SECRET)
    try:
        captured = Ambient.capture()
    finally:
        var.reset(token)

    assert "audit_lane" not in captured
    (record,) = records
    assert record.levelno == logging.WARNING
    message = record.getMessage()
    assert "ValueError" in message and "audit_lane" in message
    assert _SECRET not in message
    assert record.exc_info is None


def test_a_decode_failure_is_a_warning_naming_the_type_not_the_value(records):
    var: ContextVar = ContextVar("ambient_test_decode", default=None)
    Ambient.register("audit_lane", var, encode=str, decode=_refuse)

    with Ambient.restored({"audit_lane": _SECRET}):
        assert var.get() is None

    (record,) = records
    assert record.levelno == logging.WARNING
    message = record.getMessage()
    assert "ValueError" in message and "audit_lane" in message
    assert _SECRET not in message
    assert record.exc_info is None


def test_a_healthy_carrier_logs_nothing(records):
    var: ContextVar = ContextVar("ambient_test_healthy", default=None)
    Ambient.register("audit_lane", var, encode=str, decode=str)
    token = var.set("north")
    try:
        captured = Ambient.capture()
    finally:
        var.reset(token)

    with Ambient.restored(captured):
        assert var.get() == "north"
    assert records == []
