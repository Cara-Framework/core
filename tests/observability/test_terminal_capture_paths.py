"""Errors must have a real path into Sentry, not just scope decoration.

Every terminal catch point (the HTTP handler's 500 branch, the worker's
failure handlers, ``helpers.report``) consumes its exception without
re-raising, so no SDK integration hook ever fires. Before the explicit
``capture_exception`` path, a fully initialized Sentry could receive
nothing but interpreter-crashing boot failures — request users were
dutifully attached to events that could never exist.
"""

from __future__ import annotations

import importlib

observability = importlib.import_module("cara.observability")
sentry_module = importlib.import_module("cara.observability.Sentry")


def test_capture_exception_reaches_the_sdk(monkeypatch):
    """The SDK is an optional dependency, so a stand-in module keeps this
    pin meaningful on environments without ``sentry_sdk`` installed."""
    import sys
    from types import SimpleNamespace

    seen = []
    monkeypatch.setitem(
        sys.modules,
        "sentry_sdk",
        SimpleNamespace(capture_exception=lambda exc: seen.append(exc)),
    )

    error = RuntimeError("boom")
    sentry_module.capture_exception(error)

    assert seen == [error]


def test_capture_exception_swallows_sdk_failures(monkeypatch):
    import sys
    from types import SimpleNamespace

    def _boom(_exc):
        raise RuntimeError("transport down")

    monkeypatch.setitem(
        sys.modules, "sentry_sdk", SimpleNamespace(capture_exception=_boom)
    )

    sentry_module.capture_exception(ValueError("original"))  # must not raise


def test_the_http_500_branch_captures_the_exception(monkeypatch):
    from cara.exceptions.handlers.DefaultExceptionHandler import (
        DefaultExceptionHandler,
    )

    seen = []
    monkeypatch.setattr(
        observability, "capture_exception", lambda exc: seen.append(exc)
    )

    handler = DefaultExceptionHandler()
    error = RuntimeError("server fault")

    handler.log_exception(error)

    assert seen == [error]


def test_a_4xx_exception_is_not_captured(monkeypatch):
    from cara.exceptions.handlers.DefaultExceptionHandler import (
        DefaultExceptionHandler,
    )

    seen = []
    monkeypatch.setattr(
        observability, "capture_exception", lambda exc: seen.append(exc)
    )

    error = RuntimeError("missing")
    error.status_code = 404

    DefaultExceptionHandler().log_exception(error)

    assert seen == []


def test_helpers_report_captures_before_logging(monkeypatch):
    from cara import helpers

    seen = []
    monkeypatch.setattr(
        observability, "capture_exception", lambda exc: seen.append(exc)
    )

    error = ValueError("best effort branch failed")
    helpers.report(error)

    assert seen == [error]
