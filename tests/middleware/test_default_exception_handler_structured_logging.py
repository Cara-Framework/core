"""``DefaultExceptionHandler.log_exception`` — structured context contract.

Ensures that ``log_exception`` passes structured ``context=`` with
``status_code`` and ``exception_type`` to the Log facade, and uses
printf-style ``%s`` interpolation instead of f-strings.

The structured fields let log aggregators (Grafana Loki, Datadog) index
and filter on status code and exception type without regex-parsing the
message body.
"""

from __future__ import annotations

import importlib
import sys
import types
from unittest.mock import MagicMock

_handler_mod = importlib.import_module(
    "cara.exceptions.handlers.DefaultExceptionHandler"
)
DefaultExceptionHandler = _handler_mod.DefaultExceptionHandler


class _FakeHttpException(Exception):
    status_code = 422

    def __init__(self, msg: str = "Validation failed") -> None:
        super().__init__(msg)


class _FakeServerException(Exception):
    status_code = 500

    def __init__(self, msg: str = "DB connection lost") -> None:
        super().__init__(msg)


def _make_handler_with_mock_log():
    """Create a handler and a mock Log, patching the lazy import inside
    log_exception. The method does ``from cara.facades import Log`` at
    call time; we inject a mock module into sys.modules so the import
    resolves without pulling the full framework dependency chain."""
    mock_log = MagicMock()

    # Build a tiny fake ``cara.facades`` module with only ``Log``.
    fake_facades = types.ModuleType("cara.facades")
    fake_facades.Log = mock_log  # type: ignore[attr-defined]

    handler = DefaultExceptionHandler(application=None)
    return handler, mock_log, fake_facades


class TestLogExceptionStructuredContext:
    """The ``log_exception`` method must include structured context."""

    def test_4xx_logs_warning_with_context(self) -> None:
        handler, mock_log, fake_facades = _make_handler_with_mock_log()
        exc = _FakeHttpException("bad input")

        original = sys.modules.get("cara.facades")
        sys.modules["cara.facades"] = fake_facades
        try:
            handler.log_exception(exc)
        finally:
            if original is not None:
                sys.modules["cara.facades"] = original
            else:
                sys.modules.pop("cara.facades", None)

        mock_log.warning.assert_called_once()
        call_kwargs = mock_log.warning.call_args
        assert "context" in call_kwargs.kwargs
        ctx = call_kwargs.kwargs["context"]
        assert ctx["status_code"] == 422
        assert ctx["exception_type"] == "_FakeHttpException"

    def test_5xx_logs_error_with_context_and_exc_info(self) -> None:
        handler, mock_log, fake_facades = _make_handler_with_mock_log()
        exc = _FakeServerException("pool exhausted")

        original = sys.modules.get("cara.facades")
        sys.modules["cara.facades"] = fake_facades
        try:
            handler.log_exception(exc)
        finally:
            if original is not None:
                sys.modules["cara.facades"] = original
            else:
                sys.modules.pop("cara.facades", None)

        mock_log.error.assert_called_once()
        call_kwargs = mock_log.error.call_args
        assert "context" in call_kwargs.kwargs
        ctx = call_kwargs.kwargs["context"]
        assert ctx["status_code"] == 500
        assert ctx["exception_type"] == "_FakeServerException"
        assert call_kwargs.kwargs.get("exc_info") is True

    def test_4xx_uses_printf_not_fstring(self) -> None:
        """The message template must use %s placeholders, not f-strings.

        f-strings eagerly interpolate at call site, defeating structured
        log indexing (every unique exception message creates a new
        log-line fingerprint). Printf-style lets the logger treat the
        template as a stable key.
        """
        handler, mock_log, fake_facades = _make_handler_with_mock_log()
        exc = _FakeHttpException("field X required")

        original = sys.modules.get("cara.facades")
        sys.modules["cara.facades"] = fake_facades
        try:
            handler.log_exception(exc)
        finally:
            if original is not None:
                sys.modules["cara.facades"] = original
            else:
                sys.modules.pop("cara.facades", None)

        call_args = mock_log.warning.call_args
        template = call_args.args[0]
        assert "%s" in template

    def test_5xx_uses_printf_not_fstring(self) -> None:
        handler, mock_log, fake_facades = _make_handler_with_mock_log()
        exc = _FakeServerException("timeout")

        original = sys.modules.get("cara.facades")
        sys.modules["cara.facades"] = fake_facades
        try:
            handler.log_exception(exc)
        finally:
            if original is not None:
                sys.modules["cara.facades"] = original
            else:
                sys.modules.pop("cara.facades", None)

        call_args = mock_log.error.call_args
        template = call_args.args[0]
        assert "%s" in template

    def test_category_is_cara_exceptions(self) -> None:
        """Both 4xx and 5xx must use category='cara.exceptions'."""
        for exc, method_name in [
            (_FakeHttpException(), "warning"),
            (_FakeServerException(), "error"),
        ]:
            handler, mock_log, fake_facades = _make_handler_with_mock_log()

            original = sys.modules.get("cara.facades")
            sys.modules["cara.facades"] = fake_facades
            try:
                handler.log_exception(exc)
            finally:
                if original is not None:
                    sys.modules["cara.facades"] = original
                else:
                    sys.modules.pop("cara.facades", None)

            call_kwargs = getattr(mock_log, method_name).call_args.kwargs
            assert call_kwargs.get("category") == "cara.exceptions"
