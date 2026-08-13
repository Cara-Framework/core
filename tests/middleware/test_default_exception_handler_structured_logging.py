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
from unittest.mock import MagicMock

import pytest

_handler_mod = importlib.import_module("cara.exceptions.handlers.DefaultExceptionHandler")
DefaultExceptionHandler = _handler_mod.DefaultExceptionHandler


class _FakeHttpException(Exception):
    status_code = 422

    def __init__(self, msg: str = "Validation failed") -> None:
        super().__init__(msg)


class _FakeServerException(Exception):
    status_code = 500

    def __init__(self, msg: str = "DB connection lost") -> None:
        super().__init__(msg)


def _make_handler_with_mock_log(monkeypatch: pytest.MonkeyPatch):
    """Patch the handler's canonical module-object facade seam."""
    mock_log = MagicMock()
    import cara.facades as facades

    monkeypatch.setattr(facades, "Log", mock_log)
    handler = DefaultExceptionHandler(application=None)
    return handler, mock_log


class TestLogExceptionStructuredContext:
    """The ``log_exception`` method must include structured context."""

    def test_4xx_logs_warning_with_context(self, monkeypatch: pytest.MonkeyPatch) -> None:
        handler, mock_log = _make_handler_with_mock_log(monkeypatch)
        exc = _FakeHttpException("bad input")
        handler.log_exception(exc)

        mock_log.warning.assert_called_once()
        call_kwargs = mock_log.warning.call_args
        assert "context" in call_kwargs.kwargs
        ctx = call_kwargs.kwargs["context"]
        assert ctx["status_code"] == 422
        assert ctx["exception_type"] == "_FakeHttpException"

    def test_5xx_logs_error_with_context_and_exc_info(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        handler, mock_log = _make_handler_with_mock_log(monkeypatch)
        exc = _FakeServerException("pool exhausted")
        handler.log_exception(exc)

        mock_log.error.assert_called_once()
        call_kwargs = mock_log.error.call_args
        assert "context" in call_kwargs.kwargs
        ctx = call_kwargs.kwargs["context"]
        assert ctx["status_code"] == 500
        assert ctx["exception_type"] == "_FakeServerException"
        assert call_kwargs.kwargs.get("exc_info") is True

    def test_4xx_uses_printf_not_fstring(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """The message template must use %s placeholders, not f-strings.

        f-strings eagerly interpolate at call site, defeating structured
        log indexing (every unique exception message creates a new
        log-line fingerprint). Printf-style lets the logger treat the
        template as a stable key.
        """
        handler, mock_log = _make_handler_with_mock_log(monkeypatch)
        exc = _FakeHttpException("field X required")
        handler.log_exception(exc)

        call_args = mock_log.warning.call_args
        template = call_args.args[0]
        assert "%s" in template

    def test_5xx_uses_printf_not_fstring(self, monkeypatch: pytest.MonkeyPatch) -> None:
        handler, mock_log = _make_handler_with_mock_log(monkeypatch)
        exc = _FakeServerException("timeout")
        handler.log_exception(exc)

        call_args = mock_log.error.call_args
        template = call_args.args[0]
        assert "%s" in template

    def test_category_is_cara_exceptions(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Both 4xx and 5xx must use category='cara.exceptions'."""
        for exc, method_name in [
            (_FakeHttpException(), "warning"),
            (_FakeServerException(), "error"),
        ]:
            handler, mock_log = _make_handler_with_mock_log(monkeypatch)
            handler.log_exception(exc)

            call_kwargs = getattr(mock_log, method_name).call_args.kwargs
            assert call_kwargs.get("category") == "cara.exceptions"
