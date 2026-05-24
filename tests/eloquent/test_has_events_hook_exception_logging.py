"""Regression tests for :class:`HasEvents._fire_model_event`.

ROOT CAUSE
----------
Pre-fix, ``_fire_model_event`` wrapped every event-method call in::

    try:
        result = method(self, **kwargs)
        if result is False:
            return False
    except Exception:
        # Log the error but don't stop other handlers
        pass

The comment claimed the failure was logged but no logging actually
happened — the bare ``pass`` ate the exception whole. Consequences
on the ``@saving`` / ``@creating`` / ``@updating`` lifecycle:

* a validation hook raising :class:`ValueError` returned ``True`` to
  the caller, so the model went on to persist the unverified row;
* the underlying exception never reached stdout / Sentry / log files,
  so the failure was completely invisible at runtime;
* observer failures **were** already logged (sibling path
  ``_fire_observers``) — the silent gap was specific to model-class
  event methods, the inconsistency made the bug doubly confusing.

Contract pinned by these tests:

* an exception raised in a model event method is forwarded to
  ``cara.facades.Log.error`` with the model class name + event name
  in the message;
* the other handlers for the same event still run (matches the
  ``_fire_observers`` continue-on-failure policy);
* a hook returning ``False`` still cancels the operation (the fix
  must not swallow the explicit-cancel signal).
"""

from __future__ import annotations

from typing import Any

import pytest

from cara.eloquent.concerns.HasEvents import HasEvents


# ── Lightweight stand-in for cara.facades.Log ────────────────────────


class _LogSpy:
    """Minimal capture target — records every ``error`` call.

    We can't reach for the full ``LogFake`` because that lives in
    ``cara.testing`` and brings in the facade-swap machinery that's
    only wired into the ``api/`` and ``services/`` conftests. The
    spy speaks the same surface: ``error(message, *, category, ...)``.
    """

    def __init__(self) -> None:
        self.errors: list[tuple[str, dict[str, Any]]] = []

    def error(self, message: Any, **kwargs: Any) -> None:
        self.errors.append((str(message), kwargs))


@pytest.fixture
def log_spy(monkeypatch: pytest.MonkeyPatch) -> _LogSpy:
    """Replace ``cara.facades.Log`` with a recording spy.

    ``_fire_model_event`` imports the facade lazily inside the except
    block (``from cara.facades import Log``) so we patch the module-
    level attribute before the failure path runs.
    """
    import cara.facades as facades

    spy = _LogSpy()
    monkeypatch.setattr(facades, "Log", spy)
    return spy


# ── Stand-in model that exercises just HasEvents ─────────────────────


class _StubModel(HasEvents):
    """A no-DB model: just the bits ``_fire_model_event`` reaches for.

    Real ``Model`` subclasses pull in ``HasAttributes`` /
    ``HasRelationships`` / a query builder — none of that is needed
    to verify the event-fire control flow. We only need attributes
    flagged with ``_event_type`` so ``_get_model_events`` discovers
    them.
    """

    def __init__(self) -> None:
        # Mirror the real ``HasEvents.__init__`` super() chain (which
        # is a no-op without other mixins).
        super().__init__()


def _hook(event_type: str):
    """Decorator that tags a function with ``_event_type`` — the same
    marker ``_get_model_events`` scans for via ``hasattr``."""

    def wrap(fn):
        fn._event_type = event_type
        return fn

    return wrap


# ── Tests ────────────────────────────────────────────────────────────


class TestRaisingHookIsLogged:
    """Pre-fix this was the silent-swallow path."""

    def test_saving_hook_exception_is_forwarded_to_log_error(
        self, log_spy: _LogSpy
    ) -> None:
        class _M(_StubModel):
            @_hook("saving")
            def _validate(self, **kwargs: Any) -> None:
                raise ValueError("attribute violates business invariant")

        # Fire the event — must NOT raise (continue-on-failure
        # contract), but MUST land an ``error`` record on the log.
        result = _M()._fire_model_event("saving")

        # The hook didn't return ``False`` so the operation continues
        # (the existing semantics — a hook that raises is no different
        # from one that returns ``None``; only an explicit ``return
        # False`` cancels).
        assert result is True
        assert len(log_spy.errors) == 1
        message, kwargs = log_spy.errors[0]
        # Message must name the event + model so a grepping operator
        # can localise the failure without reading source.
        assert "saving" in message
        assert "_M" in message
        assert "attribute violates business invariant" in message
        # Category mirrors the sibling ``_fire_observers`` channel
        # so log routing rules stay symmetric.
        assert kwargs.get("category") == "cara.eloquent.events"

    def test_creating_hook_exception_is_logged(self, log_spy: _LogSpy) -> None:
        # Sibling lifecycle — same code path, just verifying the
        # event-name lands in the log message.
        class _M(_StubModel):
            @_hook("creating")
            def _validate(self, **kwargs: Any) -> None:
                raise RuntimeError("creating-time check failed")

        _M()._fire_model_event("creating")
        assert log_spy.errors, "expected creating-hook failure to be logged"
        message, _ = log_spy.errors[0]
        assert "creating" in message

    def test_remaining_hooks_run_after_one_raises(self, log_spy: _LogSpy) -> None:
        # Continue-on-failure: a buggy validator must not block other
        # hooks (audit, fan-out, etc.) attached to the same event.
        ran: list[str] = []

        class _M(_StubModel):
            @_hook("saving")
            def _a_raises(self, **kwargs: Any) -> None:
                ran.append("a")
                raise RuntimeError("hook a blew up")

            @_hook("saving")
            def _b_runs(self, **kwargs: Any) -> None:
                ran.append("b")

        # Order between two hooks discovered via ``dir()`` is
        # alphabetical — both should run.
        _M()._fire_model_event("saving")
        assert "a" in ran and "b" in ran
        # And the failure for ``_a_raises`` landed in the log even
        # though ``_b_runs`` succeeded.
        assert len(log_spy.errors) == 1
        message, _ = log_spy.errors[0]
        assert "hook a blew up" in message


class TestExplicitCancelStillWins:
    """The fix must not collapse the ``return False`` cancel signal
    into the new logging branch."""

    def test_hook_returning_false_cancels_without_logging(
        self, log_spy: _LogSpy
    ) -> None:
        class _M(_StubModel):
            @_hook("saving")
            def _veto(self, **kwargs: Any) -> bool:
                return False

        result = _M()._fire_model_event("saving")
        assert result is False
        # Cancellation is a normal control flow signal, not an error
        # — nothing should land in the log.
        assert log_spy.errors == []
