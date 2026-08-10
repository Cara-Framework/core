"""``_log_debug`` must not re-import the facade whose failure it survives.

Three response-path middlewares carried a byte-identical helper::

    try:
        from cara.facades import Log

        Log.debug(msg, category=...)
    except Exception:
        from cara.facades import Log

        Log.warning(msg, exc_info=True)

under a docstring claiming it "survives partial-boot". Both halves were
false, with two concrete wrong behaviours:

1. In the exact scenario it was written for — ``cara.facades``
   unimportable — the ``except`` branch ran the SAME import and raised out
   of ``handle``, turning a servable response into a 500. A logging helper
   became the cause of the outage it was meant to report.
2. ``except Exception`` also caught ordinary ``Log.debug`` failures and
   escalated a debug message to ``Log.warning(..., exc_info=True)``,
   attributing it to the wrong severity.

The function-local import was never a cycle-breaker either — importing
``cara.facades`` does not pull these modules in — so it belongs at the top
of the file, which is what these tests pin.
"""

from __future__ import annotations

import ast
import importlib
import inspect
from pathlib import Path

import pytest

_MODULES = {
    name: (
        importlib.import_module(f"cara.middleware.http.{name}"),
        category,
    )
    for name, category in (
        ("ConditionalGet", "cara.http.conditional_get"),
        ("CompressResponses", "cara.http.compress_responses"),
        ("SecurityHeaders", "cara.http.security_headers"),
    )
}


class _Recorder:
    """Stands in for the ``Log`` facade the module resolved at import."""

    def __init__(self, debug_error: Exception | None = None) -> None:
        self.debug_calls: list[tuple[str, dict]] = []
        self.warning_calls: list[tuple[tuple, dict]] = []
        self._debug_error = debug_error

    def debug(self, msg, *args, **kwargs) -> None:
        self.debug_calls.append((msg, kwargs))
        if self._debug_error is not None:
            raise self._debug_error

    def warning(self, *args, **kwargs) -> None:
        self.warning_calls.append((args, kwargs))


@pytest.mark.parametrize("name", sorted(_MODULES))
class TestLogDebugUsesTheModuleLevelFacade:
    def test_the_facade_is_bound_at_module_level(self, name: str) -> None:
        """The module must own a top-level ``Log`` symbol.

        Against the old code this fails outright: the only reference to the
        facade lived inside the helper body.
        """
        module, _ = _MODULES[name]
        assert hasattr(module, "Log")

    def test_debug_goes_through_that_symbol_with_its_category(
        self, name: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        module, category = _MODULES[name]
        recorder = _Recorder()
        monkeypatch.setattr(module, "Log", recorder)

        getattr(module, name)._log_debug("boom")

        assert recorder.debug_calls == [("boom", {"category": category})]

    def test_a_failing_debug_is_not_escalated_to_a_warning(
        self, name: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The old ``except`` turned any ``Log.debug`` failure into a
        WARNING with a stack trace, so operators saw debug noise reported
        at the wrong severity — and, when the facade genuinely was
        unavailable, the handler raised too."""
        module, _ = _MODULES[name]
        recorder = _Recorder(debug_error=RuntimeError("sink down"))
        monkeypatch.setattr(module, "Log", recorder)

        with pytest.raises(RuntimeError, match="sink down"):
            getattr(module, name)._log_debug("boom")

        assert recorder.warning_calls == []


@pytest.mark.parametrize("name", sorted(_MODULES))
def test_no_function_local_facade_import_remains(name: str) -> None:
    """A ``from cara.facades import ...`` inside a function body in these
    modules is the copied workaround coming back."""
    module, _ = _MODULES[name]
    tree = ast.parse(Path(inspect.getfile(module)).read_text(encoding="utf-8"))

    offenders = [
        node.lineno
        for scope in ast.walk(tree)
        if isinstance(scope, ast.FunctionDef | ast.AsyncFunctionDef)
        for node in ast.walk(scope)
        if isinstance(node, ast.ImportFrom) and (node.module or "") == "cara.facades"
    ]

    assert not offenders, (
        f"{name}: function-local cara.facades import at line(s) {offenders}"
    )
