"""Two logging-path defects.

* ``Logger._log`` rendered ``message % args`` BEFORE the level/category
  filter, so a discarded ``Log.debug("…%s", payload)`` still paid for it.
* With interception on, the framework's own ``logging.getLogger("cara.…")``
  warnings and errors reached a root ``NullHandler`` and were discarded.
"""

from __future__ import annotations

import importlib
import logging
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

from cara.logging import CategoryFilter, Logger

adapter_module = importlib.import_module("cara.logging.PythonLoggerAdapter")

_REPO_ROOT = Path(__file__).resolve().parents[2]


class _Rendered:
    """Counts how often it is formatted into a message."""

    def __init__(self) -> None:
        self.times = 0

    def __str__(self) -> str:
        self.times += 1
        return "rendered"


class _RecordingLog:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, dict]] = []

    def __getattr__(self, level: str):
        return lambda message, **kwargs: self.calls.append((level, message, kwargs))


@pytest.fixture
def quiet_logger(monkeypatch) -> Logger:
    monkeypatch.setattr(
        Logger, "_config", {"channels": {"console": {"LEVEL": "WARNING"}}}
    )
    return object.__new__(Logger)


def test_a_filtered_out_call_never_formats_its_arguments(quiet_logger):
    argument = _Rendered()

    quiet_logger.debug("payload=%s", argument)
    quiet_logger.info("payload=%s", argument)

    assert argument.times == 0


def test_a_filtered_out_category_never_formats_its_arguments(quiet_logger, monkeypatch):
    monkeypatch.setattr(
        CategoryFilter, "should_log", staticmethod(lambda level, category: False)
    )
    argument = _Rendered()

    quiet_logger.error("payload=%s", argument, category="db.queries")

    assert argument.times == 0


@pytest.fixture
def bridged(monkeypatch):
    recorder = _RecordingLog()
    monkeypatch.setattr(adapter_module, "Log", recorder)
    framework = logging.getLogger("cara")
    before = list(framework.handlers)
    adapter_module._bridge_framework_loggers()
    yield recorder
    framework.handlers[:] = before


def test_framework_warnings_reach_the_cara_logger(bridged):
    logging.getLogger("cara.cache").warning("lock %s had lapsed", "lock:export")

    (level, message, kwargs) = bridged.calls[0]
    assert (level, message) == ("warning", "lock lock:export had lapsed")
    assert kwargs["category"] == "cara.cache"


def test_framework_errors_keep_their_traceback_and_lose_their_secrets(bridged):
    try:
        raise RuntimeError("boom")
    except RuntimeError:
        logging.getLogger("cara.cache.redis").exception(
            "connect failed: redis://:hunter22@cache:6379/0"
        )

    (level, message, kwargs) = bridged.calls[0]
    assert level == "error"
    assert "hunter22" not in message and "[REDACTED]" in message
    assert kwargs["exc_info"][0] is RuntimeError


def test_the_bridge_is_installed_once_and_never_re_enters(bridged, monkeypatch):
    adapter_module._bridge_framework_loggers()
    adapter_module._bridge_framework_loggers()
    framework = logging.getLogger("cara")
    assert sum(getattr(h, "_cara_bridge", False) for h in framework.handlers) == 1

    class _LogsThroughStdlib:
        calls = 0

        def __getattr__(self, level):
            def log(message, **kwargs):
                type(self).calls += 1
                # A Cara logger that itself reports through ``cara.*``.
                logging.getLogger("cara.facade").error("structured logging failed")
                raise RuntimeError("logger is down")

            return log

    monkeypatch.setattr(adapter_module, "Log", _LogsThroughStdlib())

    logging.getLogger("cara.cache").warning("first")  # must not raise or recurse

    assert _LogsThroughStdlib.calls == 1


def test_the_facade_boot_fallback_is_not_bridged(bridged):
    logging.getLogger("cara.fallback").warning("printed by its own handler")

    assert bridged.calls == []


def test_interception_installs_the_bridge():
    # In a subprocess: ``install_cara_loggers`` rewires process-wide loggers.
    script = textwrap.dedent(
        """
        import logging
        import cara.logging.PythonLoggerAdapter as adapter

        class Recorder:
            def __getattr__(self, level):
                return lambda message, **kw: print(level, kw["category"], message)

        adapter.Log = Recorder()
        adapter.install_cara_loggers()
        logging.basicConfig(level=logging.WARNING, force=True)
        logging.getLogger().handlers.clear()
        logging.getLogger().addHandler(logging.NullHandler())
        logging.getLogger("cara.cache").warning("version stamp read failed for %s", "k")
        """
    )

    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        cwd=_REPO_ROOT,
        timeout=60,
        check=True,
    )

    assert result.stdout.strip() == "warning cara.cache version stamp read failed for k"
