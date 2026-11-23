"""Every stdlib logger of the process reaches the Cara pipeline, once, redacted.

The live defect: ``install_cara_loggers`` swapped only the table's library
loggers (``pika``, ``apscheduler``, ``urllib3``) to an adapter and set
``propagate = False`` on them, so their CHILD loggers
(``pika.adapters.blocking_connection``, ``urllib3.connectionpool``) found no
handler and fell through to ``logging.lastResort``: raw, unredacted lines on
stderr. ``LoggerProvider`` then ran ``basicConfig(force=True)`` and parked a
``NullHandler`` on the root, so every product logger (``app.*``,
``shared.*``, ``commons.*``) was discarded. There was no
``logging.python_levels`` override.

Each case runs in a subprocess: the bridge rewires process-wide loggers.
"""

from __future__ import annotations

import json
import subprocess
import sys
import textwrap
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]

_PRELUDE = textwrap.dedent(
    """
    import json
    import logging
    import cara.logging.PythonLoggerAdapter as adapter

    class Recorder:
        def __getattr__(self, level):
            def log(message, **kw):
                print(json.dumps([level, kw["category"], message]), flush=True)
            return log

    adapter.Log = Recorder()
    """
)


def _run(body: str) -> tuple[list[list[str]], str]:
    result = subprocess.run(
        [sys.executable, "-c", _PRELUDE + textwrap.dedent(body)],
        capture_output=True,
        text=True,
        cwd=_REPO_ROOT,
        timeout=60,
        check=True,
    )
    calls = [json.loads(line) for line in result.stdout.splitlines() if line.strip()]
    return calls, result.stderr


def test_a_third_party_child_logger_reaches_cara_redacted_and_not_stderr():
    calls, stderr = _run(
        """
        adapter.install_cara_loggers()
        logging.getLogger("pika.adapters.blocking_connection").warning(
            "connect failed: amqp://guest:hunter22@mq:5672/"
        )
        logging.getLogger("urllib3.connectionpool").warning(
            "retrying redis://:s3cretpw@cache:6379/0"
        )
        """
    )

    assert len(calls) == 2
    assert all(level == "warning" for level, _, _ in calls)
    assert all("hunter22" not in m and "s3cretpw" not in m for _, _, m in calls)
    assert all("[REDACTED]" in message for _, _, message in calls)
    assert "hunter22" not in stderr and "s3cretpw" not in stderr
    assert "connect failed" not in stderr


def test_an_unconfigured_library_is_admitted_from_warning_up():
    calls, _ = _run(
        """
        adapter.install_cara_loggers()
        chatty = logging.getLogger("somelib.worker")
        chatty.info("heartbeat")
        chatty.warning("slow")
        logging.getLogger("apscheduler.executors.default").info("job ran")
        """
    )

    assert [message for _, _, message in calls] == ["slow"]


def test_python_levels_overrides_a_library_level_per_logger():
    calls, _ = _run(
        """
        adapter.install_cara_loggers({"somelib": "DEBUG", "pika": "ERROR"})
        logging.getLogger("somelib.worker").info("admitted by override")
        logging.getLogger("pika.channel").warning("silenced by override")
        """
    )

    assert [message for _, _, message in calls] == ["admitted by override"]


def test_product_loggers_pass_at_their_own_level():
    calls, _ = _run(
        """
        adapter.install_cara_loggers()
        logging.getLogger("app.jobs.Sync").info("synced 3")
        logging.getLogger("shared.runtime").debug("lane picked")
        logging.getLogger("commons.models").info("hydrated")
        """
    )

    assert calls == [
        ["info", "app.jobs.Sync", "synced 3"],
        ["debug", "shared.runtime", "lane picked"],
        ["info", "commons.models", "hydrated"],
    ]


def test_a_record_is_delivered_once_and_a_log_while_logging_does_not_loop():
    calls, stderr = _run(
        """
        adapter.install_cara_loggers()
        adapter.install_cara_loggers()  # idempotent: still one bridge each
        logging.getLogger("cara.cache").warning("lapsed")

        class Reentrant:
            calls = 0
            def __getattr__(self, level):
                def log(message, **kw):
                    type(self).calls += 1
                    logging.getLogger("pika.channel").error("while logging")
                return log

        adapter.Log = Reentrant()
        logging.getLogger("somelib").error("first")
        print(json.dumps(["count", "", str(Reentrant.calls)]))
        """
    )

    assert calls == [["warning", "cara.cache", "lapsed"], ["count", "", "1"]]
    assert "while logging" not in stderr


def test_the_provider_leaves_the_bridge_on_the_root_and_no_null_handler():
    calls, _ = _run(
        """
        import importlib
        provider_module = importlib.import_module("cara.logging.LoggerProvider")
        provider_module.config = lambda key, default=None: (
            {"app": "WARNING"} if key == "logging.python_levels" else default
        )
        provider = object.__new__(provider_module.LoggerProvider)
        provider._configure_logging_interception()
        root = logging.getLogger()
        assert not any(isinstance(h, logging.NullHandler) for h in root.handlers)
        assert any(getattr(h, "_cara_bridge", False) for h in root.handlers)
        logging.getLogger("app.jobs").info("below the configured level")
        logging.getLogger("app.jobs").warning("product warning")
        """
    )

    assert calls == [["warning", "app.jobs", "product warning"]]
