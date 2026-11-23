"""A consumer slot thread outlives anything one consume cycle throws.

Nothing respawns a slot: when its thread ends, the worker stays alive, its
readiness gauge drops to 0 and nothing consumes. Two ways it used to end:

  * ``mgr.close()`` raising a pika error in the loop's ``finally`` (pika's
    ``AMQPError`` family is neither ``OSError`` nor ``RuntimeError``, so the
    manager's close did not absorb it) — closing a half-dead connection ended
    the slot during exactly the outage it should ride out;
  * a ``BaseException`` out of the stream (a job's ``SystemExit`` coming back
    through ``future.result()``), which ``except Exception`` let through.

The broker is a fake; the reconnect backoff is the real one-second minimum.
"""

from __future__ import annotations

import importlib
import threading
import time
from types import SimpleNamespace

import pika.exceptions
import pytest

from cara.commands.core.QueueWorkCommand import QueueWorkCommand

_GIVE_UP_AFTER_SECONDS = 15.0


def _run(monkeypatch, *, stream_error: BaseException, close_error: Exception | None):
    """Run the worker until a second subscription (or the give-up deadline)."""
    runtime = importlib.import_module("cara.commands.core._QueueWorkRuntime")
    closes: list[str] = []

    def _close() -> None:
        closes.append(threading.current_thread().name)
        if close_error is not None and len(closes) == 1:
            raise close_error

    monkeypatch.setattr(
        runtime,
        "AMQPConnectionManager",
        lambda _config, _driver: SimpleNamespace(close=_close),
    )
    monkeypatch.setattr(
        runtime,
        "Queue",
        SimpleNamespace(
            driver=lambda *_a, **_k: SimpleNamespace(
                _connection_parameters=lambda _options: None,
                verify_runtime_health=lambda *_a, **_k: None,
            )
        ),
    )
    monkeypatch.setattr(
        runtime,
        "Log",
        SimpleNamespace(
            **{
                level: lambda *_a, **_k: None
                for level in ("debug", "info", "warning", "error")
            }
        ),
    )
    thread_deaths: list[BaseException] = []
    monkeypatch.setattr(
        threading, "excepthook", lambda args: thread_deaths.append(args.exc_value)
    )

    command = QueueWorkCommand.__new__(QueueWorkCommand)
    command.console = SimpleNamespace(print=lambda *_a, **_k: None)
    command.shutdown_requested = False
    command.jobs_processed = command.jobs_failed = 0
    command._concurrency = 1
    command._shutdown_signal = None
    command._shutdown_drain_seconds = lambda: 1.0
    command._shutdown_cancel_seconds = lambda: 1.0
    command._shutdown_worker_resources = lambda: True
    command._check_memory_usage = lambda: True
    # Only a safety net: with the defect the slot is gone and nothing else
    # would ever end the run.
    deadline = time.monotonic() + _GIVE_UP_AFTER_SECONDS
    command._should_stop = lambda _config: time.monotonic() > deadline

    streams: list[str] = []

    def _stream(**_kwargs) -> None:
        streams.append(threading.current_thread().name)
        if len(streams) == 1:
            raise stream_error
        command.shutdown_requested = True

    command._consume_queue_stream = _stream
    command._run_worker({"queue_names": ["sync"], "driver_name": "amqp", "timeout": 1})
    return streams, closes, thread_deaths


@pytest.mark.parametrize(
    "close_error",
    [
        pika.exceptions.StreamLostError("Transport indicated EOF"),
        pika.exceptions.ConnectionWrongStateError(),
    ],
    ids=["stream-lost", "wrong-state"],
)
def test_a_pika_error_from_close_does_not_end_the_slot(monkeypatch, close_error) -> None:
    streams, closes, deaths = _run(
        monkeypatch,
        stream_error=RuntimeError("settlement outage"),
        close_error=close_error,
    )

    # The same slot thread came back for a second subscription.
    assert streams == ["queue-worker-1", "queue-worker-1"]
    assert len(closes) == 2
    assert deaths == []


def test_a_system_exit_out_of_the_stream_does_not_end_the_slot(monkeypatch) -> None:
    streams, closes, deaths = _run(
        monkeypatch, stream_error=SystemExit(3), close_error=None
    )

    assert streams == ["queue-worker-1", "queue-worker-1"]
    assert len(closes) == 2
    assert deaths == []
