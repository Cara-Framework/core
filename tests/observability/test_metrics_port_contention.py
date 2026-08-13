"""Metrics must never be the reason a process refuses to run.

One ``METRICS_PORT`` is shared by several long-lived roles (queue worker,
scheduler, queue relay). When ``start_http_server`` raised on a permanently
held port, the queue RELAY could not start at all — and the relay is the only
thing that moves durable outbox rows onto AMQP. The result was silent and
total: 1250 jobs accumulated unpublished while every dashboard button
cheerfully reported "started". A metrics endpoint is worth strictly less than
the work it was supposed to observe.

A brief EADDRINUSE is still retried (a restarting predecessor holds the socket
for a few seconds). What changed is the verdict after the retries run out:
warn and continue, not die.
"""

from __future__ import annotations

import errno
import time
from importlib import import_module

import pytest

Metrics = import_module("cara.observability.MetricsBase")
RuntimeMetrics = import_module("cara.observability._RuntimeMetrics")


@pytest.fixture(autouse=True)
def _reset_server_state(monkeypatch):
    monkeypatch.setattr(RuntimeMetrics, "_http_server_started", False)
    # Retry backoff is real seconds in production; nothing here waits for it.
    # ``Metrics`` imports ``time`` inside the function, so patching the module
    # attribute is what the retry loop actually reads.
    monkeypatch.setattr(time, "sleep", lambda _s: None)


def _always_in_use(*_args, **_kwargs):
    raise OSError(errno.EADDRINUSE, "Address already in use")


def test_permanently_held_port_warns_and_continues_after_bounded_retries(monkeypatch):
    monkeypatch.setattr(RuntimeMetrics, "_prom_start_http_server", _always_in_use)

    # The docstring's whole point: after the restart-race retries run out,
    # the process runs WITHOUT /metrics instead of refusing to boot.
    assert (
        Metrics.start_http_server(
            port=9400,
            service="test-services",
            role="queue-relay",
        )
        is None
    )
    assert RuntimeMetrics._http_server_started is False


def test_transient_contention_still_wins_the_port(monkeypatch):
    attempts = {"n": 0}

    def flaky(port, addr=None, registry=None):
        attempts["n"] += 1
        if attempts["n"] < 3:
            raise OSError(errno.EADDRINUSE, "Address already in use")

    monkeypatch.setattr(RuntimeMetrics, "_prom_start_http_server", flaky)

    assert (
        Metrics.start_http_server(
            port=9400,
            service="test-services",
            role="queue-worker",
        )
        == 9400
    )
    assert attempts["n"] == 3


def test_non_contention_oserror_is_not_swallowed(monkeypatch):
    def refused(*_args, **_kwargs):
        raise OSError(errno.EACCES, "Permission denied")

    monkeypatch.setattr(RuntimeMetrics, "_prom_start_http_server", refused)

    # A misconfigured port is an operator error worth surfacing loudly —
    # only contention gets the soft landing.
    with pytest.raises(OSError):
        Metrics.start_http_server(
            port=80,
            service="test-services",
            role="queue-relay",
        )
