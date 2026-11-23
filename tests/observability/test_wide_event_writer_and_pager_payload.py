"""Telemetry that one awkward value used to switch off.

* A wide event with a non-string key (or a cycle) raised out of the writer
  thread's loop: the thread died and every later event was dropped.
* A ``Decimal`` / ``datetime`` in an alert's ``context`` raised while building
  the PagerDuty body, so the page reached Slack only.
"""

from __future__ import annotations

import importlib
import json
import queue
import threading
from datetime import UTC, datetime
from decimal import Decimal

sink_module = importlib.import_module("cara.observability.AlertSink")
wide_module = importlib.import_module("cara.observability.WideEvent")


class _Drained(Exception):
    """Raised by the test queue once its events are consumed."""


class _DrainedQueue(queue.Queue):
    def get(self, block=True, timeout=None):
        try:
            return super().get(block=False)
        except queue.Empty:
            raise _Drained from None


def _run_writer(monkeypatch, events, post):
    """Run the writer loop over ``events``; return how it ended."""
    settings = {"wide_events.batch_rows": "1", "wide_events.batch_secs": "60"}
    monkeypatch.setattr(
        wide_module, "_env", lambda key, default="": settings.get(key, default)
    )
    monkeypatch.setattr(wide_module.requests, "post", post)
    drained = _DrainedQueue()
    for event in events:
        drained.put(event)
    monkeypatch.setattr(wide_module, "_QUEUE", drained)
    ended_by: list[BaseException] = []

    def loop() -> None:
        try:
            wide_module._run()
        except BaseException as exc:
            ended_by.append(exc)

    writer = threading.Thread(target=loop)
    writer.start()
    writer.join(timeout=5)

    assert not writer.is_alive()
    return ended_by[0]


def test_an_unencodable_event_does_not_stop_the_writer(monkeypatch):
    posted: list[bytes] = []
    cyclic: dict = {"job": "b"}
    cyclic["self"] = cyclic

    ended_by = _run_writer(
        monkeypatch,
        [
            {"job": "a"},
            {("tuple", "key"): 1},
            cyclic,
            {"job": "c", "at": datetime(2026, 1, 1, tzinfo=UTC)},
        ],
        lambda url, **kwargs: posted.append(kwargs["data"]),
    )

    # The loop ran until the queue was empty, not until the bad event.
    assert isinstance(ended_by, _Drained)
    assert [json.loads(body)["job"] for body in posted] == ["a", "c"]


def test_a_failing_post_does_not_stop_the_writer(monkeypatch):
    posted: list[bytes] = []

    def flaky_post(url, **kwargs):
        posted.append(kwargs["data"])
        if len(posted) == 1:
            raise TypeError("a client bug, not an OSError")

    ended_by = _run_writer(monkeypatch, [{"job": "a"}, {"job": "b"}], flaky_post)

    assert isinstance(ended_by, _Drained)
    assert [json.loads(body)["job"] for body in posted] == ["a", "b"]


def test_pagerduty_receives_a_page_whose_context_is_not_plain_json(monkeypatch):
    settings = {"notifications.alerts_pagerduty_routing_key": "rk"}
    monkeypatch.setattr(
        sink_module, "config", lambda key, default=None: settings.get(key, default)
    )
    sent: list[dict] = []

    class _Accepted:
        status = 202

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    def fake_urlopen(request, timeout=None):
        sent.append(json.loads(request.data))
        return _Accepted()

    monkeypatch.setattr(sink_module, "urlopen", fake_urlopen)

    delivered = sink_module.AlertSink.fire(
        severity="critical",
        title="Error rate >40%",
        dedup_key="error_rate",
        context={
            "failure_rate": Decimal("0.41"),
            "since": datetime(2026, 1, 1, tzinfo=UTC),
        },
    )

    assert delivered is True
    details = sent[0]["payload"]["custom_details"]
    assert details["failure_rate"] == "0.41"
    assert details["since"].startswith("2026-01-01")
