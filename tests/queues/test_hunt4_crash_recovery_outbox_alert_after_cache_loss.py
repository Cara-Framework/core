"""An outbox incident must still clear after the cache that remembered it is gone.

``_announce_recovery`` resolves only what ``Cache.get(active_key)`` says is open.
A Redis restart or flush while a stall page stands loses that key, so the end of
the stall is judged "quiet" and the resolve never fires: the incident stays open
on the pager until someone closes it. Found 2026-11-09 by reading the path while
killing a private stack's Redis mid-load; the stall in that run kept going, so
it re-opened.
"""

from __future__ import annotations

from typing import Any

import pytest

from cara.queues.delivery import QueueOutboxHealth
from cara.testing.fakes import CacheFake

module = __import__(
    "cara.queues.delivery.QueueOutboxHealth", fromlist=["QueueOutboxHealth"]
)


class _Sink:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def fire(self, **kwargs: Any) -> bool:
        self.calls.append(kwargs)
        return True


@pytest.fixture
def wired(monkeypatch):
    cache = CacheFake()
    sink = _Sink()
    monkeypatch.setattr(module, "Cache", cache)
    monkeypatch.setattr(module, "AlertSink", sink)
    monkeypatch.setattr(module, "config", lambda key, default=None: default)
    for level in ("error", "info", "warning"):
        monkeypatch.setattr(module.Log, level, lambda *a, **k: None)
    return cache, sink


def _snapshot() -> dict[str, float]:
    return {
        "due_pending": 12.0,
        "oldest_due_age": 900.0,
        "last_publish_age": 900.0,
        "hook_due_pending": 0.0,
        "hook_oldest_due_age": 0.0,
    }


def test_a_stall_page_is_resolved_after_the_cache_was_wiped_under_it(wired):
    cache, sink = wired
    assert QueueOutboxHealth.announce(_snapshot(), stalled=True) == "fired"

    cache.flush()

    assert QueueOutboxHealth.announce(_snapshot(), stalled=False) == "resolved"
    assert [call["severity"] for call in sink.calls] == ["critical", "resolved"]
