"""A cache failure in ``KeyCooldown`` is logged, not swallowed.

Live defect in ``cara/queues/middleware/KeyCooldown.py``: ``clear_cooldown``
caught every cache error and returned silently, so an operator who cleared
a cooldown believed it gone while it stayed in force until its TTL; and
``cooldown_remaining`` failed open without a trace. A failed clear now logs
a WARNING; an unreadable cooldown logs at DEBUG (it runs on every job of a
keyed lane) and still fails open (0.0, the job runs).
"""

from __future__ import annotations

import importlib
import logging

import pytest

from cara.queues.middleware import clear_cooldown, cooldown_remaining

module = importlib.import_module("cara.queues.middleware.KeyCooldown")


class _DownCache:
    def get(self, _key):
        raise ConnectionError("redis down")

    def forget(self, _key):
        raise ConnectionError("redis down")


@pytest.fixture
def down_cache(monkeypatch):
    monkeypatch.setattr(module, "_cache", lambda: _DownCache())


def test_a_failed_clear_logs_a_warning(down_cache, caplog) -> None:
    with caplog.at_level(logging.DEBUG, logger="cara.queue.middleware"):
        clear_cooldown("retailer:a")

    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert "not cleared" in warnings[0].getMessage()
    assert "retailer:a" in warnings[0].getMessage()
    assert "ConnectionError" in warnings[0].getMessage()


def test_an_unreadable_cooldown_logs_at_debug_and_fails_open(down_cache, caplog) -> None:
    with caplog.at_level(logging.DEBUG, logger="cara.queue.middleware"):
        assert cooldown_remaining("retailer:a") == 0.0

    records = [r for r in caplog.records if r.name == "cara.queue.middleware"]
    assert [r.levelno for r in records] == [logging.DEBUG]
    assert "unreadable" in records[0].getMessage()
