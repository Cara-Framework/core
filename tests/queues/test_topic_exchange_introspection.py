"""Regression pins for TopicExchange introspection.

``get_queue_info`` / ``list_queues_for_domain`` used to read a
``bindings`` dict that nothing ever populated, so both always came back
empty. They now derive from ``queue_bindings`` — the single source of
truth ``bind_queue`` maintains.
"""

from __future__ import annotations

from unittest.mock import patch

from cara.queues.exchanges import TopicExchange

_BINDINGS = [
    ("jobs.default", "jobs.*.default"),
    ("jobs.high", "jobs.*.high"),
    ("mail.default", "mail.send.default"),
]


def _fake_config(key, default=None):
    values = {
        "queue.topic_exchange_name": "test.exchange",
        "queue.topic_exchange_bindings": _BINDINGS,
    }
    return values.get(key, default)


def _make_exchange(name: str) -> TopicExchange:
    # Singleton-per-name: use distinct names per test and drop the cache
    # entry so a re-run starts clean.
    TopicExchange._instances.pop(name, None)
    with patch("cara.configuration.config", side_effect=_fake_config):
        return TopicExchange(name)


class TestQueueInfo:
    def test_get_queue_info_reflects_bound_queues(self):
        exchange = _make_exchange("test.introspection.info")

        info = exchange.get_queue_info()

        assert set(info) == {"jobs.default", "jobs.high", "mail.default"}
        assert info["jobs.high"]["routing_pattern"] == "jobs.*.high"
        assert info["jobs.high"]["domain"] == "jobs"
        assert info["jobs.high"]["priority"] == "high"
        assert info["jobs.high"]["exchange"] == "test.introspection.info"

    def test_queue_with_multiple_patterns_lists_them_all(self):
        exchange = _make_exchange("test.introspection.multi")
        exchange.bind_queue("jobs.default", "reports.*.default")

        info = exchange.get_queue_info()

        assert info["jobs.default"]["routing_patterns"] == [
            "jobs.*.default",
            "reports.*.default",
        ]


class TestDomainListing:
    def test_list_queues_for_domain_matches_domain_segment(self):
        exchange = _make_exchange("test.introspection.domain")

        assert sorted(exchange.list_queues_for_domain("jobs")) == [
            "jobs.default",
            "jobs.high",
        ]
        assert exchange.list_queues_for_domain("mail") == ["mail.default"]
        assert exchange.list_queues_for_domain("unknown") == []

    def test_wildcard_domain_pattern_serves_every_domain(self):
        exchange = _make_exchange("test.introspection.wildcard")
        exchange.bind_queue("catchall", "*.audit.low")

        assert "catchall" in exchange.list_queues_for_domain("jobs")
        assert "catchall" in exchange.list_queues_for_domain("anything")
