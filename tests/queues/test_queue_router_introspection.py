"""Regression pins for process-local queue routing introspection.

``get_queue_info`` / ``list_queues_for_domain`` used to read a
``bindings`` dict that nothing ever populated, so both always came back
empty. They now derive from ``queue_bindings`` — the single source of
truth ``bind_queue`` maintains.
"""

from __future__ import annotations

from unittest.mock import patch

from cara.queues.routing import QueueRouter

_BINDINGS = [
    ("jobs.default", "jobs.*.default"),
    ("jobs.high", "jobs.*.high"),
    ("mail.default", "mail.send.default"),
]


def _fake_config(key, default=None):
    values = {
        "queue.queue_routing_rules": _BINDINGS,
    }
    return values.get(key, default)


def _make_router() -> QueueRouter:
    QueueRouter._instance = None
    with patch("cara.configuration.config", side_effect=_fake_config):
        return QueueRouter()


class TestQueueInfo:
    def test_get_queue_info_reflects_bound_queues(self):
        router = _make_router()

        info = router.get_queue_info()

        assert set(info) == {"jobs.default", "jobs.high", "mail.default"}
        assert info["jobs.high"]["routing_pattern"] == "jobs.*.high"
        assert info["jobs.high"]["domain"] == "jobs"
        assert info["jobs.high"]["priority"] == "high"

    def test_queue_with_multiple_patterns_lists_them_all(self):
        router = _make_router()
        router.bind_queue("jobs.default", "reports.*.default")

        info = router.get_queue_info()

        assert info["jobs.default"]["routing_patterns"] == [
            "jobs.*.default",
            "reports.*.default",
        ]


class TestDomainListing:
    def test_list_queues_for_domain_matches_domain_segment(self):
        router = _make_router()

        assert sorted(router.list_queues_for_domain("jobs")) == [
            "jobs.default",
            "jobs.high",
        ]
        assert router.list_queues_for_domain("mail") == ["mail.default"]
        assert router.list_queues_for_domain("unknown") == []

    def test_wildcard_domain_pattern_serves_every_domain(self):
        router = _make_router()
        router.bind_queue("catchall", "*.audit.low")

        assert "catchall" in router.list_queues_for_domain("jobs")
        assert "catchall" in router.list_queues_for_domain("anything")
