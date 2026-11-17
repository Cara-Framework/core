"""Contract for the process gauges every /metrics controller prepends."""

from __future__ import annotations

import sys

from cara.observability import process_exposition


def test_process_metrics_exposition_names_and_types() -> None:
    text = process_exposition(100.0, now=142.5)

    assert "python_uptime_seconds 42.5\n" in text
    assert "# TYPE python_memory_rss_bytes gauge\n" in text
    assert "# TYPE python_gc_collections_total counter\n" in text
    version = (
        f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
    )
    assert f'python_info{{version="{version}"}} 1\n' in text
    assert text.endswith("up 1\n")


def test_process_metrics_uptime_never_negative() -> None:
    # A clock step backwards must not publish a negative gauge.
    assert "python_uptime_seconds 0.0\n" in process_exposition(200.0, now=100.0)


def test_process_metrics_rss_is_a_positive_integer() -> None:
    line = next(
        row
        for row in process_exposition(0.0, now=1.0).splitlines()
        if row.startswith("python_memory_rss_bytes ")
    )
    assert int(line.split(" ", 1)[1]) > 0
