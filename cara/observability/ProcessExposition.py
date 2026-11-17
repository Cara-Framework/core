"""Process-level gauges prepended to a ``/metrics`` exposition.

Every HTTP deployable that serves Prometheus from a controller prefixes the
registry output with the same five un-namespaced series (uptime, peak RSS,
GC collections, interpreter version, ``up``). They describe the process,
not the product, so they carry no product prefix and live here once.

They are plain text rather than registry collectors on purpose: the values
are read at scrape time and the series names are a dashboard contract
(``python_uptime_seconds`` …) that both products' Grafana boards query.
"""

from __future__ import annotations

import gc
import resource
import sys
import time


def _peak_rss_bytes() -> int:
    """``ru_maxrss`` is bytes on macOS and kibibytes on Linux."""
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return int(peak if sys.platform == "darwin" else peak * 1024)


def process_exposition(started_at: float, *, now: float | None = None) -> str:
    """Return the process gauges as Prometheus text, ending in a newline.

    ``started_at`` is the process (or controller) start as a Unix timestamp;
    ``now`` is injectable for tests. Uptime never goes negative.
    """
    current = time.time() if now is None else float(now)
    uptime = max(0.0, current - float(started_at))
    gc_collections = sum(int(item.get("collections", 0)) for item in gc.get_stats())
    version = (
        f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
    )
    return (
        "# HELP python_uptime_seconds Process uptime in seconds\n"
        "# TYPE python_uptime_seconds gauge\n"
        f"python_uptime_seconds {uptime}\n\n"
        # ru_maxrss is the PEAK resident set, not the current one; the series
        # name is kept for the dashboards that already query it.
        "# HELP python_memory_rss_bytes Peak resident set size in bytes\n"
        "# TYPE python_memory_rss_bytes gauge\n"
        f"python_memory_rss_bytes {_peak_rss_bytes()}\n\n"
        "# HELP python_gc_collections_total Total number of garbage collections\n"
        "# TYPE python_gc_collections_total counter\n"
        f"python_gc_collections_total {gc_collections}\n\n"
        "# HELP python_info Python interpreter info\n"
        "# TYPE python_info gauge\n"
        f'python_info{{version="{version}"}} 1\n\n'
        "# HELP up Service is up\n"
        "# TYPE up gauge\n"
        "up 1\n"
    )


__all__ = ["process_exposition"]
