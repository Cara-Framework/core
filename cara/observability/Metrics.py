"""Shared Prometheus metrics infrastructure for cheapa.io apps.

Provides a process-local registry, histogram bucket presets, label helpers,
safe increment/observe wrappers, and :class:`MetricsBase` with metrics common
to both the API and services worker. Each app extends ``MetricsBase`` in its
own ``app/support/Metrics.py`` with app-specific counters/gauges/histograms.

Cardinality budget (keep low to protect Prometheus scrape cost):
- ``route``: controller@action form (bounded by codebase — dozens)
- ``status_class``: "2xx"/"3xx"/"4xx"/"5xx"/"error" — always 5 values
- ``queue``: literal queue name — ~30 queues project-wide
- ``job``: class name — dozens
- ``driver``: "scrapedo_fetch"/"scrapedo_amazon_pdp"/…
- ``geo``: 2-letter country — single digits in prod
- ``provider``: "ollama"/"openrouter"/"openai" — 3
- ``model``: literal model string — single digits
- ``task``: task-profile name — single digits
- ``finish_reason``: "stop"/"length"/"error"/… — ~5
- ``event``: event class name — dozens
"""

from __future__ import annotations

import re
import threading
from typing import Any

from prometheus_client import (
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)
from prometheus_client import (
    start_http_server as _prom_start_http_server,
)
from prometheus_client.exposition import CONTENT_TYPE_LATEST

from cara.configuration import config

# Process-local registry. We don't use the default REGISTRY so tests
# can build their own without colliding, and so the worker's side-thread
# HTTP server (see ``start_http_server`` below) can use the exact same
# registry the workload code writes into.
REGISTRY: CollectorRegistry = CollectorRegistry(auto_describe=True)


def _existing_collector(
    name: str, registry: CollectorRegistry = REGISTRY
) -> Counter | Gauge | Histogram | None:
    """Return a collector already registered on *registry*, if any."""
    collectors = getattr(registry, "_names_to_collectors", None)
    if not collectors:
        return None
    return collectors.get(name)


def counter(
    name: str,
    documentation: str,
    labelnames: tuple[str, ...] | list[str] = (),
    registry: CollectorRegistry = REGISTRY,
    **kwargs: Any,
) -> Counter:
    """Create a Counter, or return the existing one after hot reload."""
    existing = _existing_collector(name, registry)
    if existing is not None:
        return existing  # type: ignore[return-value]
    try:
        return Counter(
            name,
            documentation,
            labelnames=labelnames,
            registry=registry,
            **kwargs,
        )
    except ValueError:
        existing = _existing_collector(name, registry)
        if existing is not None:
            return existing  # type: ignore[return-value]
        raise


def gauge(
    name: str,
    documentation: str,
    labelnames: tuple[str, ...] | list[str] = (),
    registry: CollectorRegistry = REGISTRY,
    **kwargs: Any,
) -> Gauge:
    """Create a Gauge, or return the existing one after hot reload."""
    existing = _existing_collector(name, registry)
    if existing is not None:
        return existing  # type: ignore[return-value]
    try:
        return Gauge(
            name,
            documentation,
            labelnames=labelnames,
            registry=registry,
            **kwargs,
        )
    except ValueError:
        existing = _existing_collector(name, registry)
        if existing is not None:
            return existing  # type: ignore[return-value]
        raise


def histogram(
    name: str,
    documentation: str,
    labelnames: tuple[str, ...] | list[str] = (),
    buckets: tuple | None = None,
    registry: CollectorRegistry = REGISTRY,
    **kwargs: Any,
) -> Histogram:
    """Create a Histogram, or return the existing one after hot reload."""
    existing = _existing_collector(name, registry)
    if existing is not None:
        return existing  # type: ignore[return-value]
    if buckets is None:
        buckets = histogram_buckets_long()
    try:
        return Histogram(
            name,
            documentation,
            labelnames=labelnames,
            buckets=buckets,
            registry=registry,
            **kwargs,
        )
    except ValueError:
        existing = _existing_collector(name, registry)
        if existing is not None:
            return existing  # type: ignore[return-value]
        raise


def histogram_buckets_short() -> tuple:
    """Latency buckets tuned for sub-second hot paths (HTTP, parse)."""
    return (
        0.005,
        0.01,
        0.025,
        0.05,
        0.1,
        0.25,
        0.5,
        1.0,
        2.5,
        5.0,
        10.0,
    )


def histogram_buckets_long() -> tuple:
    """Latency buckets tuned for multi-second jobs (scrape, AI, queue)."""
    return (
        0.1,
        0.25,
        0.5,
        1.0,
        2.5,
        5.0,
        10.0,
        30.0,
        60.0,
        120.0,
        300.0,
        600.0,
    )


_UUID_RE = re.compile(
    r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"
)


def normalize_metric_path(path: str) -> str:
    """Replace dynamic URL segments with placeholders to bound label cardinality.

    ``/api/products/123`` → ``/api/products/{id}``
    ``/api/products/01HABC...`` → ``/api/products/{ulid}``
    ``/api/products/xxxxxxxx-xxxx-…`` → ``/api/products/{uuid}``
    """
    segments = path.split("/")
    out: list[str] = []
    for seg in segments:
        if not seg:
            out.append(seg)
        elif seg.isdigit():
            out.append("{id}")
        elif _UUID_RE.fullmatch(seg):
            out.append("{uuid}")
        elif len(seg) == 26 and seg.isalnum():
            out.append("{ulid}")
        else:
            out.append(seg)
    return "/".join(out)


class MetricsBase:
    """Namespace holder for shared counters/histograms/gauges.

    Deliberately NOT a dataclass or Pydantic model — these are class-level
    singletons and should be accessed by attribute lookup only. App-specific
    metrics live on subclasses in ``api/app/support/Metrics.py`` and
    ``services/app/support/Metrics.py``.
    """

    @staticmethod
    def safe_inc(metric, labels: dict, amount: float = 1) -> None:
        """Increment a counter without raising on failure."""
        try:
            metric.labels(**labels).inc(amount)
        except Exception:
            # Intentional: metrics emission must never crash production.
            return

    @staticmethod
    def safe_observe(metric, labels: dict, value: float) -> None:
        """Observe a histogram value without raising on failure."""
        try:
            metric.labels(**labels).observe(value)
        except Exception:
            # Intentional: see safe_inc above.
            return

    # ─── Service-level info (static label) ──────────────────────────────
    build_info = Gauge(
        "cheapa_build_info",
        "Static build/version info. Always 1 while the process is alive.",
        labelnames=("service", "role"),
        registry=REGISTRY,
    )

    # ─── HTTP API ───────────────────────────────────────────────────────
    http_requests_total = Counter(
        "cheapa_http_requests_total",
        "Total HTTP requests served by the API.",
        labelnames=("method", "route", "status_class"),
        registry=REGISTRY,
    )
    http_request_duration_seconds = Histogram(
        "cheapa_http_request_duration_seconds",
        "HTTP request duration in seconds.",
        labelnames=("method", "route"),
        buckets=histogram_buckets_short(),
        registry=REGISTRY,
    )
    http_requests_in_flight = Gauge(
        "cheapa_http_requests_in_flight",
        "HTTP requests currently being handled.",
        registry=REGISTRY,
    )

    # ─── Queue worker ───────────────────────────────────────────────────
    queue_dispatches_total = Counter(
        "cheapa_queue_dispatches_total",
        "Jobs dispatched onto the queue (counted at Bus.dispatch call-site).",
        labelnames=("queue", "job"),
        registry=REGISTRY,
    )
    queue_jobs_consumed_total = Counter(
        "cheapa_queue_jobs_consumed_total",
        "Jobs consumed by workers, tagged by outcome.",
        labelnames=("queue", "job", "outcome"),  # success|failed|timeout|orphan
        registry=REGISTRY,
    )
    queue_job_duration_seconds = Histogram(
        "cheapa_queue_job_duration_seconds",
        "Wall-clock time a worker spent executing a job.",
        labelnames=("queue", "job"),
        buckets=histogram_buckets_long(),
        registry=REGISTRY,
    )
    queue_jobs_in_flight = Gauge(
        "cheapa_queue_jobs_in_flight",
        "Jobs currently being executed by this worker process.",
        labelnames=("queue", "job"),
        registry=REGISTRY,
    )

    # ─── Scrape drivers ─────────────────────────────────────────────────
    scrape_requests_total = Counter(
        "cheapa_scrape_requests_total",
        "Scrape requests issued, broken down by driver + outcome class.",
        labelnames=("driver", "geo", "status_class", "render", "super"),
        registry=REGISTRY,
    )
    scrape_request_duration_seconds = Histogram(
        "cheapa_scrape_request_duration_seconds",
        "End-to-end scrape request duration (client-observed).",
        labelnames=("driver", "geo"),
        buckets=histogram_buckets_long(),
        registry=REGISTRY,
    )
    scrape_cost_credits_total = Counter(
        "cheapa_scrape_cost_credits_total",
        "Sum of scrape.do-request-cost header values, in credits.",
        labelnames=("driver", "geo", "render", "super"),
        registry=REGISTRY,
    )
    scrape_remaining_credits = Gauge(
        "cheapa_scrape_remaining_credits",
        "Most recent scrape.do-remaining-credits reading per driver.",
        labelnames=("driver",),
        registry=REGISTRY,
    )
    scrape_bytes_total = Counter(
        "cheapa_scrape_bytes_total",
        "Total bytes pulled through scrape drivers.",
        labelnames=("driver", "geo"),
        registry=REGISTRY,
    )

    # ─── AI client ──────────────────────────────────────────────────────
    ai_requests_total = Counter(
        "cheapa_ai_requests_total",
        "AI chat completions, broken down by provider/model/task/finish_reason.",
        labelnames=("provider", "model", "task", "finish_reason"),
        registry=REGISTRY,
    )
    ai_request_duration_seconds = Histogram(
        "cheapa_ai_request_duration_seconds",
        "AI request wall-clock duration.",
        labelnames=("provider", "model", "task"),
        buckets=histogram_buckets_long(),
        registry=REGISTRY,
    )
    ai_tokens_total = Counter(
        "cheapa_ai_tokens_total",
        "AI token usage, by direction (in/out).",
        labelnames=("provider", "model", "task", "direction"),
        registry=REGISTRY,
    )
    ai_errors_total = Counter(
        "cheapa_ai_errors_total",
        "AI call failures grouped by provider/model/task and error kind.",
        labelnames=("provider", "model", "task", "kind"),
        registry=REGISTRY,
    )

    # ─── Pipeline events ────────────────────────────────────────────────
    pipeline_events_total = Counter(
        "cheapa_pipeline_events_total",
        "Product lifecycle events fired, by event class.",
        labelnames=("marketplace", "event"),
        registry=REGISTRY,
    )
    pipeline_stage_duration_seconds = Histogram(
        "cheapa_pipeline_stage_duration_seconds",
        "Wall-clock time between consecutive pipeline stages for a product.",
        labelnames=("marketplace", "from_stage", "to_stage"),
        buckets=histogram_buckets_long(),
        registry=REGISTRY,
    )

    # ─── Browser pool ───────────────────────────────────────────────────
    browser_pool_size = Gauge(
        "cheapa_browser_pool_size",
        "Browser pool occupancy by state (idle/busy/total).",
        labelnames=("state",),
        registry=REGISTRY,
    )
    browser_sessions_total = Counter(
        "cheapa_browser_sessions_total",
        "Browser acquire/release/restart events.",
        labelnames=("outcome",),
        registry=REGISTRY,
    )

    # ─── Cache ─────────────────────────────────────────────────────────
    cache_operations_total = Counter(
        "cheapa_cache_operations_total",
        "Cara Cache facade operations — rolled up by scope + outcome.",
        labelnames=("scope", "operation", "outcome"),
        registry=REGISTRY,
    )


def init_build_info(metrics_cls: type = MetricsBase) -> None:
    """Set the static build-info gauge once at import time."""
    service = config("metrics.service", "cheapa-services")
    role = config("metrics.role", "unknown")
    metrics_cls.build_info.labels(service=service, role=role).set(1)


def status_class(code: int | None) -> str:
    """Bucket an HTTP/upstream status code into 2xx/3xx/4xx/5xx/error."""
    try:
        c = int(code) if code is not None else 0
    except (TypeError, ValueError):
        return "error"
    if 200 <= c < 300:
        return "2xx"
    if 300 <= c < 400:
        return "3xx"
    if 400 <= c < 500:
        return "4xx"
    if 500 <= c < 600:
        return "5xx"
    return "error"


def bool_label(flag: object) -> str:
    """Serialize a bool-ish flag as "on"/"off" for labels."""
    return "on" if bool(flag) else "off"


def render() -> tuple[bytes, str]:
    """Produce the Prometheus text payload + content-type."""
    return generate_latest(REGISTRY), CONTENT_TYPE_LATEST


_http_server_started = False
_http_server_lock = threading.Lock()


def start_http_server(port: int | None = None, host: str = "0.0.0.0") -> int | None:
    """Stand up a ``/metrics`` HTTP server on ``port`` (default: $METRICS_PORT or 9101).

    Safe to call multiple times — subsequent calls after the first are no-ops.
    """
    global _http_server_started
    with _http_server_lock:
        if _http_server_started:
            return None
        effective_port = int(port if port is not None else config("metrics.port", 9101))
        if effective_port <= 0:
            return None
        _prom_start_http_server(effective_port, addr=host, registry=REGISTRY)
        _http_server_started = True
        return effective_port
