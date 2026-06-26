"""Shared Prometheus metrics infrastructure for Cara apps.

Provides a process-local registry, histogram bucket presets, label helpers,
safe increment/observe wrappers, and :class:`MetricsBase` carrying the metrics
the *framework itself* emits (HTTP, queue/worker, event listeners, CLI
commands, scheduled tasks, cache, health probes). Each app extends
``MetricsBase`` in its own ``app/support/Metrics.py`` with app-specific
domain counters/gauges/histograms.

Metric namespace
----------------
Framework metric names are prefixed with a configurable namespace so the
framework carries no consumer branding. Set ``CARA_METRICS_NAMESPACE`` in the
app environment; it defaults to ``app`` when unset. App subclasses are free
to name their own domain metrics with any literal prefix.

Cardinality budget (keep low to protect Prometheus scrape cost):
- ``route``: controller@action form (bounded by codebase — dozens)
- ``status_class``: "2xx"/"3xx"/"4xx"/"5xx"/"error" — always 5 values
- ``queue``: literal queue name — dozens project-wide
- ``job``: class name — dozens
- ``task``: task-profile name — single digits
- ``listener``: listener class name — dozens
- ``command``: CLI command name — dozens
- ``event``: event class name — dozens
"""

from __future__ import annotations

import os
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


def _metrics_namespace() -> str:
    """Resolve the framework metric-name prefix.

    Read from the ``CARA_METRICS_NAMESPACE`` env var (always available at
    import time, before config files load), falling back to ``app``. The
    framework deliberately carries no consumer-specific default — apps
    set ``CARA_METRICS_NAMESPACE`` in their ``.env``.
    """
    raw = os.environ.get("CARA_METRICS_NAMESPACE") or "app"
    cleaned = raw.strip().strip("_")
    # Prometheus names must match [a-zA-Z_][a-zA-Z0-9_]* — keep it sane.
    return cleaned if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", cleaned) else "app"


_NS = _metrics_namespace()


def metric_name(suffix: str) -> str:
    """Build a namespaced framework metric name (``{namespace}_{suffix}``)."""
    return f"{_NS}_{suffix}"


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
    """Latency buckets tuned for multi-second jobs (background work, queue)."""
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

    ``/api/items/123`` → ``/api/items/{id}``
    ``/api/items/01HABC...`` → ``/api/items/{ulid}``
    ``/api/items/xxxxxxxx-xxxx-…`` → ``/api/items/{uuid}``
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
    """Namespace holder for the metrics the framework itself emits.

    Deliberately NOT a dataclass or Pydantic model — these are class-level
    singletons and should be accessed by attribute lookup only. App-specific
    DOMAIN metrics live on subclasses in each consuming app's
    ``app/support/Metrics.py``.

    Everything defined here is a *framework mechanism* metric (HTTP, queue,
    events, commands, scheduler, cache, health) emitted by Cara core, so it
    is owned by the framework and shared by every app via inheritance.
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
        metric_name("build_info"),
        "Static build/version info. Always 1 while the process is alive.",
        labelnames=("service", "role"),
        registry=REGISTRY,
    )

    # ─── HTTP API ───────────────────────────────────────────────────────
    http_requests_total = Counter(
        metric_name("http_requests_total"),
        "Total HTTP requests served by the API.",
        labelnames=("method", "route", "status_class"),
        registry=REGISTRY,
    )
    http_request_duration_seconds = Histogram(
        metric_name("http_request_duration_seconds"),
        "HTTP request duration in seconds.",
        labelnames=("method", "route"),
        buckets=histogram_buckets_short(),
        registry=REGISTRY,
    )
    http_requests_in_flight = Gauge(
        metric_name("http_requests_in_flight"),
        "HTTP requests currently being handled.",
        registry=REGISTRY,
    )

    # ─── Queue worker ───────────────────────────────────────────────────
    queue_dispatches_total = Counter(
        metric_name("queue_dispatches_total"),
        "Jobs dispatched onto the queue (counted at Bus.dispatch call-site).",
        labelnames=("queue", "job"),
        registry=REGISTRY,
    )
    queue_jobs_consumed_total = Counter(
        metric_name("queue_jobs_consumed_total"),
        "Jobs consumed by workers, tagged by outcome.",
        labelnames=("queue", "job", "outcome"),  # success|failed|timeout|orphan
        registry=REGISTRY,
    )
    queue_job_duration_seconds = Histogram(
        metric_name("queue_job_duration_seconds"),
        "Wall-clock time a worker spent executing a job.",
        labelnames=("queue", "job"),
        buckets=histogram_buckets_long(),
        registry=REGISTRY,
    )
    queue_jobs_in_flight = Gauge(
        metric_name("queue_jobs_in_flight"),
        "Jobs currently being executed by this worker process.",
        labelnames=("queue", "job"),
        registry=REGISTRY,
    )
    queue_wait_seconds = Histogram(
        metric_name("queue_wait_seconds"),
        "Time a message sat in the queue before being picked up.",
        labelnames=("queue", "job"),
        buckets=histogram_buckets_long(),
        registry=REGISTRY,
    )
    queue_jobs_dead_lettered_total = Counter(
        metric_name("queue_jobs_dead_lettered_total"),
        "Jobs sent to the dead-letter queue after exhausting max_attempts.",
        labelnames=("job",),
        registry=REGISTRY,
    )
    idempotency_total = Counter(
        metric_name("idempotency_total"),
        "Idempotency-key / unique-job lookup outcomes across dispatch paths.",
        labelnames=("scope", "outcome"),
        registry=REGISTRY,
    )

    # ─── Event listeners ────────────────────────────────────────────────
    listener_invocations_total = Counter(
        metric_name("listener_invocations_total"),
        "Event-listener fires by listener class + outcome.",
        labelnames=("listener", "outcome"),
        registry=REGISTRY,
    )
    listener_duration_seconds = Histogram(
        metric_name("listener_duration_seconds"),
        "Listener handle() wall-clock duration.",
        labelnames=("listener",),
        buckets=histogram_buckets_long(),
        registry=REGISTRY,
    )

    # ─── CLI commands ───────────────────────────────────────────────────
    command_invocations_total = Counter(
        metric_name("command_invocations_total"),
        "CLI commands invoked, by name + outcome.",
        labelnames=("command", "outcome"),
        registry=REGISTRY,
    )
    command_duration_seconds = Histogram(
        metric_name("command_duration_seconds"),
        "CLI command wall-clock duration.",
        labelnames=("command",),
        buckets=histogram_buckets_long(),
        registry=REGISTRY,
    )

    # ─── Scheduled tasks ────────────────────────────────────────────────
    scheduled_tasks_total = Counter(
        metric_name("scheduled_tasks_total"),
        "Scheduled task fires grouped by task id and outcome.",
        labelnames=("task", "outcome"),
        registry=REGISTRY,
    )
    scheduled_task_duration_seconds = Histogram(
        metric_name("scheduled_task_duration_seconds"),
        "Scheduled task wall-clock duration.",
        labelnames=("task",),
        buckets=histogram_buckets_long(),
        registry=REGISTRY,
    )

    # ─── Health probes ───────────────────────────────────────────────────
    health_probe_duration_seconds = Histogram(
        metric_name("health_probe_duration_seconds"),
        "Wall-clock duration of each dependency health probe.",
        labelnames=("dependency", "status"),
        buckets=histogram_buckets_short(),
        registry=REGISTRY,
    )
    health_probe_outcomes_total = Counter(
        metric_name("health_probe_outcomes_total"),
        "Health probe outcomes per dependency and status.",
        labelnames=("dependency", "status"),
        registry=REGISTRY,
    )

    # ─── Cache ─────────────────────────────────────────────────────────
    cache_operations_total = Counter(
        metric_name("cache_operations_total"),
        "Cara Cache facade operations — rolled up by scope + outcome.",
        labelnames=("scope", "operation", "outcome"),
        registry=REGISTRY,
    )


def init_build_info(metrics_cls: type = MetricsBase) -> None:
    """Set the static build-info gauge once at import time."""
    service = config("metrics.service", _NS)
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
