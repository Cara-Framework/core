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
- ``job_class``: class name — dozens (``job`` is reserved for the Prometheus
  scrape-target label and would be overwritten during ingestion)
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

# Prefixed public-id: a short uppercase type prefix immediately followed by a
# Crockford-base32 ULID (e.g. ``CHN01KX…``, ``ORD01H…``, ``STK01J…``). This is
# the dominant dynamic segment in ``MakesPublicId``-based apps, and — unlike a
# bare ULID — it is neither all-digit, a UUID, nor exactly 26 alnum chars, so
# the heuristics below miss it. Without this rule EVERY channel/order/case id
# spawns its own ``route`` label time-series and the HTTP metrics explode.
# Bound (2–6 prefix letters, ≥20 Crockford chars) mirrors the fold the
# usage-analytics SQL already applies to the same ``http_request_log`` paths.
_PREFIXED_ID_RE = re.compile(r"[A-Z]{2,6}[0-9A-HJKMNP-TV-Z]{20,}")


def normalize_metric_path(path: str) -> str:
    """Replace dynamic URL segments with placeholders to bound label cardinality.

    ``/api/items/123`` → ``/api/items/{id}``
    ``/api/items/01HABC...`` → ``/api/items/{ulid}``
    ``/api/items/CHN01HABC...`` → ``/api/items/{id}`` (prefixed public-id)
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
        elif _PREFIXED_ID_RE.fullmatch(seg):
            out.append("{id}")
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

    @staticmethod
    def safe_set(metric, labels: dict, value: float) -> None:
        """Set a gauge value without raising on failure (mirrors safe_inc/
        safe_observe): a metric backend hiccup must never break the sampler
        that feeds it."""
        try:
            metric.labels(**labels).set(value)
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
        labelnames=("queue", "job_class"),
        registry=REGISTRY,
    )
    queue_jobs_consumed_total = Counter(
        metric_name("queue_jobs_consumed_total"),
        "Jobs consumed by workers, tagged by outcome.",
        labelnames=("queue", "job_class", "outcome"),
        registry=REGISTRY,
    )
    queue_job_duration_seconds = Histogram(
        metric_name("queue_job_duration_seconds"),
        "Wall-clock time a worker spent executing a job.",
        labelnames=("queue", "job_class"),
        buckets=histogram_buckets_long(),
        registry=REGISTRY,
    )
    queue_jobs_in_flight = Gauge(
        metric_name("queue_jobs_in_flight"),
        "Jobs currently being executed by this worker process.",
        labelnames=("queue", "job_class"),
        registry=REGISTRY,
    )
    queue_wait_seconds = Histogram(
        metric_name("queue_wait_seconds"),
        "Time a message sat in the queue before being picked up.",
        labelnames=("queue", "job_class"),
        buckets=histogram_buckets_long(),
        registry=REGISTRY,
    )
    queue_jobs_dead_lettered_total = Counter(
        metric_name("queue_jobs_dead_lettered_total"),
        "Jobs sent to the dead-letter queue after exhausting max_attempts.",
        labelnames=("job_class",),
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
    scheduler_last_tick_timestamp_seconds = Gauge(
        metric_name("scheduler_last_tick_timestamp_seconds"),
        "Unix timestamp when the scheduler most recently started a task tick.",
        registry=REGISTRY,
    )
    scheduled_task_last_run_timestamp_seconds = Gauge(
        metric_name("scheduled_task_last_run_timestamp_seconds"),
        "Unix timestamp when a scheduled task most recently finished.",
        labelnames=("task",),
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

    # ─── Database connection pool ───────────────────────────────────────
    # Saturation signal for the ORM connection pool (see
    # ``cara.eloquent.connections.PostgresConnection``). ``in_use`` vs
    # ``max`` is the headline ratio — when it pins at 1.0, callers start
    # blocking on ``_pool_semaphore.acquire`` and eventually 503 with
    # ``DatabaseUnavailableException``. ``idle`` is the count of warm
    # connections parked in the reuse list (checked out → checked back
    # in). Populated by :func:`sample_db_pool_metrics`, which is wired
    # into the existing Prometheus scrape path (see ``render``).
    db_pool_connections_in_use = Gauge(
        metric_name("db_pool_connections_in_use"),
        "DB connections currently checked out of the pool (max - free slots).",
        registry=REGISTRY,
    )
    db_pool_connections_idle = Gauge(
        metric_name("db_pool_connections_idle"),
        "Warm DB connections parked in the pool's reuse list, ready to hand out.",
        registry=REGISTRY,
    )
    db_pool_connections_max = Gauge(
        metric_name("db_pool_connections_max"),
        "Configured maximum size of the DB connection pool (total slots).",
        registry=REGISTRY,
    )


def _read_db_pool_stats() -> dict[str, int] | None:
    """Read the ORM connection pool's in-use / idle / max counts.

    Returns ``None`` when the pool has not been initialised yet (e.g.
    no query has run, or pooling is disabled) so the caller can skip the
    sample rather than emit misleading zeros. Never raises — a metrics
    probe must not be able to take down the scrape path or the request
    that triggered it.

    The numbers are read straight off the Postgres pool's module-level
    state (``_pool_semaphore`` free-slot count + the ``CONNECTION_POOL``
    reuse list). The configured ceiling comes from the database config;
    when that lookup fails we fall back to ``in_use + free`` (the live
    slot total), which equals the real maximum whenever the semaphore is
    intact.
    """
    try:
        # Import the *module*, not the re-exported class. The submodule
        # and the class share the name ``PostgresConnection``, so the
        # package attribute resolves to the class (the package __init__
        # re-exports it) — ``importlib.import_module`` is the reliable
        # way to reach the module object whose scope holds the pool
        # state (``_pool_semaphore``, ``CONNECTION_POOL``,
        # ``_pool_initialized``).
        import importlib

        _pg = importlib.import_module(
            "cara.eloquent.connections.PostgresConnection"
        )
    except Exception:
        return None

    try:
        if not getattr(_pg, "_pool_initialized", False):
            return None
        semaphore = getattr(_pg, "_pool_semaphore", None)
        if semaphore is None:
            return None

        # ``threading.Semaphore`` keeps the current free-slot count in
        # ``_value``. Reading it is racy by a connection or two, which is
        # fine for a gauge — we only need the saturation trend, not an
        # exact ledger.
        free = int(getattr(semaphore, "_value", 0) or 0)
        idle = len(getattr(_pg, "CONNECTION_POOL", []) or [])

        total = _configured_db_pool_max()
        if total is None or total < free:
            # Fall back to the live slot count when config is unavailable
            # or smaller than what the semaphore reports (stale config).
            total = free + max(0, idle)  # best-effort lower bound
            # The semaphore's free count alone can't tell us how many are
            # checked out without the max; if we still have nothing better
            # than free, treat free as the total (in_use = 0).
            total = max(total, free)

        in_use = max(0, total - free)
        return {"in_use": in_use, "idle": idle, "max": total}
    except Exception:
        return None


def _configured_db_pool_max() -> int | None:
    """Best-effort read of the configured pool ceiling from db config.

    ``database.drivers.<default>.connection_pooling_max_size``. Returns
    ``None`` when config isn't loaded or the key is absent.
    """
    try:
        default = config("database.default", None)
        drivers = config("database.drivers", {}) or {}
        if not isinstance(drivers, dict):
            return None
        details = drivers.get(default) if default is not None else None
        if not isinstance(details, dict):
            # Single-driver configs sometimes inline the details.
            details = drivers
        raw = details.get("connection_pooling_max_size")
        return int(raw) if raw is not None else None
    except Exception:
        return None


def sample_db_pool_metrics(metrics_cls: type = MetricsBase) -> None:
    """Refresh the DB-pool gauges from the live pool state.

    Idempotent and exception-safe. Call it just before serialising the
    Prometheus payload (it is invoked from :func:`render`) so the gauges
    reflect the pool at scrape time rather than whenever the last query
    happened to run. A no-op when the pool is uninitialised.
    """
    stats = _read_db_pool_stats()
    if stats is None:
        return
    try:
        metrics_cls.db_pool_connections_in_use.set(stats["in_use"])
        metrics_cls.db_pool_connections_idle.set(stats["idle"])
        metrics_cls.db_pool_connections_max.set(stats["max"])
    except Exception:
        # Gauge writes must never break the scrape.
        return


_build_info_lock = threading.Lock()
_build_info_identity: tuple[int, str, str] | None = None


def init_build_info(
    metrics_cls: type = MetricsBase,
    *,
    service: str | None = None,
    role: str | None = None,
) -> None:
    """(Re-)stamp the static build-info gauge.

    Called at import time for early exposure, but import can run BEFORE the
    app config is bootstrapped — service/role then resolve to their defaults
    ("unknown"). ``start_http_server`` calls this again once config is
    definitely loaded; ``clear()`` first so the stale default-labelled child
    doesn't linger next to the corrected one.
    """
    global _build_info_identity

    resolved_service = service or config("metrics.service", _NS)
    resolved_role = role or config("metrics.role", "unknown")
    identity = (
        id(metrics_cls.build_info),
        str(resolved_service),
        str(resolved_role),
    )
    with _build_info_lock:
        if _build_info_identity == identity:
            return
        metrics_cls.build_info.clear()
        metrics_cls.build_info.labels(service=identity[1], role=identity[2]).set(1)
        _build_info_identity = identity


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
    """Produce the Prometheus text payload + content-type.

    Refreshes build identity and pull-style gauges immediately before
    serialising so API processes whose metrics module was imported before
    config boot do not remain permanently labelled ``unknown``.
    """
    init_build_info()
    sample_db_pool_metrics()
    return generate_latest(REGISTRY), CONTENT_TYPE_LATEST


_http_server_started = False
_http_server_lock = threading.Lock()


def start_http_server(
    port: int | None = None,
    host: str = "0.0.0.0",
    *,
    service: str | None = None,
    role: str | None = None,
) -> int | None:
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
        # A worker restart races its predecessor for the socket: the old
        # process may hold the port for a few seconds after SIGTERM. Losing
        # that race must not mean a silently metrics-less process until the
        # NEXT restart — retry EADDRINUSE briefly before giving up.
        import errno
        import time as _time

        for attempt in range(5):
            try:
                _prom_start_http_server(effective_port, addr=host, registry=REGISTRY)
                break
            except OSError as e:
                if e.errno != errno.EADDRINUSE or attempt == 4:
                    raise
                _time.sleep(2)
        # Config is guaranteed loaded here (the port above came from it) —
        # re-stamp build_info so service/role reflect the real values instead
        # of the import-time "unknown" defaults.
        init_build_info(service=service, role=role)
        _http_server_started = True
        return effective_port
