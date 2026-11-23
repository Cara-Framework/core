"""Pull-time DB metrics, build identity and metrics HTTP server lifecycle."""

from __future__ import annotations

import errno
import threading
import time

from prometheus_client import generate_latest
from prometheus_client import start_http_server as _prom_start_http_server
from prometheus_client.exposition import CONTENT_TYPE_LATEST

import cara.facades as facades
from cara.configuration import config
from cara.eloquent import database_pool_stats
from cara.exceptions import InvalidConfigurationSetupException

_build_info_lock = threading.Lock()
_build_info_identity: tuple[int, str, str] | None = None
_http_server_started = False
_http_server_lock = threading.Lock()


def _read_db_pool_stats() -> dict[str, int] | None:
    """The pool gauges' figures, summed over every configured connection
    (``cara.eloquent.database_pool_stats``), or None before a first pool
    exists. The ORM keeps one pool per connection; the gauges stay the
    process-wide totals they always were.

    Figures that cannot be true raise instead of being published: a gauge
    that reports a nonsense saturation is worse than a failed scrape.
    """
    stats = database_pool_stats()
    if stats is None:
        return None
    figures = {name: stats.get(name) for name in ("in_use", "idle", "max")}
    for name, value in figures.items():
        if type(value) is not int or value < 0:
            raise RuntimeError(f"Database pool publishes an invalid '{name}' figure")
    if figures["max"] <= 0:
        raise RuntimeError(
            "Initialized database pool must publish a positive integer maximum."
        )
    if figures["in_use"] > figures["max"]:
        raise RuntimeError("Database pool in-use count exceeds its configured maximum")
    return figures


def _sample_db_pool_metrics(metrics_cls: type) -> None:
    stats = _read_db_pool_stats()
    if stats is None:
        return
    try:
        metrics_cls.db_pool_connections_in_use.set(stats["in_use"])
        metrics_cls.db_pool_connections_idle.set(stats["idle"])
        metrics_cls.db_pool_connections_max.set(stats["max"])
    except (AttributeError, KeyError, TypeError, ValueError) as exc:
        facades.Log.warning(
            "metrics: could not publish database pool gauges: %s",
            exc,
            category="cara.observability.metrics",
        )


def _config_value(key: str, default):
    """``config()`` that tolerates pre-boot contexts.

    Metrics may be imported (and build info stamped) before
    ``ConfigurationProvider`` registers the application — bare imports and
    framework tests have no configuration at all. Everything here has a
    documented default, so an unavailable configuration resolves to it."""
    try:
        return config(key, default)
    except InvalidConfigurationSetupException:
        return default


def _init_build_info(
    metrics_cls: type,
    namespace: str,
    *,
    service: str | None = None,
    role: str | None = None,
) -> None:
    """(Re-)stamp the static build-info gauge.

    May run at import time, BEFORE app config is bootstrapped — service/role
    then resolve to config defaults (``metrics.service``/``metrics.role``,
    falling back to the namespace and ``unknown``). The server start and
    every render call this again once config is definitely loaded;
    ``clear()`` first so a stale default-labelled child doesn't linger next
    to the corrected one.
    """
    global _build_info_identity

    resolved_service = str(service or _config_value("metrics.service", namespace)).strip()
    resolved_role = str(role or _config_value("metrics.role", "unknown")).strip()
    identity = (
        id(metrics_cls.build_info),
        resolved_service or namespace,
        resolved_role or "unknown",
    )
    with _build_info_lock:
        if _build_info_identity == identity:
            return
        metrics_cls.build_info.clear()
        metrics_cls.build_info.labels(service=identity[1], role=identity[2]).set(1)
        _build_info_identity = identity


def _render(
    metrics_cls: type,
    registry,
    namespace: str,
    *,
    service: str | None = None,
    role: str | None = None,
) -> tuple[bytes, str]:
    _init_build_info(metrics_cls, namespace, service=service, role=role)
    _sample_db_pool_metrics(metrics_cls)
    return generate_latest(registry), CONTENT_TYPE_LATEST


def _start_http_server(
    metrics_cls: type,
    registry,
    namespace: str,
    port: int | None = None,
    host: str = "0.0.0.0",
    *,
    service: str | None = None,
    role: str | None = None,
) -> int | None:
    """Stand up /metrics, resolving the port from ``metrics.port`` when unset.

    Observability must never be the reason work stops: an unconfigured port
    warns loudly and runs WITHOUT /metrics; an explicit ``0`` (argument or
    config) is the documented, silent opt-out; a port still held by another
    role after the restart-race retries warns and continues rather than
    killing a process whose actual job is fine (a raising version once left
    the queue relay unstartable with 1250 deliveries sitting unpublished).
    """
    global _http_server_started
    with _http_server_lock:
        if _http_server_started:
            return None
        if port is None:
            configured = _config_value("metrics.port", None)
            if configured is None or configured == "":
                facades.Log.warning(
                    f"metrics: no 'metrics.port' configured — running WITHOUT "
                    f"/metrics for role {role or 'unknown'}. Set metrics.port "
                    f"(METRICS_PORT) in this deployable's config to be scraped."
                )
                return None
            effective_port = int(configured)
        else:
            if isinstance(port, bool) or not isinstance(port, int):
                raise ValueError("Metrics HTTP server port must be an integer.")
            effective_port = port
        if effective_port <= 0:
            return None
        for attempt in range(5):
            try:
                _prom_start_http_server(effective_port, addr=host, registry=registry)
                break
            except OSError as exc:
                if exc.errno != errno.EADDRINUSE:
                    raise
                if attempt < 4:
                    time.sleep(2)
                    continue
                facades.Log.warning(
                    f"metrics: port {effective_port} is held by another "
                    f"process — continuing WITHOUT /metrics for role "
                    f"{role or 'unknown'}. Give each role its own METRICS_PORT "
                    f"to restore scraping."
                )
                return None
        _init_build_info(metrics_cls, namespace, service=service, role=role)
        _http_server_started = True
        return effective_port
