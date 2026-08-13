"""Pull-time DB metrics, build identity and metrics HTTP server lifecycle."""

from __future__ import annotations

import errno
import importlib
import threading
import time

from prometheus_client import generate_latest
from prometheus_client import start_http_server as _prom_start_http_server
from prometheus_client.exposition import CONTENT_TYPE_LATEST

import cara.facades as facades

_build_info_lock = threading.Lock()
_build_info_identity: tuple[int, str, str] | None = None
_http_server_started = False
_http_server_lock = threading.Lock()


def _read_db_pool_stats() -> dict[str, int] | None:
    postgres = importlib.import_module("cara.eloquent.connections.PostgresConnection")
    if not getattr(postgres, "_pool_initialized", False):
        return None
    semaphore = getattr(postgres, "_pool_semaphore", None)
    if semaphore is None:
        return None
    free = getattr(semaphore, "_value", None)
    if type(free) is not int or free < 0:
        raise RuntimeError("Database pool semaphore exposes an invalid free-slot count")
    pool = getattr(postgres, "CONNECTION_POOL", None)
    if not isinstance(pool, list):
        raise RuntimeError("Database pool storage is not initialized correctly")
    idle = len(pool)
    total = _configured_db_pool_max()
    if total < free:
        raise RuntimeError("Database pool free-slot count exceeds its configured maximum")
    return {"in_use": total - free, "idle": idle, "max": total}


def _configured_db_pool_max() -> int:
    postgres = importlib.import_module("cara.eloquent.connections.PostgresConnection")
    raw = getattr(postgres, "_pool_max_size", None)
    if isinstance(raw, bool) or not isinstance(raw, int) or raw <= 0:
        raise RuntimeError(
            "Initialized database pool must publish a positive integer maximum."
        )
    return raw


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


def _init_build_info(
    metrics_cls: type,
    namespace: str,
    *,
    service: str | None = None,
    role: str | None = None,
) -> None:
    global _build_info_identity
    if service is None or role is None:
        raise RuntimeError(
            "Prometheus build info requires explicit service and role labels."
        )
    if not isinstance(service, str) or not service.strip():
        raise TypeError("Prometheus build-info service must be a non-empty string.")
    if not isinstance(role, str) or not role.strip():
        raise TypeError("Prometheus build-info role must be a non-empty string.")
    identity = (id(metrics_cls.build_info), service.strip(), role.strip())
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
    service: str,
    role: str,
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
    global _http_server_started
    with _http_server_lock:
        if _http_server_started:
            return None
        if port is None:
            raise RuntimeError("Metrics HTTP server requires an explicit port.")
        if isinstance(port, bool) or not isinstance(port, int) or port <= 0:
            raise ValueError("Metrics HTTP server port must be a positive integer.")
        effective_port = port
        for attempt in range(5):
            try:
                _prom_start_http_server(effective_port, addr=host, registry=registry)
                break
            except OSError as exc:
                if exc.errno != errno.EADDRINUSE or attempt == 4:
                    raise
                time.sleep(2)
        _init_build_info(metrics_cls, namespace, service=service, role=role)
        _http_server_started = True
        return effective_port
