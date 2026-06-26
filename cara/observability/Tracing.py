"""Distributed tracing wiring (OpenTelemetry → OTLP / Grafana Tempo).

Mirrors ``Sentry.py``: idempotent, fail-open, optional-dependency.
Tracing stays a COMPLETE no-op unless BOTH are true:

    1. ``tracing.enabled`` config/env is truthy, and
    2. the ``opentelemetry`` packages are installed.

So it is safe to call from every bootstrap entry point (HTTP server,
queue worker, CLI command) in any environment — a missing dep or a
broken exporter can never block the app from booting.

Config (read via cara ``config()`` with UPPER_SNAKE env fallback):

    tracing.enabled        "1"/"true"/"yes"/"on" to turn on   (default off)
    tracing.otlp_endpoint  OTLP HTTP traces URL               (default http://localhost:4318/v1/traces)
    app.env                resource ``deployment.environment``(default "dev")

Uses the OTLP **HTTP** exporter (port 4318) rather than gRPC: it rides
on ``requests`` (already a dependency) and avoids the ``grpcio`` native
wheel, which lags on bleeding-edge Python. 4318 is the standard OTLP/HTTP
receiver port published by the monitoring stack. App processes run
natively on the host, so the default is localhost.
"""

from __future__ import annotations

import os
import sys
import threading

_setup_done = False
_setup_lock = threading.Lock()


def _env(key: str, default: str = "") -> str:
    """Read from cara ``config()`` if available, else ``os.environ``.

    Mirrors ``Sentry._env`` — kept local so the tracing module is
    self-contained and a Sentry refactor can't break it. During very
    early bootstrap ``config()`` may throw; fall back to the matching
    UPPER_SNAKE environment variable.
    """
    try:
        from cara.configuration import config

        val = config(key)
        if val is not None:
            return str(val)
    except Exception as e:
        print(
            f"[cara.observability._env] config({key!r}) failed: "
            f"{e.__class__.__name__}: {e}",
            file=sys.stderr,
        )
    return os.environ.get(key.upper().replace(".", "_"), str(default))


def _truthy(value: str) -> bool:
    return value.strip().lower() in ("1", "true", "yes", "on")


def setup_tracing(
    *,
    service_name: str,
    release: str | None = None,
) -> None:
    """Initialise OpenTelemetry tracing → OTLP/Tempo, if enabled.

    Idempotent — repeat calls after the first are no-ops, so it is safe
    to invoke from multiple bootstrap entry points. Fail-open: any
    error is swallowed so a tracing problem can't prevent boot.

    Args:
        service_name: ``service.name`` resource attribute (e.g.
            ``"example-services"``). Required.
        release: ``service.version`` resource attribute. Defaults to
            ``"dev"`` when omitted.
    """
    global _setup_done
    with _setup_lock:
        if _setup_done:
            return
        _try(_init_tracing, service_name, release or "dev")
        _setup_done = True


def _try(fn, *args, **kwargs) -> None:
    """Run a setup step swallowing failures so one broken backend
    cannot prevent the rest of the bootstrap from coming up."""
    try:
        fn(*args, **kwargs)
    except Exception as e:
        try:
            from cara.facades import Log

            Log.warning("[cara.observability] %s failed: %s: %s", fn.__name__, e.__class__.__name__, e, category='observability')
        except Exception as log_err:
            print(
                f"[cara.observability._try] Log.warning failed after "
                f"{fn.__name__} error ({e}): {log_err}",
                file=sys.stderr,
            )


def _init_tracing(service_name: str, release: str) -> None:
    if not _truthy(_env("tracing.enabled", "")):
        return  # disabled — opt-in only, nothing installed/started

    # ``opentelemetry`` is an OPTIONAL dependency. Tracing defaults ON
    # (config/tracing.py) so the monitoring stack works out of the box,
    # but a clean install without the OTel wheels is a fully supported
    # configuration — NOT an error. Catch the missing import here and
    # degrade to a silent no-op (one debug line, not a per-boot warning)
    # so we never make ``opentelemetry`` a hard requirement. Letting the
    # ImportError escape would surface as a ``Log.warning`` on every boot.
    try:
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
            OTLPSpanExporter,
        )
        from opentelemetry.propagate import set_global_textmap
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
        from opentelemetry.sdk.trace.sampling import (
            ParentBased,
            TraceIdRatioBased,
        )
        from opentelemetry.trace import (
            get_tracer_provider,
            set_tracer_provider,
        )
        from opentelemetry.trace.propagation.tracecontext import (
            TraceContextTextMapPropagator,
        )
    except ImportError as e:
        # opentelemetry not installed → tracing is a no-op. Debug-level,
        # category-tagged, emitted once (setup is idempotent), so a
        # normal boot stays quiet. Fall back to a single concise stderr
        # line if the Log facade isn't ready this early in bootstrap.
        try:
            from cara.facades import Log

            Log.debug("[cara.observability] tracing disabled: opentelemetry not installed (%s); install the OTel extras to enable span export", e.name or e, category='observability')
        except Exception:
            print(
                "[cara.observability] tracing disabled: opentelemetry "
                "not installed (no-op)",
                file=sys.stderr,
            )
        return

    # Another entry point in this process may already have installed a
    # real provider — don't stack a second one.
    if isinstance(get_tracer_provider(), TracerProvider):
        return

    endpoint = _env("tracing.otlp_endpoint", "http://localhost:4318/v1/traces")
    resource = Resource.create(
        {
            "service.name": service_name,
            "service.version": release,
            "deployment.environment": _env("app.env", "dev"),
        }
    )
    # Sampler: 100% by default (tracing.sample_ratio = 1.0); dial down
    # at scale instead of turning tracing off. ParentBased makes child
    # spans inherit the root's decision, so a sampled trace stays whole.
    # (ParentBased / TraceIdRatioBased imported in the guarded block above.)
    try:
        _ratio = float(_env("tracing.sample_ratio", "1.0"))
    except Exception:
        _ratio = 1.0
    provider = TracerProvider(
        resource=resource, sampler=ParentBased(TraceIdRatioBased(_ratio))
    )
    # BatchSpanProcessor exports asynchronously on a background thread —
    # never blocks the request/job path waiting on the exporter.
    provider.add_span_processor(
        BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint))
    )
    set_tracer_provider(provider)

    # W3C TraceContext is OTel's default propagator; set it explicitly
    # so the RabbitMQ header inject/extract in Obs-4 is unambiguous and
    # interoperable with anything else speaking ``traceparent``.
    set_global_textmap(TraceContextTextMapPropagator())

    # Auto-instrument client libraries (DB, HTTP, Redis). Each is
    # guarded independently so a missing/incompatible instrumentor can
    # never break the others or boot. Queue (RabbitMQ) propagation is
    # done manually in AMQPDriver — see cara.observability.Trace.
    _instrument_libraries()

    # Plain stderr — deliberately NOT the Log facade. Tracing setup runs
    # early in bootstrap (before providers boot); touching the Log
    # facade here forces a partial provider boot and spins up
    # non-daemon threads that block clean process exit for short-lived
    # CLI commands. stderr keeps this side-effect-free.
    print(
        f"[cara.observability] OpenTelemetry tracing enabled "
        f"(service={service_name}, otlp={endpoint})",
        file=sys.stderr,
    )


def _instrument_libraries() -> None:
    """Best-effort OTel auto-instrumentation of client libraries.

    Each instrumentor is tried in isolation: a missing package or a
    version-incompatible instrumentor logs to stderr and is skipped,
    never affecting the others or aborting bootstrap.
    """

    def _try(name: str, fn) -> None:
        try:
            fn()
        except Exception as e:
            print(
                f"[cara.observability] {name} auto-instrument skipped: "
                f"{e.__class__.__name__}: {e}",
                file=sys.stderr,
            )

    def _psycopg2() -> None:
        from opentelemetry.instrumentation.psycopg2 import Psycopg2Instrumentor

        # skip_dep_check: works with psycopg2 and psycopg2-binary alike.
        Psycopg2Instrumentor().instrument(skip_dep_check=True)

    def _requests() -> None:
        from opentelemetry.instrumentation.requests import RequestsInstrumentor

        RequestsInstrumentor().instrument()

    def _httpx() -> None:
        from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor

        HTTPXClientInstrumentor().instrument()

    def _redis() -> None:
        from opentelemetry.instrumentation.redis import RedisInstrumentor

        RedisInstrumentor().instrument()

    _try("psycopg2", _psycopg2)
    _try("requests", _requests)
    _try("httpx", _httpx)
    _try("redis", _redis)


__all__ = ["setup_tracing"]
