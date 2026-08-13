"""Explicit OpenTelemetry → OTLP process wiring."""

from __future__ import annotations

import math
import threading
from numbers import Real
from typing import Any
from urllib.parse import urlsplit

_setup_done = False
_setup_lock = threading.Lock()


def setup_tracing(
    *,
    service_name: str,
    enabled: bool,
    endpoint: str | None,
    environment: str,
    sample_ratio: float,
    release: str | None = None,
) -> None:
    """Configure tracing from composition-root values.

    Disabled tracing is a deliberate no-op. Enabled tracing is required
    infrastructure: missing dependencies, invalid settings, exporter setup,
    or instrumentation failures propagate and block boot.
    """
    if not isinstance(enabled, bool):
        raise TypeError("Tracing enabled must be a boolean.")
    name = _required_text(service_name, "service_name")
    target_environment = _required_text(environment, "environment")
    target_release = _required_text(release or "dev", "release")
    ratio = _sample_ratio(sample_ratio)
    target_endpoint = _endpoint(endpoint, enabled=enabled)

    global _setup_done
    with _setup_lock:
        if _setup_done:
            return
        if enabled:
            _init_tracing(
                service_name=name,
                release=target_release,
                endpoint=target_endpoint,
                environment=target_environment,
                sample_ratio=ratio,
            )
        _setup_done = True


def _init_tracing(
    *,
    service_name: str,
    release: str,
    endpoint: str,
    environment: str,
    sample_ratio: float,
) -> None:
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
        OTLPSpanExporter,  # local: heavy optional dep
    )
    from opentelemetry.propagate import (
        set_global_textmap,  # local: heavy optional dep
    )
    from opentelemetry.sdk.resources import (
        Resource,  # local: heavy optional dep
    )
    from opentelemetry.sdk.trace import (
        TracerProvider,  # local: heavy optional dep
    )
    from opentelemetry.sdk.trace.export import (
        BatchSpanProcessor,  # local: heavy optional dep
    )
    from opentelemetry.sdk.trace.sampling import (  # local: heavy optional dep
        ParentBased,
        TraceIdRatioBased,
    )
    from opentelemetry.trace import (  # local: heavy optional dep
        get_tracer_provider,
        set_tracer_provider,
    )
    from opentelemetry.trace.propagation.tracecontext import (
        TraceContextTextMapPropagator,  # local: heavy optional dep
    )

    if isinstance(get_tracer_provider(), TracerProvider):
        raise RuntimeError(
            "OpenTelemetry already has a TracerProvider before Cara tracing setup."
        )

    resource = Resource.create(
        {
            "service.name": service_name,
            "service.version": release,
            "deployment.environment": environment,
        }
    )
    provider = TracerProvider(
        resource=resource,
        sampler=ParentBased(TraceIdRatioBased(sample_ratio)),
    )
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint)))
    set_tracer_provider(provider)
    set_global_textmap(TraceContextTextMapPropagator())
    _instrument_libraries()


def _instrument_libraries() -> None:
    from opentelemetry.instrumentation.httpx import (
        HTTPXClientInstrumentor,  # local: heavy optional dep
    )
    from opentelemetry.instrumentation.psycopg2 import (
        Psycopg2Instrumentor,  # local: heavy optional dep
    )
    from opentelemetry.instrumentation.redis import (
        RedisInstrumentor,  # local: heavy optional dep
    )
    from opentelemetry.instrumentation.requests import (
        RequestsInstrumentor,  # local: heavy optional dep
    )

    Psycopg2Instrumentor().instrument(skip_dep_check=True)
    RequestsInstrumentor().instrument()
    HTTPXClientInstrumentor().instrument()
    RedisInstrumentor().instrument()


def _endpoint(value: Any, *, enabled: bool) -> str:
    if value is None or value == "":
        if enabled:
            raise ValueError("Tracing endpoint is required when tracing is enabled.")
        return ""
    if not isinstance(value, str):
        raise TypeError("Tracing endpoint must be a string or None.")
    candidate = value.strip()
    if not candidate:
        raise ValueError("Tracing endpoint must not contain only whitespace.")
    try:
        parsed = urlsplit(candidate)
        port = parsed.port
    except ValueError as exc:
        raise ValueError(f"Tracing endpoint is invalid: {exc}") from exc
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise ValueError("Tracing endpoint must be an absolute HTTP(S) URL.")
    if parsed.username is not None or parsed.password is not None or parsed.fragment:
        raise ValueError("Tracing endpoint must not contain credentials or a fragment.")
    if port is not None and not 1 <= port <= 65535:
        raise ValueError("Tracing endpoint port must be between 1 and 65535.")
    return candidate


def _required_text(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"Tracing {field} must be a non-empty string.")
    return value.strip()


def _sample_ratio(value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, Real):
        raise TypeError("Tracing sample_ratio must be a real number.")
    ratio = float(value)
    if not math.isfinite(ratio) or not 0.0 <= ratio <= 1.0:
        raise ValueError("Tracing sample_ratio must be between 0.0 and 1.0.")
    return ratio


__all__ = ["setup_tracing"]
