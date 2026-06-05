"""No-op-safe helpers over the OpenTelemetry trace API.

Hot-path code (jobs, queue driver, scrape driver) imports from here so
it never has to guard the optional ``opentelemetry`` import itself.
When OTel isn't installed OR tracing was never set up, every helper is
a cheap no-op — a span context manager that yields ``None``, an inject
that leaves the carrier untouched, etc.

Trace context is propagated through the queue **inside the job
payload** (a ``dict`` carrier), mirroring how AMQPDriver already
carries ``attempts`` in the payload rather than AMQP headers — so it
survives the delayed-retry republish path.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Iterator

try:
    from opentelemetry import context as _ctx
    from opentelemetry import propagate as _propagate
    from opentelemetry import trace as _trace

    _OTEL = True
except Exception:  # pragma: no cover - optional dependency
    _OTEL = False


def available() -> bool:
    """True when the opentelemetry packages are importable."""
    return _OTEL


def get_tracer(name: str = "cara"):
    """Return an OTel tracer, or ``None`` when OTel isn't installed.

    Note: when OTel is installed but tracing is disabled, the global
    provider is the default no-op provider, so the returned tracer's
    spans are themselves no-ops — zero overhead, nothing exported.
    """
    if not _OTEL:
        return None
    return _trace.get_tracer(name)


@contextmanager
def span(
    name: str,
    attributes: dict[str, Any] | None = None,
    kind: str | None = None,
) -> Iterator[Any]:
    """Start ``name`` as the current span. No-op when OTel is absent.

    ``kind`` is an OTel ``SpanKind`` name (e.g. ``"SERVER"``,
    ``"CONSUMER"``, ``"CLIENT"``); invalid/None falls back to internal.
    """
    if not _OTEL:
        yield None
        return
    tracer = _trace.get_tracer("cara")
    span_kind = _trace.SpanKind.INTERNAL
    if kind:
        span_kind = getattr(_trace.SpanKind, kind.upper(), _trace.SpanKind.INTERNAL)
    with tracer.start_as_current_span(
        name, kind=span_kind, attributes=attributes or {}
    ) as s:
        yield s


def inject(carrier: dict[str, Any] | None = None) -> dict[str, Any]:
    """Inject the current trace context (``traceparent``) into a dict.

    Returns the carrier (created if ``None``). No-op → empty/unchanged
    dict when OTel is absent, which is harmless to stash in a payload.
    """
    carrier = {} if carrier is None else carrier
    if _OTEL:
        _propagate.inject(carrier)
    return carrier


@contextmanager
def extracted_context(carrier: dict[str, Any] | None) -> Iterator[None]:
    """Attach the trace context extracted from ``carrier`` for the block.

    Use on the consume side so a job's span becomes a child of the span
    that dispatched it → one product's whole journey is a single trace.
    """
    if not _OTEL or not carrier:
        yield
        return
    parent = _propagate.extract(carrier)
    token = _ctx.attach(parent)
    try:
        yield
    finally:
        _ctx.detach(token)


def set_attributes(**attrs: Any) -> None:
    """Set attributes on the current span. No-op when absent/none."""
    if not _OTEL:
        return
    s = _trace.get_current_span()
    if s is None:
        return
    for key, value in attrs.items():
        if value is None:
            continue
        try:
            s.set_attribute(key, value)
        except Exception:
            pass


def record_exception(exc: BaseException) -> None:
    """Record an exception + mark the current span errored."""
    if not _OTEL:
        return
    s = _trace.get_current_span()
    if s is None:
        return
    try:
        s.record_exception(exc)
        s.set_status(_trace.Status(_trace.StatusCode.ERROR, str(exc)))
    except Exception:
        pass


def current_trace_id() -> str:
    """Hex trace id of the active span, or '' when none / OTel absent.

    Used to stamp wide events + log lines with the trace they belong to.
    """
    if not _OTEL:
        return ""
    try:
        ctx = _trace.get_current_span().get_span_context()
        if getattr(ctx, "is_valid", False) and ctx.trace_id:
            return format(ctx.trace_id, "032x")
    except Exception:
        pass
    return ""


def current_span_id() -> str:
    """Hex span id of the active span, or '' when none / OTel absent.

    Stamped onto each wide event so the lifecycle graph can draw edges
    (parent_span_id → span_id = "dispatching job → this job").
    """
    if not _OTEL:
        return ""
    try:
        ctx = _trace.get_current_span().get_span_context()
        if getattr(ctx, "is_valid", False) and ctx.span_id:
            return format(ctx.span_id, "016x")
    except Exception:
        pass
    return ""


def parent_span_id_from_carrier(carrier: dict[str, Any] | None) -> str:
    """Span id of the job that dispatched us, parsed from the carrier.

    The W3C ``traceparent`` is ``00-<trace32>-<span16>-<flags>``; the
    span segment is the *dispatcher's* span id, which becomes the parent
    edge in the lifecycle graph. '' when absent.
    """
    if not carrier:
        return ""
    tp = carrier.get("traceparent") if isinstance(carrier, dict) else None
    if not tp:
        return ""
    parts = str(tp).split("-")
    return parts[2] if len(parts) >= 4 else ""


@contextmanager
def root_span(
    name: str,
    attributes: dict[str, Any] | None = None,
    kind: str | None = None,
    link_carrier: dict[str, Any] | None = None,
) -> Iterator[Any]:
    """Start ``name`` as a NEW ROOT trace, optionally LINKED to a carrier.

    Use at per-product boundaries: a product's lifecycle becomes its own
    compact trace instead of being buried inside the giant discovery
    trace, while a span ``Link`` back to the discovery preserves
    navigation. No-op-safe.
    """
    if not _OTEL:
        yield None
        return
    tracer = _trace.get_tracer("cara")
    span_kind = _trace.SpanKind.INTERNAL
    if kind:
        span_kind = getattr(_trace.SpanKind, kind.upper(), _trace.SpanKind.INTERNAL)
    links = []
    try:
        if link_carrier:
            parent_ctx = _propagate.extract(link_carrier)
            parent_sc = _trace.get_current_span(parent_ctx).get_span_context()
            if getattr(parent_sc, "is_valid", False):
                links = [_trace.Link(parent_sc)]
    except Exception:
        links = []
    # Empty Context() = detach any ambient parent → this span roots a
    # brand-new trace; the Link (if any) keeps the discovery reachable.
    from opentelemetry.context import Context as _Context

    with tracer.start_as_current_span(
        name,
        context=_Context(),
        kind=span_kind,
        attributes=attributes or {},
        links=links,
    ) as s:
        yield s


__all__ = [
    "available",
    "get_tracer",
    "span",
    "root_span",
    "inject",
    "extracted_context",
    "set_attributes",
    "record_exception",
    "current_trace_id",
    "current_span_id",
    "parent_span_id_from_carrier",
]
