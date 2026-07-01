"""Observability bootstrap — generic wiring for tracking backends.

Currently ships Sentry / GlitchTip. Future backends (OpenTelemetry,
Datadog) plug into the same ``setup_observability`` family.
"""

from .AlertSink import AlertSink
from .Metrics import (
    MetricsBase,
    REGISTRY,
    bool_label,
    counter,
    gauge,
    histogram,
    histogram_buckets_long,
    init_build_info,
    normalize_metric_path,
    render,
    sample_db_pool_metrics,
    start_http_server,
    status_class,
)
from .Sentry import set_request_tag, set_request_user, setup_sentry
from .Tracing import setup_tracing

__all__ = [
    "AlertSink",
    "MetricsBase",
    "REGISTRY",
    "bool_label",
    "counter",
    "gauge",
    "histogram",
    "histogram_buckets_long",
    "init_build_info",
    "normalize_metric_path",
    "render",
    "sample_db_pool_metrics",
    "set_request_tag",
    "set_request_user",
    "setup_sentry",
    "setup_tracing",
    "start_http_server",
    "status_class",
]
