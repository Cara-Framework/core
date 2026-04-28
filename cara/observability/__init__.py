"""Observability bootstrap — generic wiring for tracking backends.

Currently ships Sentry / GlitchTip. Future backends (OpenTelemetry,
Datadog) plug into the same ``setup_observability`` family.
"""

from .Sentry import setup_sentry

__all__ = ["setup_sentry"]
