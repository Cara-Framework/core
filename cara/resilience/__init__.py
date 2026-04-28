"""Resilience primitives — circuit breaker, retry, backoff (future).

Stateful reliability patterns that guard external calls. Generic, no
domain assumptions; apps wire their own thresholds per-upstream.
"""

from .CircuitBreaker import CircuitBreaker, CircuitOpenError, CircuitState

__all__ = ["CircuitBreaker", "CircuitOpenError", "CircuitState"]
