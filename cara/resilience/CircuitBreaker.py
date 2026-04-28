"""Circuit breaker pattern for guarding external service calls.

Standard three-state breaker — CLOSED → OPEN → HALF_OPEN — that
short-circuits calls when an upstream is failing so a transient
outage doesn't cascade into a thundering-herd retry storm. Generic,
domain-free; apps wire their own thresholds + recovery timeouts per
upstream (one breaker per Amazon API, eBay search, Stripe charge,
…).

Usage:

    from cara.resilience import CircuitBreaker

    breaker = CircuitBreaker(
        name="amazon_api",
        failure_threshold=5,
        recovery_timeout=60,
    )

    # Either call-style:
    result = breaker.call(lambda: requests.get(url))

    # Or context-manager-style:
    with breaker:
        result = requests.get(url)
"""

import threading
import time
from enum import Enum
from typing import Any, Callable

from cara.facades import Log


class CircuitState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitOpenError(Exception):
    """Raised when a caller hits an OPEN circuit (still in recovery window)."""


class CircuitBreaker:
    """Thread-safe circuit breaker with configurable thresholds.

    States and transitions:

    * **CLOSED** — normal operation. Failures increment a counter;
      when the counter hits ``failure_threshold`` the circuit OPENs.
    * **OPEN** — all calls raise ``CircuitOpenError`` immediately
      until ``recovery_timeout`` seconds have elapsed since the last
      failure. Then the next call moves the circuit to HALF_OPEN.
    * **HALF_OPEN** — limited probe traffic (``half_open_max_calls``)
      is allowed through. A success closes the circuit; a single
      failure re-opens it for another ``recovery_timeout`` window.
    """

    def __init__(
        self,
        name: str,
        failure_threshold: int = 5,
        recovery_timeout: int = 60,
        half_open_max_calls: int = 1,
    ):
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.half_open_max_calls = half_open_max_calls

        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time = 0.0
        self._half_open_calls = 0
        self._lock = threading.Lock()

    @property
    def state(self) -> CircuitState:
        with self._lock:
            if self._state == CircuitState.OPEN:
                if time.time() - self._last_failure_time >= self.recovery_timeout:
                    self._state = CircuitState.HALF_OPEN
                    self._half_open_calls = 0
                    Log.info(f"Circuit '{self.name}' transitioning to HALF_OPEN")
            return self._state

    @property
    def is_available(self) -> bool:
        state = self.state
        if state == CircuitState.CLOSED:
            return True
        if state == CircuitState.HALF_OPEN:
            with self._lock:
                return self._half_open_calls < self.half_open_max_calls
        return False

    def call(self, func: Callable, *args, **kwargs) -> Any:
        """Execute ``func`` through the circuit; raise ``CircuitOpenError`` if OPEN."""
        if not self.is_available:
            raise CircuitOpenError(
                f"Circuit '{self.name}' is OPEN. "
                f"Recovery in {self.recovery_timeout - (time.time() - self._last_failure_time):.0f}s"
            )

        with self._lock:
            if self._state == CircuitState.HALF_OPEN:
                self._half_open_calls += 1

        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
        except Exception as e:
            self._on_failure(e)
            raise

    def _on_success(self) -> None:
        with self._lock:
            self._failure_count = 0
            self._success_count += 1
            if self._state == CircuitState.HALF_OPEN:
                self._state = CircuitState.CLOSED
                Log.info(f"Circuit '{self.name}' recovered → CLOSED")

    def _on_failure(self, error: Exception) -> None:
        with self._lock:
            self._failure_count += 1
            self._last_failure_time = time.time()

            if self._state == CircuitState.HALF_OPEN:
                self._state = CircuitState.OPEN
                Log.warning(f"Circuit '{self.name}' failed in HALF_OPEN → OPEN: {error}")
            elif self._failure_count >= self.failure_threshold:
                self._state = CircuitState.OPEN
                Log.warning(
                    f"Circuit '{self.name}' OPENED after {self._failure_count} failures: {error}"
                )

    def __enter__(self) -> "CircuitBreaker":
        if not self.is_available:
            raise CircuitOpenError(f"Circuit '{self.name}' is OPEN")
        with self._lock:
            if self._state == CircuitState.HALF_OPEN:
                self._half_open_calls += 1
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        if exc_type is not None:
            self._on_failure(exc_val)
            return False
        self._on_success()
        return False

    def reset(self) -> None:
        """Manually reset the circuit (forces back to CLOSED)."""
        with self._lock:
            self._state = CircuitState.CLOSED
            self._failure_count = 0
            self._half_open_calls = 0

    def get_status(self) -> dict:
        return {
            "name": self.name,
            "state": self.state.value,
            "failure_count": self._failure_count,
            "success_count": self._success_count,
            "failure_threshold": self.failure_threshold,
            "recovery_timeout": self.recovery_timeout,
        }


__all__ = ["CircuitBreaker", "CircuitOpenError", "CircuitState"]
