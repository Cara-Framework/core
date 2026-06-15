"""Circuit breaker pattern for guarding external service calls.

Standard three-state breaker — CLOSED → OPEN → HALF_OPEN — that
short-circuits calls when an upstream is failing so a transient
outage doesn't cascade into a thundering-herd retry storm. Generic,
domain-free; apps wire their own thresholds + recovery timeouts per
upstream (one breaker per Amazon API, eBay search, Stripe charge,
…).

Usage:

    from cara.resilience.CircuitBreaker import CircuitBreaker

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

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from enum import Enum
from typing import Any

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

    def _state_locked(self) -> CircuitState:
        """Return the current state, performing the OPEN→HALF_OPEN recovery
        transition if the window elapsed. MUST be called while holding
        ``self._lock`` so the transition and any subsequent probe-slot
        claim form a single atomic critical section."""
        if self._state == CircuitState.OPEN:
            if time.time() - self._last_failure_time >= self.recovery_timeout:
                self._state = CircuitState.HALF_OPEN
                self._half_open_calls = 0
                Log.info("Circuit '%s' transitioning to HALF_OPEN", self.name)
        return self._state

    @property
    def state(self) -> CircuitState:
        with self._lock:
            return self._state_locked()

    @property
    def is_available(self) -> bool:
        state = self.state
        if state == CircuitState.CLOSED:
            return True
        if state == CircuitState.HALF_OPEN:
            with self._lock:
                return self._half_open_calls < self.half_open_max_calls
        return False

    def _acquire(self) -> None:
        """Atomically check availability and claim a HALF_OPEN probe slot.

        The state transition, the OPEN/budget check, and the
        ``_half_open_calls += 1`` claim run under a SINGLE lock. Pre-fix
        ``call``/``__enter__`` did a racy check-then-act: ``is_available``
        evaluated ``_half_open_calls < half_open_max_calls`` under the
        lock, released it, then a SEPARATE lock block did the increment.
        With ``half_open_max_calls=1``, N concurrent callers all observed
        ``0 < 1`` and all incremented — so the recovery-probe burst was
        bounded by caller count instead of the configured budget,
        defeating the thundering-herd protection HALF_OPEN exists for.
        """
        with self._lock:
            state = self._state_locked()
            if state == CircuitState.OPEN:
                raise CircuitOpenError(
                    f"Circuit '{self.name}' is OPEN. "
                    f"Recovery in {self.recovery_timeout - (time.time() - self._last_failure_time):.0f}s"
                )
            if state == CircuitState.HALF_OPEN:
                if self._half_open_calls >= self.half_open_max_calls:
                    raise CircuitOpenError(
                        f"Circuit '{self.name}' is HALF_OPEN; probe budget "
                        f"({self.half_open_max_calls}) exhausted"
                    )
                self._half_open_calls += 1

    def call(self, func: Callable, *args, **kwargs) -> Any:
        """Execute ``func`` through the circuit; raise ``CircuitOpenError`` if OPEN."""
        self._acquire()
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
                Log.info("Circuit '%s' recovered → CLOSED", self.name)

    def _on_failure(self, error: Exception) -> None:
        with self._lock:
            self._failure_count += 1
            self._last_failure_time = time.time()

            if self._state == CircuitState.HALF_OPEN:
                self._state = CircuitState.OPEN
                Log.warning(
                    "Circuit '%s' failed in HALF_OPEN → OPEN: %s", self.name, error
                )
            elif self._failure_count >= self.failure_threshold:
                self._state = CircuitState.OPEN
                Log.warning("Circuit '%s' OPENED after %s failures: %s", self.name, self._failure_count, error)

    def __enter__(self) -> CircuitBreaker:
        self._acquire()
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
