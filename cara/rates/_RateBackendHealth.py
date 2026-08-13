"""Process-local transition latch for the rate-limit cache authority."""

from __future__ import annotations

import threading
import time


class _RateBackendHealth:
    """Remember backend health only to deduplicate transition logs."""

    def __init__(self) -> None:
        self._healthy = True
        self._announced_state = True
        self._last_failure_at: float | None = None
        self._lock = threading.Lock()

    @property
    def is_healthy(self) -> bool:
        return self._healthy

    @property
    def last_failure_at(self) -> float | None:
        return self._last_failure_at

    def record_failure(self) -> bool:
        """Return whether this is the first failure in the outage."""
        with self._lock:
            self._healthy = False
            self._last_failure_at = time.monotonic()
            should_announce = self._announced_state
            self._announced_state = False
            return should_announce

    def record_success(self) -> bool:
        """Return whether this is the first success after an outage."""
        with self._lock:
            was_unhealthy = not self._healthy
            self._healthy = True
            should_announce = was_unhealthy and not self._announced_state
            self._announced_state = True
            return should_announce

    def reset(self) -> None:
        with self._lock:
            self._healthy = True
            self._announced_state = True
            self._last_failure_at = None
