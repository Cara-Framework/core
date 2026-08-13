"""Canonical definition of ``ConcurrencyBackendUnavailable``."""

from __future__ import annotations

from cara.exceptions import CaraException


class ConcurrencyBackendUnavailable(CaraException):
    """Raised when the configured cache driver cannot enforce a ceiling.

    Deliberately NOT a throttle: this is a deployment mistake, not a
    transient one, so it must consume attempts, reach the dead-letter queue
    and page — the same posture as production mail refusing to boot without
    a real mail host (§9). Silently running the job uncapped is what this
    class exists to stop.
    """

    is_throttle: bool = False
