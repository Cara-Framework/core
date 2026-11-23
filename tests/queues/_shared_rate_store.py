"""A shared rate store for the job-limit tests: one cache, one clock, no Redis.

``SharedRateStore`` stands in for ``cara.facades.Cache`` behind both the rate
authority (``Cache.throttle``, the GCRA cell, in the FileCacheDriver's
arithmetic) and the plain keys a job limit keeps beside it (``get`` / ``put`` /
``forget`` / ``increment``, expiring on the same clock). ``down = True`` makes every call
raise, as an unreachable Redis does.

``install`` wires it in and returns it; ``as_another_process`` swaps a
middleware module's in-process state for a fresh one, which is all that
separates two worker processes sharing one cache.
"""

from __future__ import annotations

import importlib
import math
from types import SimpleNamespace

from cara.rates import RateLimitAuthority as authority

_outage_module = importlib.import_module("cara.queues.middleware._SharedLimitOutage")


class Clock:
    def __init__(self) -> None:
        self.now = 1_000_000.0

    def time(self) -> float:
        return self.now


class SharedRateStore:
    def __init__(self, clock: Clock) -> None:
        self._clock = clock
        self._values: dict[str, tuple[object, float | None]] = {}
        self.down = False

    def _check(self) -> None:
        if self.down:
            raise ConnectionError("redis: connection refused")

    def _live(self, key: str):
        stored = self._values.get(key)
        if stored is None:
            return None
        value, expires_at = stored
        if expires_at is not None and self._clock.now >= expires_at:
            del self._values[key]
            return None
        return value

    def get(self, key: str, default=None):
        self._check()
        value = self._live(key)
        return default if value is None else value

    def put(self, key: str, value, ttl: int | None = None) -> None:
        self._check()
        expires_at = None if not ttl else self._clock.now + ttl
        self._values[key] = (value, expires_at)

    def forget(self, key: str) -> bool:
        self._check()
        return self._values.pop(key, None) is not None

    def increment(self, key: str, amount: int = 1, ttl: int | None = None) -> int:
        """Redis' INCRBY-then-EXPIRE-if-unset: only the first hit sets the TTL."""
        self._check()
        if not ttl:
            raise ValueError("counters require a positive expiration")
        current = self._live(key)
        if current is None:
            self._values[key] = (amount, self._clock.now + ttl)
            return amount
        expires_at = self._values[key][1]
        self._values[key] = (current + amount, expires_at)
        return current + amount

    def throttle(self, key, *, emission_interval_ms, burst, cost=1):
        self._check()
        now = int(self._clock.now * 1000)
        stored = self._live(key)
        tolerance = emission_interval_ms * burst
        tat = now if stored is None or stored < now else stored
        next_tat = tat + emission_interval_ms * cost
        allow_at = next_tat - tolerance
        if now < allow_at:
            remaining = max(0, (tolerance - (tat - now)) // emission_interval_ms)
            return False, remaining, allow_at - now, tat - now
        full_after = next_tat - now
        self._values[key] = (next_tat, self._clock.now + math.ceil(full_after / 1000))
        return (
            True,
            max(0, (tolerance - full_after) // emission_interval_ms),
            0,
            full_after,
        )


class RecordingLog:
    def __init__(self) -> None:
        self.warnings: list[str] = []

    def warning(self, message, *args, **_kwargs) -> None:
        self.warnings.append(message % args if args else message)


def install(monkeypatch, module) -> tuple[SharedRateStore, Clock, RecordingLog]:
    """One store and one clock behind the rate authority and ``module``.

    The returned log records what the module's outage latch says.
    """
    clock = Clock()
    store = SharedRateStore(clock)
    quiet = SimpleNamespace(warning=lambda *_a, **_k: None)
    monkeypatch.setattr(authority.facades, "Cache", store)
    monkeypatch.setattr(authority.facades, "Log", quiet)
    authority._reset_for_tests()
    monkeypatch.setattr(module, "time", clock)
    monkeypatch.setattr(module, "Log", quiet)
    if hasattr(module, "Cache"):
        monkeypatch.setattr(module, "Cache", store)
    outage_log = RecordingLog()
    monkeypatch.setattr(_outage_module, "Log", outage_log)
    monkeypatch.setattr(module, "_outage", _outage_module._SharedLimitOutage("limit"))
    return store, clock, outage_log


def as_another_process(monkeypatch, module, *state_names: str) -> None:
    """Give ``module`` fresh in-process state, as a second worker process has."""
    for name in state_names:
        monkeypatch.setattr(module, name, {})
