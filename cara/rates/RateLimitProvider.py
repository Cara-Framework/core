"""Registers the named-limiter registry under ``rate``."""

from __future__ import annotations

from cara.configuration import config
from cara.exceptions import RateLimitConfigurationException
from cara.foundation import DeferredProvider
from cara.rates.RateLimiter import RateLimiter


class RateLimitProvider(DeferredProvider):
    """Deferred provider for the ``rate`` binding.

    The registry has no driver and no default budget. A fixed-window driver
    used to carry ``rate.drivers.fixed.limit`` / ``window_seconds``, which only
    ``RateLimiter.attempt(key)`` read and no route ever reached — so
    ``RATE_LIMIT=600`` in an operator's environment read as the API's ceiling
    while every throttled route enforced its own named limiter. Every ceiling
    now lives in ``rate.limiters``, where a route names it.
    """

    @classmethod
    def provides(cls) -> list[str]:
        return ["rate"]

    def register(self) -> None:
        limiters = config("rate.limiters", {})
        if not isinstance(limiters, dict):
            raise RateLimitConfigurationException(
                "rate.limiters must map limiter names to callbacks "
                "(config/rate.py LIMITERS)."
            )
        registry = RateLimiter(self.application)
        for name, callback in limiters.items():
            if not callable(callback):
                raise RateLimitConfigurationException(
                    f"rate.limiters[{name!r}] must be a callback returning a Limit."
                )
            registry.for_(name, callback)
        self.application.bind("rate", registry)

    def boot(self) -> None:
        pass
