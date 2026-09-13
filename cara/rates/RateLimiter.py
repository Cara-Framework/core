"""The named-limiter registry behind ``throttle:<name>``."""

from __future__ import annotations

from collections.abc import Callable

from cara.exceptions import RateLimitConfigurationException
from cara.facades import Cache
from cara.rates.contracts import RateLimit
from cara.rates.Limit import Limit
from cara.rates.RateLimitAuthority import attempt_rate_limit, rate_limit_cache_key
from cara.rates.RateLimitDecision import RateLimitDecision


class RateLimiter(RateLimit):
    """Named limiters, each a callback from a request to ONE ``Limit``.

    ``config/rate.py`` LIMITERS is the only place a ceiling is declared: a
    route names a limiter, and the limiter decides the pace, the burst and the
    key for the request in front of it. ``attempt`` is the one door every
    transport spends through — the HTTP ``throttle:<name>`` middleware, the
    WebSocket handshake throttle, and any job or command that needs a budget.
    """

    def __init__(self, application) -> None:
        self.application = application
        self._limiters: dict[str, Callable] = {}

    def attempt(self, limit: Limit, *, cost: int = 1) -> RateLimitDecision:
        """Spend ``cost`` from ``limit``'s bucket — see ``attempt_rate_limit``."""
        return attempt_rate_limit(limit, cost=cost)

    def reset(self, key: str) -> None:
        """Refill the bucket ``Limit.by(key)`` spends, immediately."""
        Cache.forget(rate_limit_cache_key(key))

    def for_(self, name: str, callback: Callable) -> RateLimiter:
        """Register a named limiter: a callback from a request to one ``Limit``."""
        self._limiters[name] = callback
        return self

    def limiter(self, name: str) -> Callable | None:
        """The callback registered under ``name``, if any."""
        return self._limiters.get(name)

    def resolve_limiter(self, name: str, request) -> Limit | None:
        """The ``Limit`` ``name`` sets for ``request``; ``None`` when unregistered.

        ``ThrottleRequests`` turns ``None`` into a refusal — an unregistered
        name is an unconfigured gate, not a permissive default (§9). Anything
        but one keyed ``Limit`` is refused here, naming the limiter: a list, a
        number or a fall-through ``None`` used to reach the middleware as an
        ``AttributeError`` that named nothing, and an unkeyed Limit has no
        bucket to spend.
        """
        callback = self._limiters.get(name)
        if callback is None:
            return None
        limit = callback(request)
        if not isinstance(limit, Limit):
            raise RateLimitConfigurationException(
                f"throttle:{name} resolved to {type(limit).__name__}, which is not "
                "a Limit; a limiter callback returns exactly one Limit "
                "(see config/rate.py LIMITERS)."
            )
        if not limit.unlimited and not limit.key:
            raise RateLimitConfigurationException(
                f"throttle:{name} resolved a Limit with no key; name its bucket "
                "with .by(...) in config/rate.py LIMITERS."
            )
        return limit
