"""
Fixed-Window Rate Limiter for the Cara framework.

This module implements a fixed-window rate limiting algorithm using the cache system, enforcing
request limits per key within a time window. It supports named limiters (Laravel-style)
for flexible per-user, per-endpoint rate limiting.
"""

from __future__ import annotations

from collections.abc import Callable

from cara.exceptions import RateLimitConfigurationException
from cara.facades import Cache
from cara.rates.contracts import RateLimit
from cara.rates.RateLimitAuthority import attempt_rate_limit


class RateLimiter(RateLimit):
    """
    Fixed‐window rate limiter with named limiter support.

    Uses the 'cache' to store per‐key counts and supports named rate limiters
    for flexible per-user, per-endpoint configuration.
    """

    driver_name = "fixed"

    def __init__(self, application, options: dict):
        """
        Args:
            application: the IoC container / application instance
            options: dict containing:
                - limit: int, max hits per window
                - window_seconds: int, length of window in seconds
                - cache_prefix: str, prefix for all counter keys
        """
        if not isinstance(options, dict):
            raise RateLimitConfigurationException(
                "Rate limiter options must be a dictionary."
            )
        limit = options.get("limit")
        window = options.get("window_seconds")
        prefix = options.get("cache_prefix")
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1:
            raise RateLimitConfigurationException(
                "rate.drivers.fixed.limit must be a positive integer."
            )
        if isinstance(window, bool) or not isinstance(window, int) or window < 1:
            raise RateLimitConfigurationException(
                "rate.drivers.fixed.window_seconds must be a positive integer."
            )
        if not isinstance(prefix, str) or not prefix.strip():
            raise RateLimitConfigurationException(
                "rate.drivers.fixed.cache_prefix must be a non-empty string."
            )
        self.application = application
        self.limit = limit
        self.window = window
        self.prefix = prefix.strip()
        self._limiters = {}  # Named limiter definitions (name -> callback)

    def attempt(self, key: str) -> tuple[bool, int, int]:
        """
        Record one attempt.

        Returns (allowed, remaining, reset_in).

        ROOT-CAUSE / scenario 6 (concurrent load probe).
        ------------------------------------------------
        The previous implementation was a textbook non-atomic
        read-modify-write:

            current = Cache.get(cache_key, {"count": 0, ...})
            count   = current.get("count", 0) + 1
            Cache.put(cache_key, {"count": count, ...})

        Under concurrent traffic, N threads all read the same ``count``,
        all increment locally, and all write back the same ``count + 1``.
        The on-storage count under-counts by ``N - 1`` for every burst,
        which means the rate limiter silently allows roughly ``N x``
        the configured budget when callers slam the same key in
        parallel — exactly when rate limiting matters most (abuse,
        automated clients, account-creation bots).

        ``ThrottleRequests`` (the framework's HTTP middleware) was
        already migrated to ``Cache.increment`` (atomic Redis
        ``INCRBY``); ``RateLimiter.attempt`` is the public
        ``RateLimit`` contract method and was still on the unsafe path.
        Apps calling ``RateLimiter.attempt(key)`` directly (custom
        middleware, queue jobs, console commands) inherited the race.

        This rewrite delegates to ``Cache.increment`` for the count and
        ``Cache.ttl`` for the reset deadline, matching the throttle
        middleware's semantics and giving the same atomic guarantee on
        every backend the framework supports (Redis ``INCRBY`` is
        atomic; the file driver acquires a per-key lock around the
        increment).
        """
        cache_key = f"{self.prefix}{key}"

        # Both this method and transport middleware use one fail-closed
        # authority, so accounting and outage policy cannot drift.
        allowed, remaining, reset_in = attempt_rate_limit(
            cache_key=cache_key,
            window_seconds=self.window,
            max_attempts=self.limit,
        )
        return allowed, remaining, reset_in

    def for_(self, name: str, callback: Callable) -> RateLimiter:
        """
        Register a named rate limiter with a callback.

        The callback receives a request object and must return exactly ONE
        ``Limit``. Both this docstring and ``resolve_limiter`` used to promise
        "a Limit object or list of Limit objects" — nothing implements the
        list. ``ThrottleRequests._get_limit_config`` returns the callback's
        value unchanged and ``_attempt_limit`` then reads
        ``limit_config.max_attempts``, so a limiter that took the documented
        second option answered ``AttributeError: 'list' object has no
        attribute 'max_attempts'`` — a 500 on every request to every route
        carrying that ``throttle:<name>``. §10: the promise is deleted rather
        than implemented, because composing several windows also needs a key
        per limit, a most-restrictive rule for the ``X-RateLimit-*`` headers
        and a choice of which limit's ``response`` callback wins — none of
        which exist. ``resolve_limiter`` now refuses a non-``Limit`` return
        instead of letting it reach the middleware as an AttributeError.

        Args:
            name: Unique identifier for this named limiter
            callback: Function that takes a request and returns a ``Limit``

        Returns:
            self for method chaining
        """
        self._limiters[name] = callback
        return self

    def limiter(self, name: str) -> Callable | None:
        """
        Get a registered named limiter callback.

        Args:
            name: Name of the limiter to retrieve

        Returns:
            The callback function or None if not found
        """
        return self._limiters.get(name)

    def resolve_limiter(self, name: str, request):
        """
        Resolve a named limiter for a given request.

        Args:
            name: Name of the registered limiter
            request: The HTTP request object

        Returns:
            The ``Limit`` the callback produced, or ``None`` when no limiter
            is registered under ``name`` (``ThrottleRequests`` turns that
            ``None`` into a refusal — an unregistered limiter name is an
            unconfigured gate, not a permissive default).

        Raises:
            RateLimitConfigurationException: the callback returned something
                the throttle cannot enforce. Checked on the two attributes
                ``ThrottleRequests`` actually dereferences rather than on the
                concrete class, so an application's own limit object still
                works. The list form this method's docstring used to advertise
                lands here: it reached ``_attempt_limit`` as a bare
                ``AttributeError`` 500 with nothing naming the misconfigured
                limiter, and a shape the gate cannot read must fail closed
                and say which limiter is wrong (§9).
        """
        callback = self._limiters.get(name)
        if callback is None:
            return None

        limit = callback(request)
        if not hasattr(limit, "max_attempts") or not hasattr(limit, "decay_minutes"):
            raise RateLimitConfigurationException(
                f"throttle:{name} resolved to {type(limit).__name__}, which is "
                f"not a Limit; a limiter callback must return exactly one "
                f"Limit (see config/rate.py LIMITERS)."
            )
        return limit

    def reset(self, key: str) -> None:
        """Immediately reset this key's counter."""
        cache_key = f"{self.prefix}{key}"
        # Simply remove it
        Cache.forget(cache_key)
