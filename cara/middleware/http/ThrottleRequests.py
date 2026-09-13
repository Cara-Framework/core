"""Rate-limit a route through one named limiter: ``throttle:<name>``."""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from typing import Any

import cara.facades as facades
from cara.exceptions import RateLimitConfigurationException, TooManyRequestsException
from cara.http import Request, Response
from cara.middleware.Middleware import Middleware
from cara.observability import counter, metric_name
from cara.rates import Limit, RateLimitDecision

_logger = logging.getLogger("cara.middleware.throttle")


class ThrottleRequests(Middleware):
    """Spend one request from a named limiter's bucket, or refuse with 429.

    A route names its limiter — ``throttle:api`` — and ``config/rate.py``
    LIMITERS maps that name to a callback returning the ``Limit`` for the
    request in front of it: its pace, its burst and its key. The name is the
    only form. ``throttle:60,1`` built an unreviewed budget at the call site,
    outside the one file every other ceiling lives in, and no route used it.

    The parameter is deliberately unannotated: ``MiddlewareParameterParser``
    coerces raw route strings through ``__init__`` annotations, and an ``int``
    annotation once turned ``throttle:api`` into a silent no-op.

    An allowed response carries the IETF ``RateLimit-Policy`` and ``RateLimit``
    headers (draft-ietf-httpapi-ratelimit-headers); a refusal raises
    ``TooManyRequestsException`` with the same pair plus ``Retry-After``, so it
    leaves through the one error path — envelope, CORS and all. The
    ``X-RateLimit-*`` trio it replaced was nobody's standard.

    Nothing registers this middleware globally. It used to sit in the
    framework's default stack as a parameterless instance that had to
    recognise itself and do nothing, so a throttled route was not charged
    twice; a middleware that exists to skip itself is not a feature.
    """

    def __init__(self, application, limiter=None):
        super().__init__(application)
        self.limiter_name = limiter

    async def handle(
        self, request: Request, next_fn: Callable[..., Awaitable[Any]]
    ) -> Response:
        if self._is_trusted_ip(request):
            return await next_fn(request)

        limit = self._resolve_limit(request)
        if limit.unlimited:
            return await next_fn(request)

        decision = facades.RateLimiter.attempt(limit)
        self._count(decision)
        headers = self._headers(decision)
        if not decision.allowed:
            raise TooManyRequestsException(
                retry_after=decision.retry_after,
                response_headers=headers,
            )

        response = await next_fn(request)
        for name, value in headers.items():
            response.header(name, value)
        return response

    def _resolve_limit(self, request: Request) -> Limit:
        """The ``Limit`` this route's named limiter sets for ``request``.

        Refuses rather than defaulting (§9): a missing or unregistered name is
        an unconfigured gate. The global 60/min fallback that once answered a
        mistyped ``throttle:login`` enforced twelve times the intended ceiling
        while every header still read "throttled".
        """
        name = self.limiter_name
        if not isinstance(name, str) or not name:
            raise RateLimitConfigurationException(
                "throttle needs a limiter name — throttle:<name>, registered in "
                "config/rate.py LIMITERS."
            )
        limit = facades.RateLimiter.resolve_limiter(name, request)
        if limit is None:
            raise RateLimitConfigurationException(
                f"throttle:{name} names an unregistered rate limiter; register it "
                "in config/rate.py LIMITERS."
            )
        return limit

    def _headers(self, decision: RateLimitDecision) -> dict[str, str]:
        """The IETF pair for this policy; ``t`` is seconds until the bucket is full."""
        policy = self.limiter_name
        return {
            "RateLimit-Policy": f'"{policy}";q={decision.limit};w={decision.period_seconds}',
            "RateLimit": f'"{policy}";r={decision.remaining};t={decision.reset_after}',
        }

    def _count(self, decision: RateLimitDecision) -> None:
        """Record the decision for alerting; a metrics fault never fails the request."""
        try:
            counter(
                metric_name("rate_limit_decisions_total"),
                "Rate-limit decisions by named limiter and outcome.",
                ("limiter", "outcome"),
            ).labels(
                limiter=self.limiter_name,
                outcome="allowed" if decision.allowed else "denied",
            ).inc()
        except Exception:
            _logger.debug("rate-limit decision metric failed", exc_info=True)

    def _is_trusted_ip(self, request: Request) -> bool:
        """Check whether the request originates from a trusted IP.

        ROOT-CAUSE (scenario 8 / cycle 1, deferred from scenario 6):
        ``Configuration.load`` lower-cases every module attribute name when
        storing it (``commons/cara/cara/configuration/Configuration.py:64``
        — ``self._config[f"{module_name}.{name.lower()}"] = value``).
        ``config/rate.py`` declares ``TRUSTED_IPS = [...]`` (uppercase, the
        Python module-level convention for constants), which is stored as
        ``rate.trusted_ips``. The previous lookup of ``rate.TRUSTED_IPS``
        always missed → default ``[]`` → trusted-IP bypass was dead, and
        every health-check / monitoring probe / local dev request was
        consuming rate-limit budget. Verified at runtime in scenario 6:
        ``Config.get("rate.TRUSTED_IPS")`` → ``[]`` while
        ``Config.get("rate.trusted_ips")`` → ``["127.0.0.1", "::1"]``.

        The lowercase path is the only canonical post-load shape.
        """
        try:
            trusted = facades.Config.get("rate.trusted_ips", [])
            if not trusted:
                return False
            client_ip = (
                request.ip()
                if callable(getattr(request, "ip", None))
                else getattr(request, "ip", None)
            )
            return str(client_ip) in trusted
        except Exception as e:
            facades.Log.warning("ThrottleRequests internal failure: %s", e)
            return False
