"""
Middleware that enforces a fixed-window rate limit on each request.

Laravel-style parametric usage: throttle:60,1 (60 requests per 1 minute)
If the client exceeds the limit, returns a 429 Response with appropriate headers so the client
knows when to retry. Otherwise, adds rate-limit info in response headers.
"""

from typing import Callable, Optional

from cara.facades import Log, RateLimiter
from cara.http import Request, Response
from cara.middleware import Middleware


class ThrottleRequests(Middleware):
    """Rate limiting middleware with automatic parameter parsing."""

    def __init__(self, application, limit=None, window=None):
        """ROOT-CAUSE / scenario 6 (concurrent load probe).
        ----------------------------------------------------
        ``limit`` and ``window`` are intentionally **untyped**.
        ``MiddlewareParameterParser`` inspects ``__init__`` annotations
        and uses them to coerce raw route-middleware strings — so
        annotating ``limit: Optional[int]`` made the parser run
        ``int("catalog")`` for ``throttle:catalog``, raise ``ValueError``,
        and silently fall back to ``limit=None``. The route then
        behaved as if it had no throttle at all, and the parameterless
        ``ThrottleRequests`` global did the actual rate-limit work
        with the global fallback ``Limit(RateLimiter.limit, ...)``
        instead of the named limiter the route asked for. With the
        annotations removed the parser keeps the raw string and the
        constructor's existing string/int branching does the right
        thing — named limiters resolve via
        ``RateLimiter.resolve_limiter(name, request)``.
        """
        super().__init__(application)

        if limit is not None and not isinstance(limit, str):
            self.custom_limit = limit
        elif limit is not None:
            try:
                self.custom_limit = int(limit)
            except (ValueError, TypeError):
                self.custom_limit = limit
        else:
            self.custom_limit = None

        if window is not None:
            try:
                self.custom_window_minutes = int(window)
            except (ValueError, TypeError):
                self.custom_window_minutes = window
        else:
            self.custom_window_minutes = None

    async def handle(self, request: Request, next: Callable):
        """Handle rate limiting logic."""
        # Bypass for trusted IPs (monitoring, health checks, local dev).
        if self._is_trusted_ip(request):
            return await next(request)

        # ROOT-CAUSE / scenario 6 (concurrent load probe).
        # ``ThrottleRequests`` is registered TWICE in the framework:
        #   1. As a default global middleware in
        #      ``MiddlewareProvider.default_http_middleware`` (no params).
        #   2. As the route-level alias ``throttle:<name>`` (with params).
        #
        # Previously, the parameterless global instance fell through to
        # ``Limit(max_attempts=RateLimiter.limit, ...)`` and incremented
        # the same ``throttle_<method>:<endpoint>:<ip>`` key the
        # route-level instance later incremented again — every request
        # to a throttled route burned TWO units of budget instead of
        # one. ``X-RateLimit-Remaining`` decremented at 2x the request
        # rate, halving the effective per-IP budget for any route that
        # opted into ``throttle:<name>`` (which is most of the API).
        #
        # The default global instance has ``custom_limit is None`` and
        # ``custom_window_minutes is None`` (no constructor args).
        # Treat that shape as opt-out: rate limiting is now strictly
        # opt-in via ``throttle:<name>`` per route (or via parameterised
        # global registration like ``throttle:60,1`` in
        # ``config/middleware.py``). This eliminates the double-charge
        # without taking protection away from any route that already
        # declared ``throttle:<name>``.
        if self.custom_limit is None and self.custom_window_minutes is None:
            return await next(request)

        # First, check if parameter is a named limiter
        limit_config = self._resolve_limit_config(request)

        if limit_config is None:
            # If no limit config found, allow the request through
            return await next(request)

        # Get the rate limit key
        key = self._resolve_key(request, limit_config)

        # Attempt to check/record the request
        allowed, remaining, reset_in = self._attempt_limit(key, limit_config)

        if not allowed:
            # Build a 429 Response and attach headers
            resp = Response(self.application)
            # Standard 429 body (JSON)
            body = {
                "success": False,
                "message": "Too Many Requests",
            }

            # Set JSON payload and status
            resp.json(body, 429)

            # Attach rate-limit headers
            max_attempts = getattr(limit_config, 'max_attempts', self.custom_limit or 60)
            resp.header("X-RateLimit-Limit", str(max_attempts))
            resp.header("X-RateLimit-Remaining", "0")
            resp.header("X-RateLimit-Reset", str(reset_in))

            # It's also common to include a Retry-After header (seconds)
            resp.header("Retry-After", str(reset_in))

            return resp

        # If allowed, call the next handler to get the response
        response = await next(request)

        # Attach headers so clients can see their quota
        max_attempts = getattr(limit_config, 'max_attempts', self.custom_limit or 60)
        response.header("X-RateLimit-Limit", str(max_attempts))
        response.header("X-RateLimit-Remaining", str(remaining))
        response.header("X-RateLimit-Reset", str(reset_in))

        return response

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

        We probe the lowercase path first (the canonical, post-load shape)
        and fall back to the uppercase path so any legacy out-of-tree
        config that was registered manually with ``Config.set("rate.TRUSTED_IPS", ...)``
        keeps working without a behaviour change.
        """
        try:
            from cara.facades import Config
            trusted = Config.get("rate.trusted_ips", None)
            if trusted is None:
                # Defensive fallback — if a caller registered the
                # uppercase key explicitly via Config.set, keep honouring it.
                trusted = Config.get("rate.TRUSTED_IPS", [])
            if not trusted:
                return False
            client_ip = request.ip() if callable(getattr(request, "ip", None)) else getattr(request, "ip", None)
            return str(client_ip) in trusted
        except Exception as e:
            Log.warning(f"ThrottleRequests internal failure: {e}")
            return False

    def _resolve_limit_config(self, request: Request):
        """
        Resolve the limit configuration for this request.
        
        Checks in order:
        1. Named limiter (if middleware parameter matches a registered limiter name)
        2. Custom numeric parameters (throttle:60,1 format)
        3. Global rate limiter configuration
        
        Returns a Limit object or None if no rate limiting applies.
        """
        from cara.rates import Limit

        # Check if custom_limit is actually a limiter name (string)
        if isinstance(self.custom_limit, str):
            # Try to resolve as named limiter
            resolved = RateLimiter.resolve_limiter(self.custom_limit, request)
            if resolved:
                return resolved
        elif self.custom_limit is not None:
            # Custom numeric parameters provided (throttle:60,1)
            window_minutes = self.custom_window_minutes or 1
            return Limit(max_attempts=self.custom_limit, decay_minutes=window_minutes)

        # Fall back to global RateLimiter config (if available)
        # RateLimiter.window is in seconds; convert to minutes for Limit
        return Limit(max_attempts=RateLimiter.limit, decay_minutes=RateLimiter.window / 60)

    def _resolve_key(self, request: Request, limit_config) -> str:
        """
        Resolve the rate limit key for this request and limit config.
        
        Uses the limit's _key attribute if set, otherwise constructs a default key
        from endpoint + user_id (or IP address).
        
        Args:
            request: The HTTP request object
            limit_config: The Limit object
            
        Returns:
            A unique rate limit key
        """
        # If the Limit has a custom key set, use that
        if hasattr(limit_config, '_key') and limit_config._key:
            return limit_config._key

        # Default: method:route_template:user_id_or_ip
        #
        # Two correctness fixes vs. the previous version:
        #
        # 1. Use the route TEMPLATE (e.g. ``/users/@id``), not the
        #    literal request path. Otherwise ``/users/1`` and
        #    ``/users/2`` get separate buckets so a ``throttle:60,1``
        #    rule on ``/users/@id`` is per-id, not per-route — exactly
        #    the inverse of what every caller expects.
        #
        # 2. Resolve the user via the canonical ``request.user()``
        #    method (set by ShouldAuthenticate). The previous code
        #    looked at ``request.user_id`` which never gets populated;
        #    every authenticated request silently fell back to the
        #    IP-keyed bucket, defeating ``throttle:auth``-style
        #    per-user limits.
        method = (request.method or "GET").upper()

        endpoint: str = ""
        route = getattr(request, "route", None)
        if route is not None:
            endpoint = getattr(route, "url", "") or getattr(route, "uri", "") or ""
        if not endpoint:
            endpoint = request.path or "/"

        user_id = None
        try:
            user = request.user() if callable(getattr(request, "user", None)) else None
            if user is not None:
                user_id = (
                    getattr(user, "id", None)
                    or getattr(user, "user_id", None)
                )
        except Exception:
            user_id = None

        client_ip = request.ip() or "anonymous"
        identifier = str(user_id) if user_id is not None else client_ip

        return f"{method}:{endpoint}:{identifier}"

    def _attempt_limit(self, key: str, limit_config) -> tuple[bool, int, int]:
        """
        Attempt to record a request against the rate limit.

        Args:
            key: The rate limit key
            limit_config: The Limit object defining the limit

        Returns:
            Tuple of (allowed: bool, remaining: int, reset_in: int).
            ``reset_in`` is the actual remaining seconds until the
            counter resets — it queries ``Cache.ttl(...)`` after the
            atomic increment instead of returning the full window
            length. The previous implementation always reported the
            full window, so a client that hit the limit at second 50
            of a 60-second window was told "retry in 60 s" when the
            truth was "retry in ~10 s".
        """
        from cara.facades import Cache

        # Handle unlimited case
        if limit_config.max_attempts == 0:
            return True, -1, 0

        window_seconds = int(limit_config.decay_minutes * 60)
        cache_key = f"throttle_{key}"

        try:
            # Atomic increment — avoids the read-modify-write race that
            # allowed concurrent requests to slip past the limit.
            # Cache.increment creates the key with TTL if it doesn't exist.
            count = Cache.increment(cache_key, 1, window_seconds)
        except Exception:
            # Cache backend down — degrade open (allow traffic) rather
            # than crash the request or silently block legitimate users.
            return True, limit_config.max_attempts, 0

        allowed = count <= limit_config.max_attempts
        remaining = max(limit_config.max_attempts - count, 0)

        # Query the actual remaining TTL on the bucket. ``Cache.ttl``
        # returns ``None`` when the driver doesn't expose TTL or the
        # key was just removed; in those edge cases fall back to the
        # full window so the header is at least an upper bound.
        actual_ttl = Cache.ttl(cache_key)
        reset_in = actual_ttl if actual_ttl is not None else window_seconds

        return allowed, remaining, reset_in
