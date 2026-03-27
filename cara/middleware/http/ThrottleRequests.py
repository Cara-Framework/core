"""
Middleware that enforces a fixed-window rate limit on each request.

Laravel-style parametric usage: throttle:60,1 (60 requests per 1 minute)
If the client exceeds the limit, returns a 429 Response with appropriate headers so the client
knows when to retry. Otherwise, adds rate-limit info in response headers.
"""

from typing import Callable, Optional

from cara.facades import RateLimiter
from cara.http import Request, Response
from cara.middleware import Middleware


class ThrottleRequests(Middleware):
    """Rate limiting middleware with automatic parameter parsing."""

    def __init__(
        self, application, limit: Optional[int] = None, window: Optional[int] = None
    ):
        super().__init__(application)

        # If no custom parameters provided, RateLimiter will use rate.py config
        self.custom_limit = limit
        self.custom_window_minutes = window

    async def handle(self, request: Request, next: Callable):
        """Handle rate limiting logic."""
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
        # Return a Limit object based on global settings
        return Limit(max_attempts=RateLimiter.limit, decay_minutes=self.window / 60)

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
        
        # Default: endpoint:user_id_or_ip
        endpoint = request.path or "/"
        user_id = getattr(request, 'user_id', None)
        client_ip = request.ip() or "anonymous"
        identifier = str(user_id) if user_id else client_ip
        
        return f"{endpoint}:{identifier}"

    def _attempt_limit(self, key: str, limit_config) -> tuple[bool, int, int]:
        """
        Attempt to record a request against the rate limit.
        
        Args:
            key: The rate limit key
            limit_config: The Limit object defining the limit
            
        Returns:
            Tuple of (allowed: bool, remaining: int, reset_in: int)
        """
        import time

        from cara.facades import Cache

        # Handle unlimited case
        if limit_config.max_attempts == 0:
            return True, -1, 0

        window_seconds = limit_config.decay_minutes * 60
        now = int(time.time())

        # Get current count from cache
        cache_key = f"throttle_{key}"
        current = Cache.get(cache_key, {"count": 0, "expires_at": 0})
        count = current.get("count", 0)
        expires_at = current.get("expires_at", now + window_seconds)

        # If window expired, reset
        if now >= expires_at:
            count = 0
            expires_at = now + window_seconds

        # Increment counter
        count += 1

        # Check if allowed
        allowed = count <= limit_config.max_attempts
        remaining = max(limit_config.max_attempts - count, 0)
        reset_in = max(expires_at - now, 0)

        # Save back to cache
        Cache.put(cache_key, {"count": count, "expires_at": expires_at}, ttl=reset_in)

        return allowed, remaining, reset_in
