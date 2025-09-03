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
        # HTTP rate limit logic
        # Derive a unique key per client. Here: use IP + path.
        client_ip = request.ip() or "anonymous"
        route_path = request.path or "/"
        # You might also include request.method if you want GET vs POST separate.
        key = f"{client_ip}|{route_path}"

        # If custom parameters provided, we need to implement custom logic
        # Otherwise, use the global RateLimiter which uses rate.py config
        if self.custom_limit is not None:
            # Custom rate limiting with parameters
            allowed, remaining, reset_in = self._custom_attempt(key)
        else:
            # Use global RateLimiter from rate.py config
            allowed, remaining, reset_in = RateLimiter.attempt(key)

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
            resp.header("X-RateLimit-Limit", str(self.custom_limit or RateLimiter.limit))
            resp.header("X-RateLimit-Remaining", "0")
            resp.header("X-RateLimit-Reset", str(reset_in))

            # It's also common to include a Retry-After header (seconds)
            resp.header("Retry-After", str(reset_in))

            return resp

        # If allowed, call the next handler to get the response
        response = await next(request)

        # Attach headers so clients can see their quota
        response.header("X-RateLimit-Limit", str(self.custom_limit or RateLimiter.limit))
        response.header("X-RateLimit-Remaining", str(remaining))
        response.header("X-RateLimit-Reset", str(reset_in))

        return response

    def _custom_attempt(self, key: str) -> tuple[bool, int, int]:
        """Custom rate limiting when parameters are provided."""
        import time

        from cara.facades import Cache

        window_seconds = (self.custom_window_minutes or 1) * 60
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
        allowed = count <= self.custom_limit
        remaining = max(self.custom_limit - count, 0)
        reset_in = max(expires_at - now, 0)

        # Save back to cache
        Cache.put(cache_key, {"count": count, "expires_at": expires_at}, ttl=reset_in)

        return allowed, remaining, reset_in
